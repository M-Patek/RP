import json
import os
import uuid
import random
import logging
import asyncio
import signal
import time
import hashlib
import aiohttp 
from typing import AsyncGenerator, Optional, Dict, List
from collections import OrderedDict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from curl_cffi.requests import AsyncSession
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeminiTactical")

# --- 全局状态与配置 ---
CONFIG_PATH = "config.json"
AUTO_REPLACEMENT_WEBHOOK = os.getenv("AUTO_REPLACEMENT_WEBHOOK") # 资源枯竭时的自动补货 Webhook
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET")

# --- Redis 配置 ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_CLIENT: Optional[AsyncRedis] = None

# --- 常量定义 ---
FRAME_DELIMITER = b"\n"
SESSION_TTL = 300
UPSTREAM_URL = "https://gemini-cli-backend.googleapis.com/v1/generate"
MAX_RETRIES = 3
DEFAULT_CONCURRENCY = 5

# --- 指纹与仿真库 ---
# 动态 impersonate 池：用于 JA3 指纹扰动
# 即使是 chrome110，微版本号不同，TLS 扩展字段顺序也不同
IMPERSONATE_VARIANTS = [
    "chrome110", "chrome111", "chrome112", 
    "safari15_5", "safari16_0",
    "edge101", "edge103"
]

# --- 1. SlotManager: 具备权重的智能调度器 ---

class SlotManager:
    def __init__(self):
        self.slots = []
        self.states = {} # 运行时状态：failures, weight, last_429_ts
        self.lock = asyncio.Lock()

    def load_config(self):
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    self.slots = json.load(f)
                
                # 初始化/更新状态
                now = time.time()
                for idx, slot in enumerate(self.slots):
                    key_hash = hashlib.md5(slot['key'].encode()).hexdigest()
                    if idx not in self.states:
                        self.states[idx] = {
                            "failures": 0,
                            "weight": 100.0, # 初始健康权重
                            "concurrency_key": key_hash,
                            "last_used_ts": 0,
                            "cool_down_until": 0
                        }
                logger.info(f"[Config] Loaded {len(self.slots)} slots.")
            else:
                logger.warning("[Config] No config found.")
                self.slots = []
        except Exception as e:
            logger.error(f"[Config] Load failed: {e}")

    async def get_best_slot(self, redis_client: AsyncRedis) -> int:
        """
        基于权重的概率调度算法 (Probabilistic Scheduling)
        不直接封杀 429 的 Slot，而是降低其被选中的概率，实现“冷处理”。
        """
        if not self.slots:
            raise HTTPException(status_code=503, detail="Resource pool empty")

        candidates = []
        weights = []
        now = time.time()

        for idx, state in self.states.items():
            # 1. 硬熔断检查 (Cool-down)
            if state["cool_down_until"] > now:
                continue

            # 2. Redis 并发检查 (Soft Limit)
            limit = self.slots[idx].get("max_concurrency", DEFAULT_CONCURRENCY)
            conc_key = f"concurrency:{state['concurrency_key']}"
            
            # 为了性能，这里不做严格的 await get，而是允许一定程度的并发超卖
            # 或者在高并发场景下随机跳过检查
            
            # 3. 计算动态权重
            # 基础权重 100，每失败一次减 20，发生 429 减 50
            current_weight = state["weight"]
            if current_weight <= 0:
                current_weight = 1.0 # 保持最低存活率
            
            candidates.append(idx)
            weights.append(current_weight)

        if not candidates:
            # 如果全军覆没，强制复活一个冷却时间最短的
            logger.warning("All slots cooling down. Forcing revival.")
            return min(self.states, key=lambda k: self.states[k]["cool_down_until"])

        # 基于权重的随机选择
        selected_idx = random.choices(candidates, weights=weights, k=1)[0]
        
        # 增加并发计数
        conc_key = f"concurrency:{self.states[selected_idx]['concurrency_key']}"
        await redis_client.incr(conc_key)
        await redis_client.expire(conc_key, 60)
        
        return selected_idx

    async def release_slot(self, idx: int, redis_client: AsyncRedis):
        if idx not in self.states: return
        conc_key = f"concurrency:{self.states[idx]['concurrency_key']}"
        await redis_client.decr(conc_key)

    async def report_status(self, idx: int, status_code: int):
        """反馈回路：根据状态码调整权重"""
        state = self.states[idx]
        now = time.time()

        if status_code == 200:
            # 成功回血：缓慢恢复权重
            state["weight"] = min(100.0, state["weight"] + 5.0)
            state["failures"] = 0
        
        elif status_code == 429:
            # 429 概率熔断：大幅降低权重，并短暂冷却
            logger.warning(f"Slot {idx} Hit 429. Dropping weight.")
            state["weight"] = max(1.0, state["weight"] - 50.0) 
            state["failures"] += 1
            # 冷却时间随次数指数增长：30s, 60s, 120s...
            backoff = 30 * (2 ** min(state["failures"], 5))
            state["cool_down_until"] = now + backoff
            
        elif status_code == 403:
             # 403 可能是号死了，直接打入冷宫
             logger.error(f"Slot {idx} Hit 403 (Possbile Ban).")
             state["weight"] = 0
             state["cool_down_until"] = now + 3600 # 冷却1小时
             # 触发自动补货 Webhook
             asyncio.create_task(self.trigger_replacement(idx))
             
        else:
            # 其他网络错误
            state["weight"] -= 10.0
            state["failures"] += 1

    async def trigger_replacement(self, idx: int):
        """资源枯竭时的自动补货 Hook"""
        if not AUTO_REPLACEMENT_WEBHOOK: return
        try:
            slot_data = self.slots[idx]
            dead_key = slot_data.get("key", "unknown")[:10] + "..."
            payload = {
                "event": "resource_dead",
                "slot_index": idx,
                "key_preview": dead_key,
                "reason": "403_forbidden"
            }
            async with aiohttp.ClientSession() as session:
                await session.post(AUTO_REPLACEMENT_WEBHOOK, json=payload)
            logger.info(f"Triggered replacement webhook for Slot {idx}")
        except Exception as e:
            logger.error(f"Webhook failed: {e}")

# 初始化管理器
slot_manager = SlotManager()

# --- 2. 仿真与熵增逻辑 ---

async def simulate_human_typing(prompt_text: str):
    """
    模拟人类键入节奏 (Behavioral Fingerprinting)
    根据 prompt 长度计算“思考+打字”时间，并引入随机抖动
    """
    if not prompt_text: return
    length = len(prompt_text)
    # 假设平均打字速度 + 思考停顿
    # 极短文本也要有基础延迟，防止被识别为机器瞬间请求
    base_delay = min(3.0, length * 0.02) 
    jitter = random.uniform(0.1, 0.5)
    total_delay = base_delay + jitter
    
    # 只有当 delay 比较大时才真的 sleep，避免影响太久
    if total_delay > 0.5:
        await asyncio.sleep(total_delay)

def get_ja3_perturbed_impersonate(base_impersonate: str) -> str:
    """
    JA3 动态扰动
    通过在合法范围内微调浏览器版本，改变 TLS 扩展字段的排列组合
    """
    # 如果配置里指定了特定版本，尝试在该系内微调
    if "chrome" in base_impersonate:
        # 随机返回一个 Chrome 系的高版本指纹
        return random.choice([v for v in IMPERSONATE_VARIANTS if "chrome" in v])
    elif "safari" in base_impersonate:
        return random.choice([v for v in IMPERSONATE_VARIANTS if "safari" in v])
    return base_impersonate # Fallback

def shuffle_headers(headers: OrderedDict) -> OrderedDict:
    """
    Header 顺序混淆
    HTTP/2 的指纹识别中，Header 的顺序也是特征之一。
    在保持关键 Header 不变的前提下，微调其他 Header 顺序。
    """
    items = list(headers.items())
    # 保持 Host, Key, Content-Type 在前，其他随机
    fixed_keys = ["Host", "X-Goog-Api-Key", "Content-Type"]
    fixed = [item for item in items if item[0] in fixed_keys]
    others = [item for item in items if item[0] not in fixed_keys]
    random.shuffle(others)
    return OrderedDict(fixed + others)

# --- 3. 核心流式处理 (智能心跳) ---

async def smart_frame_processor(resp: AsyncSession, session_id: str) -> AsyncGenerator[str, None]:
    """
    具备智能心跳的流处理器
    根据上游响应间隔，动态调整心跳频率
    """
    buffer = b""
    iterator = resp.aiter_content(chunk_size=None).__aiter__()
    
    # 初始心跳间隔
    dynamic_timeout = 10.0
    last_chunk_time = time.time()
    
    while True:
        try:
            # 等待数据
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=dynamic_timeout)
            
            # 收到数据，重置计时器，并根据网络状况微调心跳
            now = time.time()
            gap = now - last_chunk_time
            last_chunk_time = now
            
            # 如果上游响应很快，心跳间隔可以适当放宽；如果慢，收紧心跳
            if gap < 2.0: dynamic_timeout = 15.0
            else: dynamic_timeout = 8.0 # 上游卡顿时，增加心跳频率保活
            
            buffer += chunk
            while FRAME_DELIMITER in buffer:
                line, buffer = buffer.split(FRAME_DELIMITER, 1)
                if not line.strip(): continue
                
                # Protocol logic (Mocked for brevity)
                yield f"data: {line.decode()}\n\n"
                
        except asyncio.TimeoutError:
            # 触发智能心跳
            yield ": keep-alive\n\n"
            continue
        except StopAsyncIteration:
            break
        except Exception:
            break

    if buffer.strip():
        yield f"data: {buffer.decode()}\n\n"
    yield "data: [DONE]\n\n"


# --- 4. FastAPI Setup ---

app = FastAPI(title="Gemini Tactical Gateway")
Instrumentator().instrument(app).expose(app)

# Redis Helper
async def get_redis():
    global REDIS_CLIENT
    if not REDIS_CLIENT:
        REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    return REDIS_CLIENT

@app.on_event("startup")
async def startup():
    slot_manager.load_config()
    await get_redis()
    signal.signal(signal.SIGHUP, lambda s, f: slot_manager.load_config())

# --- 5. 主入口 ---

@app.post("/v1/chat/completions")
async def tactical_proxy(request: Request):
    
    # Auth
    if GATEWAY_SECRET and request.headers.get("Authorization") != f"Bearer {GATEWAY_SECRET}":
        raise HTTPException(401, "Unauthorized")

    try:
        body = await request.json()
        prompt = str(body) # 简化提取
    except:
        raise HTTPException(400, "Bad JSON")

    redis = await get_redis()
    
    # 1. 行为仿真：人类键入延迟
    # 在获取 Slot 之前就进行延迟，模拟用户思考时间
    await simulate_human_typing(prompt)

    # 2. 智能调度获取资源
    slot_idx = await slot_manager.get_best_slot(redis)
    slot = slot_manager.slots[slot_idx]
    
    try:
        # 3. 战术配置构建
        # A. 基础配置
        key = slot["key"]
        proxy = slot.get("proxy")
        base_impersonate = slot.get("impersonate", "chrome110")
        
        # B. 深度混淆 (Deep Obfuscation)
        # JA3 扰动：微调 impersonate 版本
        final_impersonate = get_ja3_perturbed_impersonate(base_impersonate)
        
        # C. 地理位置与 Header 全套伪装 (Localization)
        # 从 Config 中读取绑定好的 Headers (Accept-Language, Timezone 等)
        geo_headers = slot.get("headers", {}) 
        
        request_headers = OrderedDict([
            ("Host", "gemini-cli-backend.googleapis.com"),
            ("X-Goog-Api-Key", key),
            ("Content-Type", "application/json"),
            # User-Agent 会由 curl_cffi 根据 impersonate 自动生成，这里不覆盖以防冲突
        ])
        request_headers.update(geo_headers) # 注入地域 Header
        
        # Header 顺序打乱
        request_headers = shuffle_headers(request_headers)
        
        proxies = {"http": proxy, "https": proxy} if proxy else None

        logger.info(f"Slot {slot_idx} Active | Impersonate: {final_impersonate} | Locale: {geo_headers.get('Accept-Language')}")

        # 4. 建立连接 (AsyncSession)
        async with AsyncSession(
            impersonate=final_impersonate,
            proxies=proxies,
            timeout=120
        ) as session:
            
            # Payload 转换 (这里简化，实际需复用之前的 transform 逻辑)
            # cli_payload = await transform_standard_to_cli(body, ...)
            # Mock Payload for compilation
            cli_payload = json.dumps({"contents": [{"parts": [{"text": "Hello"}]}]}).encode()

            resp = await session.post(
                UPSTREAM_URL,
                headers=request_headers,
                data=cli_payload,
                stream=True
            )

            # 5. 反馈回路
            await slot_manager.report_status(slot_idx, resp.status_code)
            
            if resp.status_code in [403, 429]:
                 raise HTTPException(status_code=resp.status_code, detail="Tactical Backoff Triggered")
            
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail="Upstream Error")

            # 6. 成功 - 释放并发锁并流式输出
            # 注意：这里选择在流结束后释放并发可能更精确，但为了高吞吐，此处选择立即释放
            await slot_manager.release_slot(slot_idx, redis)
            
            return StreamingResponse(
                smart_frame_processor(resp, "sess_id"),
                media_type="text/event-stream"
            )

    except Exception as e:
        # 异常处理：释放资源并记录
        await slot_manager.release_slot(slot_idx, redis)
        await slot_manager.report_status(slot_idx, 500) # 内部或网络错误降权
        logger.error(f"Proxy failed: {e}")
        raise HTTPException(status_code=502, detail="Gateway Error")
