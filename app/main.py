import json
import os
import random
import logging
import asyncio
import signal
import time
import hashlib
import secrets
import uuid  # [Restored] 恢复引用
from typing import AsyncGenerator, Optional, Dict, List
from collections import OrderedDict
from contextlib import asynccontextmanager # [Restored] 恢复引用

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from curl_cffi.requests import AsyncSession # [Cloud] 核心: 使用 curl_cffi 抗指纹
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeminiTactical-Cloud")

# --- 全局状态与配置 ---
CONFIG_PATH = "config.json"
AUTO_REPLACEMENT_WEBHOOK = os.getenv("AUTO_REPLACEMENT_WEBHOOK")
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET")

# --- Redis 配置 (Cloud) ---
REDIS_HOST = "redis"  # [Cloud] Docker 内部服务名
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_CLIENT: Optional[AsyncRedis] = None

# --- 常量定义 ---
FRAME_DELIMITER = b"\n"
# [Unified] 统一使用 Google 官方 API 地址
UPSTREAM_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
DEFAULT_CONCURRENCY = 5
MAX_BUFFER_SIZE = 1024 * 1024 

# Lua 脚本：原子性并发控制 (保持不变)
LUA_ACQUIRE_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local current = tonumber(redis.call('get', key) or "0")

if current >= limit then
    return -1
else
    local new_val = redis.call('incr', key)
    redis.call('expire', key, ttl)
    return new_val
end
"""

# --- 指纹与仿真库 (Cloud) ---
IMPERSONATE_VARIANTS = [
    "chrome110", "chrome111", "chrome112", 
    "safari15_5", "safari16_0",
    "edge101", "edge103"
]

class SlotManager:
    def __init__(self):
        self.slots = []
        self.states = {} 
        self.lock = asyncio.Lock() # [Restored] 恢复锁对象

    def load_config(self):
        """
        加载配置文件，支持环境变量展开和 JSON 容错
        """
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    raw_content = f.read()
                    # 关键: 解析环境变量占位符 (如 ${API_KEY})
                    expanded_content = os.path.expandvars(raw_content)
                    new_slots = json.loads(expanded_content)
                
                if not isinstance(new_slots, list):
                    raise ValueError("Config must be a list of slots")

                self.slots = new_slots
                
                # 初始化状态
                for idx, slot in enumerate(self.slots):
                    # 检查 Key 是否有效（防止环境变量未注入）
                    if 'key' not in slot or not slot['key'] or slot['key'].startswith("$"): 
                        logger.warning(f"[Config] Slot {idx} missing 'key' or env var not set, skipping.")
                        continue
                        
                    key_hash = hashlib.md5(slot['key'].encode()).hexdigest()
                    if idx not in self.states:
                        self.states[idx] = {
                            "failures": 0,
                            "weight": 100.0,
                            "concurrency_key": key_hash,
                            "cool_down_until": 0,
                            "last_used_ts": 0
                        }
                logger.info(f"[Config] Successfully loaded {len(self.slots)} slots.")
            else:
                logger.warning("[Config] No config found.")
                if not self.slots: self.slots = []

        except json.JSONDecodeError as e:
            # [Restored] 恢复对 JSON 格式错误的详细捕获
            logger.error(f"[Config] JSON Syntax Error: {e}. Keeping previous configuration.")
        except Exception as e:
            logger.error(f"[Config] Load failed: {e}")

    async def get_best_slot(self, redis_client: AsyncRedis) -> int:
        if not self.slots: raise HTTPException(status_code=503, detail="Resource pool empty or config invalid")
        
        candidates, weights, now = [], [], time.time()
        for idx, state in self.states.items():
            # 1. 冷却检查
            if state["cool_down_until"] > now: continue
            
            # 2. Redis 并发硬限制检查
            limit = self.slots[idx].get("max_concurrency", DEFAULT_CONCURRENCY)
            conc_key = f"concurrency:{state['concurrency_key']}"
            current_conc = await redis_client.get(conc_key)
            if current_conc and int(current_conc) >= limit: continue
            
            candidates.append(idx)
            weights.append(max(1.0, state["weight"]))

        if not candidates:
            # logger.warning("All slots busy or cooling down.") # 可选日志
            raise HTTPException(status_code=503, detail="No available slots (Busy/RateLimit)")

        # 3. 加权随机选择
        selected_idx = random.choices(candidates, weights=weights, k=1)[0]
        state = self.states[selected_idx]
        limit = self.slots[selected_idx].get("max_concurrency", DEFAULT_CONCURRENCY)
        conc_key = f"concurrency:{state['concurrency_key']}"
        
        try:
            # 4. 执行 Lua 脚本原子获取
            result = await redis_client.eval(LUA_ACQUIRE_SCRIPT, 1, conc_key, limit, 60)
            if result == -1:
                logger.warning(f"Slot {selected_idx} hit race condition limit.")
                raise HTTPException(status_code=503, detail="Slot busy (Race Condition)")
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            logger.error(f"Redis Acquire Error: {e}")
            raise HTTPException(status_code=500, detail="Internal Concurrency Error")
        
        return selected_idx

    async def release_slot(self, idx: int, redis_client: AsyncRedis):
        if idx not in self.states: return
        conc_key = f"concurrency:{self.states[idx]['concurrency_key']}"
        await redis_client.decr(conc_key)

    async def report_status(self, idx: int, status_code: int):
        state = self.states[idx]
        now = time.time()
        
        if status_code == 200:
            state["weight"] = min(100.0, state["weight"] + 5.0)
            state["failures"] = 0
        elif status_code == 429:
            logger.warning(f"Slot {idx} Hit 429. Dropping weight.")
            state["weight"] = max(1.0, state["weight"] - 50.0) 
            state["failures"] += 1
            # 指数退避
            state["cool_down_until"] = now + (30 * (2 ** min(state["failures"], 5)))
        elif status_code == 403: # Forbidden/Key Invalid
             logger.error(f"Slot {idx} Hit 403 (Possbile Ban).")
             state["weight"] = 0
             state["cool_down_until"] = now + 3600
             asyncio.create_task(self.trigger_replacement(idx))
        else:
            state["weight"] -= 10.0
            state["failures"] += 1

    async def trigger_replacement(self, idx: int):
        if not AUTO_REPLACEMENT_WEBHOOK: return
        # 这里预留了 Webhook 逻辑的位置，保持代码结构
        pass

slot_manager = SlotManager()

# --- 2. 仿真逻辑 (Cloud: 启用随机化) ---
def get_ja3_perturbed_impersonate(base_impersonate: str) -> str:
    # [Cloud] 保持指纹随机化，增加云端抗封锁能力
    if "chrome" in base_impersonate:
        return random.choice([v for v in IMPERSONATE_VARIANTS if "chrome" in v])
    elif "safari" in base_impersonate:
        return random.choice([v for v in IMPERSONATE_VARIANTS if "safari" in v])
    return base_impersonate

def shuffle_headers(headers: OrderedDict) -> OrderedDict:
    # 简单的洗牌逻辑
    items = list(headers.items())
    random.shuffle(items)
    return OrderedDict(items)

# --- 3. 核心流式处理 (Cloud: curl_cffi 版) ---
async def smart_frame_processor(resp: AsyncSession, session_id: str) -> AsyncGenerator[str, None]:
    """
    流式响应处理器：具备超时检测和缓冲区限制
    """
    buffer = b""
    # [Cloud] 使用 curl_cffi 的 aiter_content
    iterator = resp.aiter_content().__aiter__()
    
    # 动态超时控制
    dynamic_timeout = 10.0
    last_chunk_time = time.time()

    while True:
        try:
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=dynamic_timeout)
            
            # 更新心跳时间
            now = time.time()
            if (now - last_chunk_time) < 2.0: dynamic_timeout = 15.0
            else: dynamic_timeout = 8.0
            last_chunk_time = now

            buffer += chunk
            
            # DoS 防御
            if len(buffer) > MAX_BUFFER_SIZE:
                logger.warning(f"Session {session_id} exceeded buffer limit.")
                raise HTTPException(status_code=500, detail="Response too large")

            while FRAME_DELIMITER in buffer:
                line, buffer = buffer.split(FRAME_DELIMITER, 1)
                if not line.strip(): continue
                yield f"data: {line.decode('utf-8')}\n\n"
                
        except asyncio.TimeoutError:
            yield ": keep-alive\n\n"
            continue
        except StopAsyncIteration:
            break
        except Exception as e:
            if isinstance(e, HTTPException): yield f"data: [ERROR] {e.detail}\n\n"
            logger.error(f"Stream Error: {e}")
            break

    if buffer.strip():
        yield f"data: {buffer.decode('utf-8')}\n\n"
    yield "data: [DONE]\n\n"

# --- 4. FastAPI Setup ---
app = FastAPI(title="Gemini Tactical Gateway (Cloud)")
Instrumentator().instrument(app).expose(app)

async def get_redis():
    global REDIS_CLIENT
    if not REDIS_CLIENT:
        # [Cloud] 连接到 Docker 网络中的 'redis' 主机
        REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    return REDIS_CLIENT

@app.on_event("startup")
async def startup():
    slot_manager.load_config()
    await get_redis()
    # 监听重载信号 (SIGHUP)
    try:
        signal.signal(signal.SIGHUP, lambda s, f: slot_manager.load_config())
    except AttributeError:
        # Windows 不支持 SIGHUP，这里是为了本地测试兼容性
        pass

@app.post("/v1/chat/completions")
async def tactical_proxy(request: Request):
    # 1. 鉴权检查
    if GATEWAY_SECRET:
        auth = request.headers.get("Authorization") or ""
        if not secrets.compare_digest(auth, f"Bearer {GATEWAY_SECRET}"):
            raise HTTPException(401, "Unauthorized")

    # 2. 解析请求体
    try:
        body = await request.json()
    except:
        raise HTTPException(400, "Bad JSON")

    redis = await get_redis()
    
    # 3. 智能调度获取 Slot
    slot_idx = await slot_manager.get_best_slot(redis)
    slot = slot_manager.slots[slot_idx]
    
    try:
        # 4. 准备请求参数
        key = slot["key"]
        proxy = slot.get("proxy")
        # [Cloud] 启用指纹扰动
        final_impersonate = get_ja3_perturbed_impersonate(slot.get("impersonate", "chrome110"))
        
        request_headers = OrderedDict([
            ("Content-Type", "application/json"),
        ])
        if "headers" in slot: request_headers.update(slot["headers"])
        
        # [Cloud] 自动添加 API Key 到 URL 参数 (Google API 标准)
        url_with_key = f"{UPSTREAM_URL}?key={key}"
        proxies = {"http": proxy, "https": proxy} if proxy else None

        logger.info(f"Slot {slot_idx} Active | Impersonate: {final_impersonate}")

        # [Cloud] 使用 curl_cffi 发送请求
        async with AsyncSession(
            impersonate=final_impersonate,
            proxies=proxies,
            timeout=120
        ) as session:
            
            # [Unified] 透传 body, 使用 json 参数
            resp = await session.post(
                url_with_key,
                headers=request_headers,
                json=body, 
                stream=True
            )

            # [Unified] 错误处理增强：读取上游详细报错
            if resp.status_code != 200:
                error_text = await resp.text()
                await slot_manager.report_status(slot_idx, resp.status_code)
                
                # 400 包含 Key 失效的情况，403/429 是风控
                if resp.status_code in [403, 429, 400]:
                     logger.warning(f"Upstream Error ({resp.status_code}): {error_text}")
                     raise HTTPException(status_code=resp.status_code, detail=f"API Error ({resp.status_code}): {error_text}")
                
                raise HTTPException(status_code=resp.status_code, detail=f"Upstream Error: {error_text}")

            # 成功：汇报状态并释放锁
            await slot_manager.report_status(slot_idx, resp.status_code)
            await slot_manager.release_slot(slot_idx, redis)
            
            return StreamingResponse(
                smart_frame_processor(resp, "sess_id"),
                media_type="text/event-stream"
            )

    except Exception as e:
        # 异常兜底：确保释放锁
        await slot_manager.release_slot(slot_idx, redis)
        await slot_manager.report_status(slot_idx, 500)
        logger.error(f"Proxy failed: {e}")
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=502, detail="Gateway Error")
