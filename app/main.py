import json
import os
import uuid
import random
import logging
import asyncio
import signal
import time
import hashlib
import math
from typing import AsyncGenerator, Optional, Dict, List
from collections import OrderedDict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends, Header
from fastapi.responses import StreamingResponse, JSONResponse
from curl_cffi.requests import AsyncSession
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeminiGateway")

# --- 全局状态 ---
RESOURCE_POOL: List[Dict] = []  # 资源池
SLOT_STATE: Dict[int, Dict] = {} # 运行时状态：记录熔断、失败计数等
CONFIG_PATH = "config.json"

# --- Redis 配置 ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_CLIENT: Optional[AsyncRedis] = None

# --- 常量配置 ---
FRAME_DELIMITER = b"\n"
SESSION_TTL = 300
UPSTREAM_URL = "https://gemini-cli-backend.googleapis.com/v1/generate"
MAX_RETRIES = 3             # 最大故障重试次数
BASE_COOL_DOWN = 60         # 基础熔断冷却时间 (秒)
MAX_COOL_DOWN = 3600        # 最大熔断冷却时间 (1小时)
KEEP_ALIVE_INTERVAL = 15    # 流式心跳间隔 (秒)
DEFAULT_CONCURRENCY = 5     # 单 Key 默认并发限制
WARMUP_PERIOD = 300         # 预热期 (秒)，新 Slot 在此前期间并发受限

# 安全认证 Token (为空则不开启)
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET")

# --- 智能指纹库 (User-Agent 映射) ---
# 确保 UA 与 TLS 指纹 (impersonate) 严格对应，减少指纹冲突风险
UA_MAPPING = {
    "chrome110": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "chrome100": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "safari15_5": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "edge101": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53"
}

# --- 1. 资源管理与热重载 ---

def load_resource_pool():
    """加载配置并初始化状态"""
    global RESOURCE_POOL, SLOT_STATE
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                new_pool = json.load(f)
            
            # 初始化运行时状态
            RESOURCE_POOL = new_pool
            now = time.time()
            
            # 重新建立索引，保留旧状态或初始化新状态
            # 使用 Key 的 Hash 作为持久化标识，防止重载配置导致状态丢失
            new_slot_state = {}
            for idx, slot in enumerate(RESOURCE_POOL):
                key_hash = hashlib.md5(slot['key'].encode()).hexdigest()
                
                # 尝试从旧状态中恢复 (如果在运行中热重载)
                existing_state = None
                for old_idx, old_state in SLOT_STATE.items():
                    if old_state.get("concurrency_key") == key_hash:
                        existing_state = old_state
                        break
                
                if existing_state:
                    new_slot_state[idx] = existing_state
                else:
                    new_slot_state[idx] = {
                        "failures": 0,
                        "next_retry_ts": 0,
                        "concurrency_key": key_hash,
                        "enabled_ts": now  # 记录启用时间用于预热逻辑
                    }
            
            SLOT_STATE = new_slot_state
            logger.info(f"[Config] Loaded {len(RESOURCE_POOL)} slots. Hot reload complete.")
        else:
            logger.warning("[Config] config.json not found. Waiting for mount...")
            RESOURCE_POOL = []
    except Exception as e:
        logger.error(f"[Config] Load failed: {e}")

def handle_sighup(signum, frame):
    """SIGHUP 信号处理：热重载"""
    logger.info("[System] SIGHUP received. Reloading config...")
    load_resource_pool()

# 注册信号监听
signal.signal(signal.SIGHUP, handle_sighup)

# --- 2. Redis 连接管理 ---

async def get_redis_client():
    global REDIS_CLIENT
    if REDIS_CLIENT is None:
        try:
            REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, 
                                      password=REDIS_PASSWORD, decode_responses=True)
            await REDIS_CLIENT.ping()
        except Exception as e:
            logger.error(f"Redis Error: {e}")
            raise HTTPException(status_code=503, detail="Redis unavailable")
    return REDIS_CLIENT

# --- 3. 智能调度与并发控制 (核心逻辑) ---

async def acquire_slot() -> int:
    """
    智能选择一个健康的 Slot：
    1. 熔断过滤：跳过处于冷却期的 Slot。
    2. 预热限制：新 Slot 并发能力缓慢爬坡。
    3. 软限流：Redis 全局并发计数检查。
    """
    if not RESOURCE_POOL:
        raise HTTPException(status_code=503, detail="Resource pool is empty.")

    redis = await get_redis_client()
    now = time.time()
    healthy_indices = []

    # 第一轮筛选：本地熔断状态
    for idx, slot in enumerate(RESOURCE_POOL):
        state = SLOT_STATE.get(idx, {})
        if state.get("next_retry_ts", 0) > now:
            continue # 还在熔断冷却中
        healthy_indices.append(idx)

    if not healthy_indices:
        logger.warning("All slots circuit-broken. Forcing retry on earliest.")
        return min(range(len(RESOURCE_POOL)), key=lambda i: SLOT_STATE[i]["next_retry_ts"])

    # 第二轮筛选：Redis 并发限制 + 预热逻辑
    random.shuffle(healthy_indices)
    selected_idx = -1
    
    for idx in healthy_indices:
        state = SLOT_STATE[idx]
        conc_key = f"concurrency:{state['concurrency_key']}"
        
        # 获取配置的最大并发
        base_limit = RESOURCE_POOL[idx].get("max_concurrency", DEFAULT_CONCURRENCY)
        
        # --- 预热逻辑 (Warming Up) ---
        # 如果 Slot 刚启用不久，限制其最大并发数
        enabled_duration = now - state.get("enabled_ts", now)
        if enabled_duration < WARMUP_PERIOD:
            # 线性增长：从 1 到 base_limit
            warmup_factor = max(0.2, enabled_duration / WARMUP_PERIOD)
            effective_limit = max(1, int(base_limit * warmup_factor))
        else:
            effective_limit = base_limit
            
        # 检查当前并发
        current = await redis.get(conc_key)
        if current and int(current) >= effective_limit:
            continue # 该 Key 太忙 (或处于预热限制中)，跳过
            
        selected_idx = idx
        break
    
    # 降级策略：如果所有健康的都满了，随机选一个健康的（允许轻微超限）
    if selected_idx == -1:
        selected_idx = random.choice(healthy_indices)

    # 预增加并发计数
    conc_key = f"concurrency:{SLOT_STATE[selected_idx]['concurrency_key']}"
    await redis.incr(conc_key)
    await redis.expire(conc_key, 60) 
    
    return selected_idx

async def release_slot(idx: int):
    """释放并发计数"""
    if idx < 0 or idx >= len(RESOURCE_POOL): return
    redis = await get_redis_client()
    state = SLOT_STATE[idx]
    conc_key = f"concurrency:{state['concurrency_key']}"
    await redis.decr(conc_key)

def mark_slot_failure(idx: int, status_code: int = 0):
    """触发熔断机制 (指数退避)"""
    state = SLOT_STATE[idx]
    state["failures"] += 1
    
    # 指数退避算法：Failures越多，冷却越久
    # 429/403 惩罚更重
    multiplier = 2 if status_code in [429, 403] else 1
    backoff = min(MAX_COOL_DOWN, BASE_COOL_DOWN * (2 ** (state["failures"] - 1)) * multiplier)
    
    state["next_retry_ts"] = time.time() + backoff
    logger.warning(f"Slot {idx} Circuit Broken! Status: {status_code}. Cooling for {backoff}s. (Failures: {state['failures']})")

def mark_slot_success(idx: int):
    """恢复健康状态"""
    state = SLOT_STATE[idx]
    if state["failures"] > 0:
        state["failures"] = 0
        state["next_retry_ts"] = 0

# --- 4. 协议处理 (Keep-Alive & Optimize) ---

async def frame_processor(resp: AsyncSession, session_id: str) -> AsyncGenerator[str, None]:
    """
    增强版流处理器：Keep-Alive 心跳保活
    """
    buffer = b""
    iterator = resp.aiter_content(chunk_size=None).__aiter__()
    
    while True:
        try:
            # 关键：使用 wait_for 实现心跳机制
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=KEEP_ALIVE_INTERVAL)
            buffer += chunk
            
            while FRAME_DELIMITER in buffer:
                line, buffer = buffer.split(FRAME_DELIMITER, 1)
                if not line.strip(): continue
                
                transformed = await transform_cli_to_standard(line, session_id) 
                if transformed:
                    yield f"data: {json.dumps(transformed)}\n\n"
                    
        except asyncio.TimeoutError:
            # 发送 SSE 注释保活
            yield ": keep-alive\n\n"
            continue
        except StopAsyncIteration:
            break
        except Exception as e:
            logger.error(f"Stream error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            break

    if buffer.strip():
        transformed = await transform_cli_to_standard(buffer, session_id)
        if transformed:
            yield f"data: {json.dumps(transformed)}\n\n"
            
    yield "data: [DONE]\n\n"

# --- 5. 辅助功能 (Tool Logic) ---
# ... (Redis 状态逻辑省略，保持不变) ...
async def get_session_id_from_request(request: Request) -> str:
    return request.headers.get("X-Session-ID") or str(uuid.uuid4())

# Mock for compilation purposes, replace with actual logic
async def transform_standard_to_cli(data: dict, session_id: str) -> bytes:
    return json.dumps({"contents": [{"parts": [{"text": data.get("prompt", "")}]}]}).encode()

async def transform_cli_to_standard(raw_line: bytes, session_id: str) -> dict:
    try: return {"choices": [{"delta": {"content": "..."}}]} 
    except: return {}

# --- 6. 后台健康审计 ---

async def background_health_monitor():
    """定期审计 Slot 状态，自动报警"""
    while True:
        await asyncio.sleep(60) # 每分钟检查一次
        for idx, state in SLOT_STATE.items():
            if state["failures"] >= 5:
                logger.error(f"[Audit] Slot {idx} is UNHEALTHY! Continuous failures: {state['failures']}. Please check IP.")

# --- 7. FastAPI 应用入口 ---

app = FastAPI(title="Gemini Tactical Gateway")
Instrumentator().instrument(app).expose(app)

# 安全中间件
async def verify_auth(request: Request):
    if GATEWAY_SECRET:
        auth = request.headers.get("Authorization")
        if auth != f"Bearer {GATEWAY_SECRET}":
            raise HTTPException(status_code=401, detail="Unauthorized Gateway Access")

@app.on_event("startup")
async def startup_event():
    load_resource_pool()
    await get_redis_client()
    asyncio.create_task(background_health_monitor()) # 启动后台审计
    logger.info(">>> Tactical Gateway Started. Systems Online.")

@app.post("/v1/chat/completions", dependencies=[Depends(verify_auth)])
async def reverse_proxy_endpoint(request: Request):
    
    try:
        body = await request.json()
        session_id = await get_session_id_from_request(request)
        cli_body = await transform_standard_to_cli(body, session_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Bad Request: {e}")

    # 1. 流量碎片化 (Traffic Fragmentation)
    # 引入 50ms-200ms 的随机延迟，打破机器请求的机械节奏
    await asyncio.sleep(random.uniform(0.05, 0.2))

    last_error = None
    
    for attempt in range(MAX_RETRIES):
        slot_idx = -1
        try:
            slot_idx = await acquire_slot()
            slot = RESOURCE_POOL[slot_idx]
            
            # --- 动态行为仿真配置 ---
            current_impersonate = slot.get("impersonate", "chrome110")
            current_proxy = slot.get("proxy")
            current_key = slot.get("key")
            
            # 自动匹配 User-Agent
            user_agent = slot.get("user_agent") or UA_MAPPING.get(current_impersonate, UA_MAPPING["chrome110"])
            
            proxies = {"http": current_proxy, "https": current_proxy} if current_proxy else None
            
            logger.info(f"[Request] Sess:{session_id} | Try:{attempt+1} | Slot:{slot_idx} ({current_impersonate})")

            # 4. 发起请求
            async with AsyncSession(
                impersonate=current_impersonate,
                proxies=proxies,
                timeout=120
            ) as session:
                
                # --- 多维特征解耦 (Header Injection) ---
                # 注入 Slot 特定的 Header (如时区、语言) 以匹配 IP 地理位置
                custom_headers = slot.get("headers", {})
                
                headers = OrderedDict([
                    ("Host", "gemini-cli-backend.googleapis.com"),
                    ("X-Goog-Api-Key", current_key), 
                    ("User-Agent", user_agent), # 使用严格匹配的 UA
                    ("Content-Type", "application/json"),
                ])
                # 合并自定义 Headers (覆盖默认值)
                headers.update(custom_headers)

                resp = await session.post(
                    url=UPSTREAM_URL,
                    headers=headers,
                    data=cli_body,
                    stream=True
                )

                # 5. 状态检查与智能熔断
                if resp.status_code in [403, 429]:
                    logger.warning(f"Slot {slot_idx} denied ({resp.status_code}). Triggering Exponential Backoff.")
                    # 传递状态码以触发更严厉的熔断
                    mark_slot_failure(slot_idx, status_code=resp.status_code)
                    raise HTTPException(status_code=resp.status_code, detail="Auth/Rate Limit")
                
                if resp.status_code != 200:
                    raise HTTPException(status_code=resp.status_code, detail="Upstream Error")

                mark_slot_success(slot_idx)
                await release_slot(slot_idx) 
                
                return StreamingResponse(
                    frame_processor(resp, session_id),
                    media_type="text/event-stream"
                )

        except Exception as e:
            last_error = e
            logger.error(f"Attempt {attempt+1} failed: {e}")
            if slot_idx != -1:
                await release_slot(slot_idx)
                if isinstance(e, (asyncio.TimeoutError)):
                     mark_slot_failure(slot_idx)
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(0.5, 1.5))
            continue

    raise HTTPException(status_code=502, detail=f"All upstream retries failed. Last error: {str(last_error)}")
