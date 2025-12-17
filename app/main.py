import json
import os
import uuid
import random
import logging
import asyncio
import signal
import time
import hashlib
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
MAX_RETRIES = 3            # 最大故障重试次数
CIRCUIT_BREAKER_TIME = 60  # 熔断冷却时间 (秒)
KEEP_ALIVE_INTERVAL = 15   # 流式心跳间隔 (秒)
DEFAULT_CONCURRENCY = 5    # 单 Key 默认并发限制

# 安全认证 Token (为空则不开启)
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET")

# --- 1. 资源管理与热重载 ---

def load_resource_pool():
    """加载配置并初始化状态"""
    global RESOURCE_POOL, SLOT_STATE
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                new_pool = json.load(f)
            
            # 初始化运行时状态 (保留原有状态或重置)
            RESOURCE_POOL = new_pool
            # 为每个 Slot 建立索引 ID，方便状态追踪
            for idx, slot in enumerate(RESOURCE_POOL):
                if idx not in SLOT_STATE:
                    SLOT_STATE[idx] = {
                        "failures": 0,
                        "next_retry_ts": 0,
                        "concurrency_key": hashlib.md5(slot['key'].encode()).hexdigest()
                    }
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
    1. 过滤掉处于熔断期 (Cool-down) 的 Slot。
    2. 检查 Redis 并发计数 (Soft Limit)。
    3. 从剩余可用 Slot 中随机选择。
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
        # 如果全部熔断，尝试强制复活一个等待时间最短的
        logger.warning("All slots circuit-broken. Forcing retry on earliest.")
        return min(range(len(RESOURCE_POOL)), key=lambda i: SLOT_STATE[i]["next_retry_ts"])

    # 第二轮筛选：Redis 并发限制 (为了性能，随机选几个检查，而不是全部检查)
    # 避免惊群效应和 Redis 压力，我们随机打散检查顺序
    random.shuffle(healthy_indices)
    
    selected_idx = -1
    
    for idx in healthy_indices:
        state = SLOT_STATE[idx]
        conc_key = f"concurrency:{state['concurrency_key']}"
        limit = RESOURCE_POOL[idx].get("max_concurrency", DEFAULT_CONCURRENCY)
        
        # 检查当前并发
        current = await redis.get(conc_key)
        if current and int(current) >= limit:
            continue # 该 Key 太忙，跳过
            
        selected_idx = idx
        break
    
    # 如果所有健康的都满了，随机回退到一个健康的（降级策略）
    if selected_idx == -1:
        selected_idx = random.choice(healthy_indices)

    # 预增加并发计数 (有效期 60秒，防止死锁)
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

def mark_slot_failure(idx: int):
    """触发熔断机制"""
    state = SLOT_STATE[idx]
    state["failures"] += 1
    # 指数退避：失败次数越多，冷却越久 (60s, 120s, 180s...)
    backoff = CIRCUIT_BREAKER_TIME * min(state["failures"], 5) 
    state["next_retry_ts"] = time.time() + backoff
    logger.warning(f"Slot {idx} Circuit Broken! Cooling down for {backoff}s. (Failures: {state['failures']})")

def mark_slot_success(idx: int):
    """恢复健康状态"""
    state = SLOT_STATE[idx]
    if state["failures"] > 0:
        state["failures"] = 0
        state["next_retry_ts"] = 0

# --- 4. 协议处理 (Keep-Alive & Optimize) ---

async def frame_processor(resp: AsyncSession, session_id: str) -> AsyncGenerator[str, None]:
    """
    增强版流处理器：
    1. 智能拆包
    2. Keep-Alive 心跳保活
    """
    buffer = b""
    iterator = resp.aiter_content(chunk_size=None).__aiter__() # 让 curl_cffi 自动管理最佳 chunk size
    
    while True:
        try:
            # 关键：使用 wait_for 实现心跳机制
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=KEEP_ALIVE_INTERVAL)
            buffer += chunk
            
            while FRAME_DELIMITER in buffer:
                line, buffer = buffer.split(FRAME_DELIMITER, 1)
                if not line.strip(): continue
                
                # 此处调用之前的转换逻辑 (为节省篇幅假设 transform_cli_to_standard 已定义)
                transformed = await transform_cli_to_standard(line, session_id) 
                if transformed:
                    yield f"data: {json.dumps(transformed)}\n\n"
                    
        except asyncio.TimeoutError:
            # 超时未收到上游数据，发送 SSE 注释作为心跳 (Keep-Alive)
            # 这不会破坏 JSON 结构，但能保持 HTTP 连接活跃
            yield ": keep-alive\n\n"
            continue
        except StopAsyncIteration:
            break
        except Exception as e:
            logger.error(f"Stream error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            break

    # 处理剩余 buffer
    if buffer.strip():
        transformed = await transform_cli_to_standard(buffer, session_id)
        if transformed:
            yield f"data: {json.dumps(transformed)}\n\n"
            
    yield "data: [DONE]\n\n"


# --- 5. 辅助功能 (Tool Logic 复用原代码) ---
# ... (此处省略 record_tool_calls, check_and_get_pending_calls 等 Redis 逻辑，与之前一致) ...
# 为了完整性，这里必须包含这些函数，请将之前提供的 transform_standard_to_cli 等函数粘贴于此
# ---------------------------------------------------------------------------
async def get_session_id_from_request(request: Request) -> str:
    # ... (保持不变) ...
    return request.headers.get("X-Session-ID") or str(uuid.uuid4())

async def transform_standard_to_cli(data: dict, session_id: str) -> bytes:
    # ... (保持不变) ...
    # 简化的 Mock，实际请使用之前提供的完整逻辑
    return json.dumps({"contents": [{"parts": [{"text": data.get("prompt", "")}]}]}).encode()

async def transform_cli_to_standard(raw_line: bytes, session_id: str) -> dict:
    # ... (保持不变) ...
    # 简化的 Mock，实际请使用之前提供的完整逻辑
    try:
        return {"choices": [{"delta": {"content": "..."}}]}
    except: return {}
# ---------------------------------------------------------------------------


# --- 6. FastAPI 应用入口 ---

app = FastAPI(title="Gemini Industrial Gateway")
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
    logger.info(">>> Gateway Started. Monitoring Signals...")

@app.post("/v1/chat/completions", dependencies=[Depends(verify_auth)])
async def reverse_proxy_endpoint(request: Request):
    
    # 1. 预处理请求
    try:
        body = await request.json()
        session_id = await get_session_id_from_request(request)
        cli_body = await transform_standard_to_cli(body, session_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Bad Request: {e}")

    # 2. 自动故障转移循环 (Failover Loop)
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        slot_idx = -1
        try:
            # 3. 获取资源槽 (包含软限流逻辑)
            slot_idx = await acquire_slot()
            slot = RESOURCE_POOL[slot_idx]
            
            current_impersonate = slot.get("impersonate", "chrome110")
            current_proxy = slot.get("proxy")
            current_key = slot.get("key")
            
            proxies = {"http": current_proxy, "https": current_proxy} if current_proxy else None
            
            logger.info(f"[Request] Sess:{session_id} | Try:{attempt+1}/{MAX_RETRIES} | Slot:{slot_idx} ({current_impersonate})")

            # 4. 发起请求
            async with AsyncSession(
                impersonate=current_impersonate,
                proxies=proxies,
                timeout=120 # 总超时
            ) as session:
                
                headers = OrderedDict([
                    ("Host", "gemini-cli-backend.googleapis.com"),
                    ("X-Goog-Api-Key", current_key), 
                    ("User-Agent", "Gemini-CLI/1.0"), 
                    ("Content-Type", "application/json"),
                ])

                resp = await session.post(
                    url=UPSTREAM_URL,
                    headers=headers,
                    data=cli_body,
                    stream=True
                )

                # 5. 状态检查
                if resp.status_code == 403 or resp.status_code == 429:
                    # 关键：鉴权失败或限流，立即熔断该 Slot
                    logger.warning(f"Slot {slot_idx} returned {resp.status_code}. Marking as failed.")
                    mark_slot_failure(slot_idx)
                    raise HTTPException(status_code=resp.status_code, detail="Auth/Rate Limit")
                
                if resp.status_code != 200:
                    raise HTTPException(status_code=resp.status_code, detail="Upstream Error")

                # 6. 请求成功，标记状态并开始流式传输
                mark_slot_success(slot_idx)
                
                # 注意：这里我们立即返回 StreamingResponse，它会在后台运行 frame_processor
                # 并发计数的 release 稍微有点复杂，因为 streaming 是异步的
                # 为了简化，我们在“连接建立成功”后就释放并发计数(假设长连接占用不计入高频 API 限制)，
                # 或者您可以选择在 generator 结束时释放。这里选择立即释放以允许吞吐。
                await release_slot(slot_idx) 
                
                return StreamingResponse(
                    frame_processor(resp, session_id),
                    media_type="text/event-stream"
                )

        except Exception as e:
            # 捕获所有错误（网络、超时、HTTP错误）
            last_error = e
            logger.error(f"Attempt {attempt+1} failed: {e}")
            if slot_idx != -1:
                # 释放并发计数
                await release_slot(slot_idx)
                # 如果是网络层面的报错（非 403），也可以选择性熔断
                if isinstance(e, (asyncio.TimeoutError)):
                     mark_slot_failure(slot_idx)
            
            # 如果还有重试机会，稍微等待一下避免瞬间雪崩
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(0.5, 1.5))
            continue

    # 如果重试耗尽
    raise HTTPException(status_code=502, detail=f"All upstream retries failed. Last error: {str(last_error)}")
