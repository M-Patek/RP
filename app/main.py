import os
import random
import logging
import secrets
import signal
import time
import asyncio
from typing import AsyncGenerator, Optional
from collections import OrderedDict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from curl_cffi.requests import AsyncSession # [Cloud] 核心: 使用 curl_cffi 抗指纹
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# 导入核心模块
from app.core import (
    slot_manager, ProxyRequest, UPSTREAM_URL, 
    MAX_BUFFER_SIZE, FRAME_DELIMITER
)

# --- 日志配置 ---
logger = logging.getLogger("GeminiTactical-Cloud")

# --- 全局配置 ---
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET")
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_CLIENT: Optional[AsyncRedis] = None

# --- 指纹库 ---
IMPERSONATE_VARIANTS = [
    "chrome110", "chrome111", "chrome112", 
    "safari15_5", "safari16_0",
    "edge101", "edge103"
]

def get_ja3_perturbed_impersonate(base_impersonate: str) -> str:
    """[Cloud] 指纹随机化逻辑"""
    if "chrome" in base_impersonate:
        return random.choice([v for v in IMPERSONATE_VARIANTS if "chrome" in v])
    elif "safari" in base_impersonate:
        return random.choice([v for v in IMPERSONATE_VARIANTS if "safari" in v])
    return base_impersonate

# --- 核心流式处理 (修复了生命周期 Bug) ---
async def smart_frame_processor(
    session: AsyncSession, 
    resp: AsyncSession, 
    slot_idx: int, 
    redis: AsyncRedis
) -> AsyncGenerator[str, None]:
    """
    负责处理流式响应，并在结束时安全关闭 Session。
    """
    buffer = b""
    # 使用 curl_cffi 的 aiter_content
    iterator = resp.aiter_content().__aiter__()
    
    dynamic_timeout = 10.0
    last_chunk_time = time.time()

    try:
        while True:
            try:
                chunk = await asyncio.wait_for(iterator.__anext__(), timeout=dynamic_timeout)
                
                # 动态心跳
                now = time.time()
                if (now - last_chunk_time) < 2.0: dynamic_timeout = 15.0
                else: dynamic_timeout = 8.0
                last_chunk_time = now

                buffer += chunk
                
                # DoS 防御
                if len(buffer) > MAX_BUFFER_SIZE:
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
        
        if buffer.strip():
            yield f"data: {buffer.decode('utf-8')}\n\n"
        yield "data: [DONE]\n\n"

    except Exception as e:
        logger.error(f"Stream Error: {e}")
        if isinstance(e, HTTPException): 
            yield f"data: [ERROR] {e.detail}\n\n"
    finally:
        # 🌟 关键修复: 确保流结束或异常时关闭 Session，并释放 Redis 锁
        if session:
            await session.close()
        # 释放 Slot 并汇报成功 (流式只要能开始通常算成功，或者需要更细粒度的判断)
        # 这里简化为只要没抛出 HTTP 异常就算 200，实际可优化
        await slot_manager.report_status(slot_idx, 200)
        await slot_manager.release_slot(slot_idx, redis)
        logger.debug(f"Slot {slot_idx} released & Session closed.")


# --- FastAPI Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    slot_manager.load_config()
    global REDIS_CLIENT
    REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    # 监听重载信号 (Linux only)
    try:
        signal.signal(signal.SIGHUP, lambda s, f: slot_manager.load_config())
    except AttributeError:
        pass
    
    yield
    
    # Shutdown
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()

app = FastAPI(title="Gemini Tactical Gateway (Cloud)", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

@app.post("/v1/chat/completions")
async def tactical_proxy(request: Request, body: ProxyRequest):
    # 1. 鉴权
    if GATEWAY_SECRET:
        auth = request.headers.get("Authorization") or ""
        if not secrets.compare_digest(auth, f"Bearer {GATEWAY_SECRET}"):
            raise HTTPException(401, "Unauthorized")

    redis = REDIS_CLIENT
    
    # 2. 调度
    slot_idx = await slot_manager.get_best_slot(redis)
    slot = slot_manager.slots[slot_idx]
    
    # 3. 准备 Session (Cloud 版为了抗指纹，每个请求新建 Session)
    # 注意：不要使用 async with，因为要将 session 所有权移交给 StreamingResponse
    session = None
    try:
        key = slot["key"]
        proxy = slot.get("proxy")
        final_impersonate = get_ja3_perturbed_impersonate(slot.get("impersonate", "chrome110"))
        
        request_headers = OrderedDict([("Content-Type", "application/json")])
        if "headers" in slot: request_headers.update(slot["headers"])
        
        url_with_key = f"{UPSTREAM_URL}?key={key}"
        proxies = {"http": proxy, "https": proxy} if proxy else None

        logger.info(f"Slot {slot_idx} Active | Impersonate: {final_impersonate}")

        session = AsyncSession(
            impersonate=final_impersonate,
            proxies=proxies,
            timeout=120
        )
            
        # 发起请求
        resp = await session.post(
            url_with_key,
            headers=request_headers,
            json=body.model_dump(), # 使用 Pydantic 导出字典
            stream=True
        )

        # 错误速判 (非流式阶段的错误)
        if resp.status_code != 200:
            error_text = await resp.text()
            await session.close() # 立即关闭
            await slot_manager.report_status(slot_idx, resp.status_code)
            await slot_manager.release_slot(slot_idx, redis)
            
            if resp.status_code in [403, 429, 400]:
                 raise HTTPException(status_code=resp.status_code, detail=f"API Error: {error_text}")
            raise HTTPException(status_code=resp.status_code, detail=f"Upstream Error: {error_text}")

        # 成功连接，移交控制权
        return StreamingResponse(
            smart_frame_processor(session, resp, slot_idx, redis),
            media_type="text/event-stream"
        )

    except Exception as e:
        # 发生异常（如连接失败），手动清理
        if session: await session.close()
        await slot_manager.release_slot(slot_idx, redis)
        await slot_manager.report_status(slot_idx, 500)
        logger.error(f"Proxy Init Failed: {e}")
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=502, detail="Gateway Error")
