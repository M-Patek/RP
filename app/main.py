import json
import os
import uuid
import random
import logging
import asyncio
from typing import AsyncGenerator, Optional, Dict, Any, List
from collections import OrderedDict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from curl_cffi.requests import AsyncSession
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 全局配置 ---
app = FastAPI(title="Gemini Advanced Gateway (Async & Slot-Based)")

# 初始化 Prometheus
Instrumentator().instrument(app).expose(app)

# 1. 资源池加载 (Slot Management)
# 在启动时加载 config.json，实现配置解耦
RESOURCE_POOL: List[Dict] = []

def load_resource_pool():
    global RESOURCE_POOL
    config_path = "config.json"
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                RESOURCE_POOL = json.load(f)
            logger.info(f"Loaded {len(RESOURCE_POOL)} resource slots from {config_path}")
        else:
            logger.warning(f"Config file {config_path} not found! Falling back to ENV variables.")
            # 只有在找不到配置文件时才回退到环境变量
            fallback_key = os.getenv("GEMINI_API_KEY")
            if fallback_key:
                RESOURCE_POOL.append({
                    "key": fallback_key,
                    "proxy": None,
                    "impersonate": "chrome110"
                })
    except Exception as e:
        logger.error(f"Failed to load resource pool: {e}")
        raise RuntimeError("Configuration loading failed.")

# 2. Redis 配置
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

REDIS_CLIENT: Optional[AsyncRedis] = None

# 3. 协议常量
FRAME_DELIMITER = b"\n"
SESSION_TTL = 300
UPSTREAM_URL = "https://gemini-cli-backend.googleapis.com/v1/generate"

# --- Redis 辅助函数 ---

async def get_redis_client():
    global REDIS_CLIENT
    if REDIS_CLIENT is None:
        if not REDIS_PASSWORD:
             logger.warning("Starting without Redis Password (NOT RECOMMENDED for production)")
        try:
            REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, 
                                      password=REDIS_PASSWORD, decode_responses=True)
            await REDIS_CLIENT.ping()
        except Exception as e:
            logger.error(f"Redis connection error: {e}")
            raise HTTPException(status_code=503, detail="Redis unavailable")
    return REDIS_CLIENT

async def get_session_id_from_request(request: Request) -> str:
    session_id = request.headers.get("X-Session-ID")
    if session_id: return session_id
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header.split(" ")[1]
    return str(uuid.uuid4())

async def record_tool_calls(session_id: str, tool_calls: list):
    """异步记录工具调用状态"""
    client = await get_redis_client()
    if not tool_calls: return
    pending_calls = {call['id']: json.dumps(call['function']) for call in tool_calls}
    if pending_calls:
        # 使用 pipeline 减少网络往返
        async with client.pipeline() as pipe:
            await pipe.hset(f"session:{session_id}:pending", mapping=pending_calls)
            await pipe.expire(f"session:{session_id}:pending", SESSION_TTL)
            await pipe.execute()

async def check_and_get_pending_calls(session_id: str) -> Dict[str, str]:
    client = await get_redis_client()
    return await client.hgetall(f"session:{session_id}:pending")

# --- 协议转换逻辑 (保持业务逻辑不变，确保纯异步) ---

async def transform_standard_to_cli(data: dict, session_id: str) -> bytes:
    try:
        tool_outputs = data.get("tool_outputs")
        if tool_outputs:
            pending_calls = await check_and_get_pending_calls(session_id)
            client = await get_redis_client()
            tool_output_parts = []
            
            # 批量删除已处理的 key，优化 redis 性能
            processed_ids = []
            
            for output in tool_outputs:
                call_id = output.get("tool_call_id")
                if call_id in pending_calls:
                    function_info = json.loads(pending_calls[call_id])
                    tool_output_parts.append({
                        "functionResponse": {
                            "name": function_info['name'],
                            "response": output.get("output")
                        }
                    })
                    processed_ids.append(call_id)
            
            if processed_ids:
                 await client.hdel(f"session:{session_id}:pending", *processed_ids)

            cli_payload = {
                "contents": [{"parts": tool_output_parts, "role": "tool"}]
            }
        else:
            cli_payload = {
                "contents": [{"parts": [{"text": data.get("prompt", "")}]}],
                "generationConfig": {"temperature": data.get("temperature", 0.7)}
            }
        return json.dumps(cli_payload).encode()
    except Exception as e:
        logger.error(f"Transform Error: {e}")
        raise HTTPException(status_code=400, detail="Invalid request format")

async def transform_cli_to_standard(raw_line: bytes, session_id: str) -> dict:
    try:
        data = json.loads(raw_line)
        candidate = data.get("candidates", [{}])[0]
        
        # 处理 Tool Calls
        cli_tool_calls = candidate.get("tool_calls")
        if cli_tool_calls:
            await record_tool_calls(session_id, cli_tool_calls)
            return {"choices": [{"delta": {"tool_calls": cli_tool_calls}}], "action": "tool_required"}

        # 处理 Grounding Metadata
        metadata = data.get("grounding_metadata") 
        
        # 处理文本内容
        content = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")
        
        transformed = {"choices": [{"delta": {"content": content}}]}
        if metadata: transformed['grounding_metadata'] = metadata
        if candidate.get('finish_reason'): transformed['choices'][0]['finish_reason'] = candidate['finish_reason']
        
        return transformed
    except json.JSONDecodeError: return {}
    except Exception as e: return {"error": {"message": str(e)}}

# --- 流式处理生成器 ---

async def frame_processor(resp, session_id) -> AsyncGenerator[str, None]:
    buffer = b""
    try:
        # 这里的 resp.aiter_content 是 curl_cffi 的异步迭代器
        async for chunk in resp.aiter_content(chunk_size=1024):
            buffer += chunk
            while FRAME_DELIMITER in buffer:
                line, buffer = buffer.split(FRAME_DELIMITER, 1)
                if not line.strip(): continue
                
                transformed = await transform_cli_to_standard(line, session_id) 
                if transformed:
                     yield f"data: {json.dumps(transformed)}\n\n"
        
        if buffer.strip():
             transformed = await transform_cli_to_standard(buffer, session_id)
             if transformed:
                 yield f"data: {json.dumps(transformed)}\n\n"
                 
        yield "data: [DONE]\n\n"

    except Exception as e:
        logger.error(f"Stream interrupted: {e}")
        yield f"data: {json.dumps({'error': str(e)})}\n\n"

# --- 生命周期 ---

@app.on_event("startup")
async def startup_event():
    await get_redis_client()
    load_resource_pool()
    logger.info("System Ready. All systems async.")

@app.on_event("shutdown")
async def shutdown_event():
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
    logger.info("System Shutdown.")

# --- 核心入口 ---

@app.post("/v1/chat/completions")
async def reverse_proxy_endpoint(request: Request):
    
    if not RESOURCE_POOL:
        raise HTTPException(status_code=503, detail="No resources available in pool.")

    # 1. 资源调度策略：随机 (负载均衡)
    # 主人可以根据需要改为 Round-Robin 或 基于权重的选择
    slot = random.choice(RESOURCE_POOL)
    
    current_key = slot.get("key")
    current_proxy = slot.get("proxy")
    current_impersonate = slot.get("impersonate", "chrome110")
    
    # 构造代理字典 (curl_cffi 格式)
    proxies = None
    if current_proxy:
        proxies = {"http": current_proxy, "https": current_proxy}

    # 2. 准备请求数据
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    session_id = await get_session_id_from_request(request)
    cli_body = await transform_standard_to_cli(body, session_id)
    
    headers = OrderedDict([
        ("Host", "gemini-cli-backend.googleapis.com"),
        ("X-Goog-Api-Key", current_key), 
        ("User-Agent", "Gemini-CLI/1.0 (Go-http-client/1.1)"), 
        ("Content-Type", "application/json"),
    ])

    logger.info(f"Session {session_id} -> Slot used: {current_impersonate} | Proxy: {current_proxy is not None}")

    # 3. 动态异步会话 (核心重构点)
    # 彻底移除了 ThreadPoolExecutor，使用原生 AsyncSession
    try:
        # 注意：这里我们使用 context manager，确保请求结束后连接被正确归还/关闭
        # curl_cffi 的 AsyncSession 是非阻塞的，完美适配 FastAPI
        async with AsyncSession(
            impersonate=current_impersonate,
            proxies=proxies,
            timeout=120 # 对于流式传输，设置较长的总超时时间
        ) as session:
            
            resp = await session.post(
                url=UPSTREAM_URL,
                headers=headers,
                data=cli_body,
                stream=True 
            )
            
            if resp.status_code != 200:
                error_content = await resp.content.read()
                logger.error(f"Upstream Fail ({resp.status_code}): {error_content.decode()}")
                raise HTTPException(status_code=resp.status_code, detail=f"Upstream provider error: {resp.status_code}")

            # 4. 建立流式管道
            return StreamingResponse(
                frame_processor(resp, session_id),
                media_type="text/event-stream"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Proxy internal error: {e}")
        raise HTTPException(status_code=500, detail="Internal proxy error during execution")
