import json
import os
import uuid
import random # 新增：用于随机选择资源
import logging
from typing import AsyncGenerator, Optional, Dict, Any, List
from collections import OrderedDict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from curl_cffi.requests import AsyncSession
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 全局配置 ---
app = FastAPI(title="Gemini Enhanced Gateway - Master Edition")

# 初始化 Prometheus
Instrumentator().instrument(app).expose(app)

# 1. 废弃全局 http_session，改为资源池配置
# 注意：在生产环境中，这个列表应该从数据库或配置文件加载喵！
# 格式：{"key": "API_KEY", "impersonate": "指纹", "proxy": "代理地址"}
RESOURCE_POOL = [
    {
        "key": os.getenv("GEMINI_API_KEY_1", "key_fallback_1"),
        "impersonate": "chrome110",
        "proxy": os.getenv("PROXY_URL_1") # 例如 http://user:pass@1.2.3.4:8080
    },
    {
        "key": os.getenv("GEMINI_API_KEY_2", "key_fallback_2"),
        "impersonate": "safari15_5", # 不同的指纹
        "proxy": os.getenv("PROXY_URL_2") # 建议绑定不同的出口IP
    },
    # 主人可以继续添加更多喵...
]

# 2. Redis 客户端配置
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
if not REDIS_PASSWORD:
    raise RuntimeError("CRITICAL: REDIS_PASSWORD environment variable is not set.")

REDIS_CLIENT: Optional[AsyncRedis] = None

# 3. 协议配置
FRAME_DELIMITER = b"\n"
SESSION_TTL = 300
UPSTREAM_URL = "https://gemini-cli-backend.googleapis.com/v1/generate"

# --- 辅助函数：Redis 状态管理 (保持不变) ---

async def get_redis_client():
    global REDIS_CLIENT
    if REDIS_CLIENT is None:
        try:
            REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, 
                                      password=REDIS_PASSWORD, decode_responses=True)
            await REDIS_CLIENT.ping()
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise HTTPException(status_code=503, detail="Redis connection failed")
    return REDIS_CLIENT

async def get_session_id_from_request(request: Request) -> str:
    # ... (保持原有的 session_id 提取逻辑) ...
    session_id = request.headers.get("X-Session-ID")
    if session_id: return session_id
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header.split(" ")[1]
    return str(uuid.uuid4())

async def record_tool_calls(session_id: str, tool_calls: list):
    # ... (保持原有的 Redis 记录逻辑) ...
    client = await get_redis_client()
    pending_calls = {call['id']: json.dumps(call['function']) for call in tool_calls}
    if pending_calls:
        await client.hset(f"session:{session_id}:pending", mapping=pending_calls)
        await client.expire(f"session:{session_id}:pending", SESSION_TTL)

async def check_and_get_pending_calls(session_id: str) -> Dict[str, str]:
    client = await get_redis_client()
    return await client.hgetall(f"session:{session_id}:pending")

# --- 协议转译函数 (保持不变) ---
# transform_standard_to_cli 和 transform_cli_to_standard 逻辑复用原来的即可
# 为了节省篇幅，这里假设这两个函数内容与原文件一致

async def transform_standard_to_cli(data: dict, session_id: str) -> bytes:
    # ... (此处复用原文件代码，处理 tool_outputs 注入) ...
    try:
        tool_outputs = data.get("tool_outputs")
        if tool_outputs:
            pending_calls = await check_and_get_pending_calls(session_id)
            client = await get_redis_client()
            tool_output_parts = []
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
                    await client.hdel(f"session:{session_id}:pending", call_id)
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
        logger.error(f"Protocol Transformation Error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid request format: {str(e)}")

async def transform_cli_to_standard(raw_line: bytes, session_id: str) -> dict:
    # ... (此处复用原文件代码，解析 CLI 响应) ...
    try:
        data = json.loads(raw_line)
        candidate = data.get("candidates", [{}])[0]
        cli_tool_calls = candidate.get("tool_calls")
        if cli_tool_calls:
            await record_tool_calls(session_id, cli_tool_calls)
            return {"choices": [{"delta": {"tool_calls": cli_tool_calls}}], "action": "tool_required"}
        metadata = data.get("grounding_metadata") 
        content = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")
        transformed = {"choices": [{"delta": {"content": content}}]}
        if metadata: transformed['grounding_metadata'] = metadata
        if candidate.get('finish_reason'): transformed['choices'][0]['finish_reason'] = candidate['finish_reason']
        return transformed
    except json.JSONDecodeError: return {}
    except Exception as e: return {"error": {"message": str(e)}}

# --- 流处理核心 ---

async def frame_processor(resp, session_id) -> AsyncGenerator[str, None]:
    buffer = b""
    try:
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
        logger.error(f"Stream Error: {e}")
        yield f"data: {json.dumps({'error': str(e)})}\n\n"

# --- 生命周期管理 ---

@app.on_event("startup")
async def startup_event():
    await get_redis_client()
    logger.info("[Startup] Redis connection initialized. Resource pool ready.")

@app.on_event("shutdown")
async def shutdown_event():
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
    logger.info("[Shutdown] Resources cleaned up.")

# --- 核心接口改造 ---

@app.post("/v1/chat/completions")
async def reverse_proxy_endpoint(request: Request):
    
    body = await request.json()
    session_id = await get_session_id_from_request(request)
    
    # --- 关键改造：资源选择逻辑 ---
    # 策略：简单的随机负载均衡 (也可以改成 Round Robin)
    selected_resource = random.choice(RESOURCE_POOL)
    
    current_key = selected_resource["key"]
    current_impersonate = selected_resource["impersonate"]
    current_proxy = selected_resource.get("proxy") # 可能为 None

    # 如果请求头强制指定了 key，则覆盖资源池（仅用于调试或特定用户）
    # upstream_key = request.headers.get("x-goog-api-key") or current_key 
    # 建议生产环境强制使用资源池的 key
    upstream_key = current_key

    if not upstream_key:
         raise HTTPException(status_code=500, detail="No available API Key in resource pool.")

    # 1. 构造伪装请求头
    headers = OrderedDict([
        ("Host", "gemini-cli-backend.googleapis.com"),
        ("X-Goog-Api-Key", upstream_key), 
        ("User-Agent", "Gemini-CLI/1.0 (Go-http-client/1.1)"), # 这里的 UA 最好根据 impersonate 动态调整
        ("Content-Type", "application/json"),
    ])

    # 2. 构造 Body
    cli_body = await transform_standard_to_cli(body, session_id)

    # 3. 动态创建 Session 发起请求
    # 使用 context manager 确保 session 用完即销毁，防止连接泄漏
    # 重点：每次请求通过 impersonate 参数模拟不同浏览器指纹，通过 proxy 参数绑定 IP
    try:
        async with AsyncSession(
            impersonate=current_impersonate,
            proxies={"http": current_proxy, "https": current_proxy} if current_proxy else None,
            timeout=60
        ) as session:
            
            logger.info(f"Session {session_id} using fingerprint: {current_impersonate} | Proxy: {current_proxy is not None}")
            
            resp = await session.post(
                url=UPSTREAM_URL,
                headers=headers,
                data=cli_body,
                stream=True
            )
            
            if resp.status_code != 200:
                error_content = await resp.content.read()
                logger.error(f"Upstream Error ({current_impersonate}): {resp.status_code} - {error_content.decode()}")
                raise HTTPException(status_code=resp.status_code, detail=f"Upstream Error: {resp.status_code}")

            # 4. 返回流式响应
            # 注意：frame_processor 是个 async generator，FastAPI 会自动处理流式返回
            return StreamingResponse(
                frame_processor(resp, session_id),
                media_type="text/event-stream"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Proxy Execution Failed: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Proxy Error: {str(e)}")
