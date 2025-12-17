import json
import os
import uuid
import asyncio
import logging
from typing import AsyncGenerator, Optional, Dict, Any
from collections import OrderedDict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from curl_cffi.requests import AsyncSession
from redis.asyncio import Redis as AsyncRedis
from prometheus_fastapi_instrumentator import Instrumentator

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 全局配置与初始化 ---
app = FastAPI(title="Gemini Enhanced Gateway")

# 初始化 Prometheus 监控并暴露 /metrics 端点
Instrumentator().instrument(app).expose(app)

# 1. 声明全局 AsyncSession (在 startup 中初始化)
# 使用 AsyncSession 替代同步 Session，消除线程池
http_session: Optional[AsyncSession] = None

# 2. Redis 客户端 (模拟状态机)
REDIS_HOST = "redis" # 对应 docker-compose service name
REDIS_PORT = 6379
# 强制从环境变量获取密码
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
if not REDIS_PASSWORD:
    # 如果环境变量缺失，阻止服务不安全启动
    raise RuntimeError("CRITICAL: REDIS_PASSWORD environment variable is not set.")

REDIS_CLIENT: Optional[AsyncRedis] = None

# 3. 协议配置
FRAME_DELIMITER = b"\n"
SESSION_TTL = 300 # Tool Calling 状态的生存时间 (秒)
UPSTREAM_URL = "https://gemini-cli-backend.googleapis.com/v1/generate" # 逆向出的实际端点

# --- 辅助函数：Redis 状态管理 ---

async def get_redis_client():
    global REDIS_CLIENT
    if REDIS_CLIENT is None:
        try:
            # 使用 URL 方式连接 Redis
            REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, 
                                      password=REDIS_PASSWORD, decode_responses=True)
            await REDIS_CLIENT.ping()
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise HTTPException(status_code=503, detail="Redis connection failed")
    return REDIS_CLIENT

async def get_session_id_from_request(request: Request) -> str:
    """从请求头提取唯一的 Session ID，实现会话隔离"""
    # 优先尝试从请求头提取 Session ID
    session_id = request.headers.get("X-Session-ID")
    if session_id:
        return session_id
    
    # 其次尝试从 Authorization Bearer Token 提取
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        # 简单使用 Token 作为 Session ID (实际应验证 JWT)
        return auth_header.split(" ")[1]

    # 如果未提供，生成一个新的 UUID 以确保会话隔离
    return str(uuid.uuid4())

async def record_tool_calls(session_id: str, tool_calls: list):
    """记录模型发出的待处理工具调用 (Tool Calling Manager)"""
    client = await get_redis_client()
    pending_calls = {call['id']: json.dumps(call['function']) for call in tool_calls}
    if pending_calls:
        await client.hset(f"session:{session_id}:pending", mapping=pending_calls)
        await client.expire(f"session:{session_id}:pending", SESSION_TTL)

async def check_and_get_pending_calls(session_id: str) -> Dict[str, str]:
    """获取所有待处理的工具调用"""
    client = await get_redis_client()
    return await client.hgetall(f"session:{session_id}:pending")

# --- 协议转译函数 (增强异常处理) ---

async def transform_standard_to_cli(data: dict, session_id: str) -> bytes:
    """将标准 API 请求转换为 CLI 协议格式，并处理上下文压缩/Tool Output注入"""
    try:
        # 假设开发者提交的 body 中包含 tool_outputs 字段
        tool_outputs = data.get("tool_outputs")
        
        if tool_outputs:
            # --- [Tool Output 上下文注入逻辑] ---
            pending_calls = await check_and_get_pending_calls(session_id)
            client = await get_redis_client()
            
            tool_output_parts = []
            for output in tool_outputs:
                call_id = output.get("tool_call_id")
                if call_id in pending_calls:
                    function_info = json.loads(pending_calls[call_id])
                    
                    # 构造符合 CLI 协议的工具返回结构
                    tool_output_parts.append({
                        "functionResponse": {
                            "name": function_info['name'],
                            "response": output.get("output")
                        }
                    })
                    # 清理已完成的 Call ID
                    await client.hdel(f"session:{session_id}:pending", call_id)
            
            # 构造下一轮带工具输出的请求
            cli_payload = {
                "contents": [{
                    "parts": tool_output_parts,
                    "role": "tool" # 标识这是工具的返回结果
                }]
            }
        
        else:
            # 正常的文本请求
            cli_payload = {
                "contents": [{"parts": [{"text": data.get("prompt", "")}]}],
                "generationConfig": {"temperature": data.get("temperature", 0.7)}
            }
            
        return json.dumps(cli_payload).encode()
    except Exception as e:
        logger.error(f"Protocol Transformation Error (Std->CLI): {e}")
        raise HTTPException(status_code=400, detail=f"Invalid request format: {str(e)}")


async def transform_cli_to_standard(raw_line: bytes, session_id: str) -> dict:
    """将逆向出的数据帧映射回标准格式，并提取 Tool Calls 和 Grounding Metadata"""
    try:
        data = json.loads(raw_line)
        
        # 假设逆向结果：模型返回信息在 candidates[0]
        candidate = data.get("candidates", [{}])[0]

        # 1. 提取 Tool Calls 
        cli_tool_calls = candidate.get("tool_calls")
        if cli_tool_calls:
            # 记录所有待处理的 call ID 到 Redis
            await record_tool_calls(session_id, cli_tool_calls)
            
            # 映射回标准 API 格式
            return {
                "choices": [{"delta": {"tool_calls": cli_tool_calls}}],
                "action": "tool_required" 
            }

        # 2. 提取 Grounding Metadata (搜索来源)
        metadata = data.get("grounding_metadata") 
        
        # 3. 提取文本
        content = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")
        
        # 4. 构造标准响应
        transformed = {
            "choices": [{"delta": {"content": content}}]
        }
        
        # 5. 注入元数据 (Grounding Metadata)
        if metadata:
            transformed['grounding_metadata'] = metadata

        # 6. 检查是否是最终帧 (Final Frame Recognition)
        if candidate.get('finish_reason'):
             transformed['choices'][0]['finish_reason'] = candidate['finish_reason']

        return transformed
        
    except json.JSONDecodeError:
        logger.warning(f"JSON Decode Error on line: {raw_line.decode()}")
        return {} # 忽略非 JSON 行
    except Exception as e:
        logger.error(f"Protocol Parsing Error (CLI->Std): {e}")
        # 返回错误信息以便注入流中
        return {"error": {"message": f"Protocol parsing error: {str(e)}", "type": "protocol_error"}}


# --- 流处理核心 (异步) ---

async def frame_processor(resp, session_id) -> AsyncGenerator[str, None]:
    """处理 TCP 粘包/拆包的核心生成器，利用 aiter_content 异步读取流"""
    buffer = b""
    
    try:
        # 使用 aiter_content 进行异步流式读取
        async for chunk in resp.aiter_content(chunk_size=1024):
            buffer += chunk
            
            while FRAME_DELIMITER in buffer:
                line, buffer = buffer.split(FRAME_DELIMITER, 1)
                if not line.strip(): continue
                
                # 协议转换 (异步调用，因为它涉及 Redis)
                transformed = await transform_cli_to_standard(line, session_id) 
                
                if transformed:
                    # 检查是否包含错误信息
                    if "error" in transformed:
                         yield f"data: {json.dumps(transformed)}\n\n"
                    else:
                         yield f"data: {json.dumps(transformed)}\n\n"
        
        # 处理剩余的 buffer (如果有)
        if buffer.strip():
             transformed = await transform_cli_to_standard(buffer, session_id)
             if transformed:
                 yield f"data: {json.dumps(transformed)}\n\n"

        yield "data: [DONE]\n\n"

    except Exception as e:
        logger.error(f"Stream Processing Error: {e}")
        yield f"data: {json.dumps({'error': str(e)})}\n\n"


# --- FastAPI 路由与生命周期 ---

@app.on_event("startup")
async def startup_event():
    """优雅启动钩子：初始化 Redis 和 AsyncSession"""
    global http_session
    await get_redis_client()
    # 初始化异步 Session，使用指定指纹
    http_session = AsyncSession(impersonate="chrome110")
    logger.info("[Startup] Gemini Gateway and Redis connections initialized.")

@app.on_event("shutdown")
async def shutdown_event():
    """优雅停机钩子：确保连接被清理"""
    logger.info("[Shutdown] Graceful shutdown initiated...")
    
    # 关闭 AsyncSession
    if http_session:
        await http_session.close()
        logger.info("[Shutdown] AsyncSession closed.")
    
    # 关闭 Redis 连接
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
        logger.info("[Shutdown] Redis client closed.")


@app.post("/v1/chat/completions")
async def reverse_proxy_endpoint(request: Request):
    
    body = await request.json()
    session_id = await get_session_id_from_request(request)
    
    # 获取上游 API Key (优先环境变量，其次请求头)
    upstream_key = os.getenv("GEMINI_API_KEY")
    if not upstream_key:
        upstream_key = request.headers.get("x-goog-api-key")
    
    if not upstream_key:
         raise HTTPException(status_code=401, detail="Missing API Key. Please set GEMINI_API_KEY environment variable or pass x-goog-api-key header.")

    # 1. 构造伪装请求头 (指纹伪装)
    headers = OrderedDict([
        ("Host", "gemini-cli-backend.googleapis.com"),
        ("X-Goog-Api-Key", upstream_key), 
        ("User-Agent", "Gemini-CLI/1.0 (Go-http-client/1.1)"),
        ("Content-Type", "application/json"),
    ])

    # 2. 构造 CLI 专用 Payload (包含工具输出上下文)
    cli_body = await transform_standard_to_cli(body, session_id)

    try:
        # 3. 使用 AsyncSession 发起异步请求
        resp = await http_session.post(
            url=UPSTREAM_URL,
            headers=headers,
            data=cli_body,
            stream=True,
            timeout=60
        )
        
        if resp.status_code != 200:
            # 读取错误响应内容
            error_content = await resp.content.read()
            error_detail = error_content.decode() if error_content else f"Upstream error: Status {resp.status_code}"
            logger.error(f"Upstream Error: {error_detail}")
            raise HTTPException(status_code=resp.status_code, detail=error_detail)

        # 4. 返回流式响应
        return StreamingResponse(
            frame_processor(resp, session_id),
            media_type="text/event-stream"
        )

    except HTTPException:
        # 传递上游 HTTP 错误
        raise
    except Exception as e:
        logger.error(f"Critical Proxy Error: {e}")
        raise HTTPException(status_code=500, detail=f"Proxy execution failed: {str(e)}")
