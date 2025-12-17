import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator, Optional, Dict, Any
from collections import OrderedDict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from curl_cffi.requests import Session
from redis.asyncio import Redis as AsyncRedis

# --- 全局配置与初始化 ---
app = FastAPI(title="Gemini Enhanced Gateway")

# 1. 初始化线程池 (用于执行阻塞的 curl_cffi I/O)
# 设置高并发工作线程，以应对长时间的流式请求
THREAD_POOL_WORKERS = 100
executor = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS)

# 2. 初始化 curl_cffi Session (用于指纹伪装和连接复用)
http_session = Session(impersonate="chrome110") 

# 3. Redis 客户端 (模拟状态机)
REDIS_HOST = "redis" # 对应 docker-compose service name
REDIS_PORT = 6379
REDIS_PASSWORD = "YOUR_REDIS_PASSWORD" # 生产环境应使用环境变量
REDIS_CLIENT: Optional[AsyncRedis] = None

# 4. 协议分隔符 (根据逆向结果，假设是 \n)
FRAME_DELIMITER = b"\n"
SESSION_TTL = 300 # Tool Calling 状态的生存时间 (秒)
UPSTREAM_URL = "https://gemini-cli-backend.googleapis.com/v1/generate" # 逆向出的实际端点

# --- 辅助函数：Redis 状态管理 (Mocked) ---

async def get_redis_client():
    global REDIS_CLIENT
    if REDIS_CLIENT is None:
        try:
            # 使用 URL 方式连接 Redis
            REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, 
                                      password=REDIS_PASSWORD, decode_responses=True)
            await REDIS_CLIENT.ping()
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            raise HTTPException(status_code=503, detail="Redis connection failed")
    return REDIS_CLIENT

async def get_session_id_from_request(request: Request) -> str:
    # 实际生产环境：应从认证 Header 或 Token 中提取
    # 简化示例：使用一个 UUID 或默认 ID
    return "session_debug_id" 

async def record_tool_calls(session_id: str, tool_calls: list):
    """记录模型发出的待处理工具调用 (Tool Calling Manager)"""
    client = await get_redis_client()
    pending_calls = {call['id']: json.dumps(call['function']) for call in tool_calls}
    await client.hset(f"session:{session_id}:pending", mapping=pending_calls)
    await client.expire(f"session:{session_id}:pending", SESSION_TTL)

async def check_and_get_pending_calls(session_id: str) -> Dict[str, str]:
    """获取所有待处理的工具调用"""
    client = await get_redis_client()
    return await client.hgetall(f"session:{session_id}:pending")

# --- 协议转译函数 ---

async def transform_standard_to_cli(data: dict, session_id: str) -> bytes:
    """将标准 API 请求转换为 CLI 协议格式，并处理上下文压缩/Tool Output注入"""
    
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
                # 清理已完成的 Call ID (原子性操作)
                await client.hdel(f"session:{session_id}:pending", call_id)
        
        # 构造下一轮带工具输出的请求
        cli_payload = {
            "contents": [{
                "parts": tool_output_parts,
                "role": "tool" # 标识这是工具的返回结果
            }]
        }
        
    else:
        # --- [滑动窗口上下文压缩逻辑 (Mocked)] ---
        # 实际代码中，需要在这里实现历史消息的加载、压缩和重组
        
        # 正常的文本请求
        cli_payload = {
            "contents": [{"parts": [{"text": data.get("prompt", "")}]}],
            "generationConfig": {"temperature": data.get("temperature", 0.7)}
        }
        
    return json.dumps(cli_payload).encode()


async def transform_cli_to_standard(raw_line: bytes, session_id: str) -> dict:
    """将逆向出的数据帧映射回标准格式，并提取 Tool Calls 和 Grounding Metadata"""
    try:
        data = json.loads(raw_line)
        
        # 假设逆向结果：模型返回信息在 candidates[0]
        candidate = data.get("candidates", [{}])[0]

        # 1. 提取 Tool Calls (包括 Google Search_retrieval)
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
        # 这里应检查是否有 finish_reason 字段来触发 RAG 合并
        # 简单实现：将 finish_reason 字段透传
        if candidate.get('finish_reason'):
             transformed['choices'][0]['finish_reason'] = candidate['finish_reason']

        return transformed
        
    except Exception as e:
        # 协议降级监控点：如果解析失败，意味着协议可能已更改
        print(f"Protocol parsing error: {e} on data: {raw_line.decode()}")
        return {}


# --- 流处理核心 ---

async def frame_processor(resp, session_id) -> AsyncGenerator[str, None]:
    """处理 TCP 粘包/拆包的核心生成器，并在线程池中读取同步流"""
    buffer = b""
    loop = asyncio.get_event_loop()
    
    # 获取 RAG 结果缓冲区 (Mocked)
    # rag_buffer_key = f"session:{session_id}:rag_buffer"

    while True:
        # 在线程池中同步读取下一个数据块 (解决 iter_content 阻塞问题)
        chunk = await loop.run_in_executor(
            executor, lambda: next(resp.iter_content(chunk_size=1024), None)
        )
        
        if chunk is None:
            break
            
        buffer += chunk
        
        while FRAME_DELIMITER in buffer:
            line, buffer = buffer.split(FRAME_DELIMITER, 1)
            if not line.strip(): continue
            
            # 协议转换 (异步调用，因为它涉及 Redis)
            transformed = await transform_cli_to_standard(line, session_id) 
            
            if transformed:
                yield f"data: {json.dumps(transformed)}\n\n"
                
                # --- [最终帧识别与 RAG 合并逻辑] ---
                # if transformed.get('choices', [{}])[0].get('finish_reason') == 'STOP':
                #     rag_results = await client.get(rag_buffer_key)
                #     if rag_results:
                #         # 合并 RAG 结果到最后一帧，并推送
                #         yield f"data: {json.dumps({'rag_metadata': rag_results})}\n\n"
    
    yield "data: [DONE]\n\n"


# --- FastAPI 路由与生命周期 ---

@app.on_event("startup")
async def startup_event():
    # 确保在启动时尝试连接 Redis
    await get_redis_client()
    print("[Startup] Gemini Gateway and Redis connections initialized.")

@app.on_event("shutdown")
async def shutdown_event():
    """优雅停机钩子：确保线程池和连接被清理"""
    print("[Shutdown] Graceful shutdown initiated...")
    
    # 清理线程池，等待正在执行的请求完成
    executor.shutdown(wait=True)
    print("[Shutdown] ThreadPoolExecutor has been shut down.")
    
    # 关闭 Redis 连接
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
        print("[Shutdown] Redis client closed.")


@app.post("/v1/chat/completions")
async def reverse_proxy_endpoint(request: Request):
    
    loop = asyncio.get_event_loop()
    body = await request.json()
    
    session_id = await get_session_id_from_request(request)
    
    # 1. 构造伪装请求头 (指纹伪装)
    headers = OrderedDict([
        ("Host", "gemini-cli-backend.googleapis.com"),
        ("X-Goog-Api-Key", "YOUR_CAPTURED_API_KEY"), # 替换为逆向出的 Key
        ("User-Agent", "Gemini-CLI/1.0 (Go-http-client/1.1)"),
        ("Content-Type", "application/json"),
    ])

    # 2. 构造 CLI 专用 Payload (包含工具输出上下文)
    cli_body = await transform_standard_to_cli(body, session_id)

    # 3. 在线程池中执行同步的 curl_cffi 调用
    def sync_proxy_call():
        # curl_cffi.Session 的 post 方法
        return http_session.post(
            url=UPSTREAM_URL,
            headers=headers,
            data=cli_body,
            stream=True,
            timeout=60
        )

    try:
        resp = await loop.run_in_executor(executor, sync_proxy_call)
        
        if resp.status_code != 200:
            error_detail = resp.text if resp.text else f"Upstream error: Status {resp.status_code}"
            raise HTTPException(status_code=resp.status_code, detail=error_detail)

        # 4. 返回流式响应，进入 frame_processor 处理
        return StreamingResponse(
            frame_processor(resp, session_id),
            media_type="text/event-stream"
        )

    except HTTPException:
        # 传递上游 HTTP 错误
        raise
    except Exception as e:
        print(f"Critical Proxy Error: {e}")
        raise HTTPException(status_code=500, detail=f"Proxy execution failed: {str(e)}")
