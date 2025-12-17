import json
import os
import random
import logging
import asyncio
import time
import hashlib
import aiohttp # [Local] 使用 aiohttp 兼容 Windows
import secrets
from typing import AsyncGenerator, Optional
from collections import OrderedDict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis as AsyncRedis
from dotenv import load_dotenv

load_dotenv()

# --- Windows 异步循环修复 ---
if os.name == 'nt':
    import sys
    if sys.platform == 'win32' and sys.version_info >= (3, 8, 0):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except AttributeError:
            pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeminiTactical-Local")

CONFIG_PATH = "config.json"
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET")
AUTO_REPLACEMENT_WEBHOOK = os.getenv("AUTO_REPLACEMENT_WEBHOOK")

# --- Redis 配置 (Local) ---
REDIS_HOST = "127.0.0.1" # [Local] 本地地址
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_CLIENT: Optional[AsyncRedis] = None

# [Unified] 统一上游地址
UPSTREAM_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
DEFAULT_CONCURRENCY = 5
MAX_BUFFER_SIZE = 1024 * 1024 

LUA_ACQUIRE_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local current = tonumber(redis.call('get', key) or "0")
if current >= limit then return -1
else
    local new_val = redis.call('incr', key)
    redis.call('expire', key, ttl)
    return new_val
end
"""

# [Local] 仅保留基础指纹，不使用随机化列表
IMPERSONATE_VARIANTS = ["chrome110"]

class SlotManager:
    def __init__(self):
        self.slots = []
        self.states = {} 

    def load_config(self):
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    new_slots = json.loads(os.path.expandvars(f.read()))
                
                self.slots = new_slots
                for idx, slot in enumerate(self.slots):
                    if 'key' not in slot: continue
                    key_hash = hashlib.md5(slot['key'].encode()).hexdigest()
                    if idx not in self.states:
                        self.states[idx] = {
                            "failures": 0, "weight": 100.0, "concurrency_key": key_hash,
                            "cool_down_until": 0
                        }
                logger.info(f"[Config] Loaded {len(self.slots)} slots.")
        except Exception as e:
            logger.error(f"[Config] Error: {e}")

    async def get_best_slot(self, redis_client: AsyncRedis) -> int:
        if not self.slots: raise HTTPException(status_code=503, detail="No config")
        candidates, weights, now = [], [], time.time()

        for idx, state in self.states.items():
            if state["cool_down_until"] > now: continue
            limit = self.slots[idx].get("max_concurrency", DEFAULT_CONCURRENCY)
            conc_key = f"concurrency:{state['concurrency_key']}"
            if (val := await redis_client.get(conc_key)) and int(val) >= limit: continue
            candidates.append(idx)
            weights.append(max(1.0, state["weight"]))

        if not candidates: raise HTTPException(status_code=503, detail="All slots busy")
        
        selected_idx = random.choices(candidates, weights=weights, k=1)[0]
        state = self.states[selected_idx]
        limit = self.slots[selected_idx].get("max_concurrency", DEFAULT_CONCURRENCY)
        
        try:
            if await redis_client.eval(LUA_ACQUIRE_SCRIPT, 1, f"concurrency:{state['concurrency_key']}", limit, 60) == -1:
                raise HTTPException(status_code=503, detail="Race condition limit")
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=500, detail="Redis Error")
            
        return selected_idx

    async def release_slot(self, idx: int, redis_client: AsyncRedis):
        if idx in self.states:
            await redis_client.decr(f"concurrency:{self.states[idx]['concurrency_key']}")

    async def report_status(self, idx: int, status_code: int):
        state = self.states[idx]
        if status_code == 200:
            state["weight"] = min(100.0, state["weight"] + 5.0)
            state["failures"] = 0
        elif status_code in [429, 403, 400]:
            state["weight"] = 0 if status_code == 403 else max(1.0, state["weight"] - 50.0)
            state["failures"] += 1
            backoff = 3600 if status_code == 403 else 30 * (2 ** min(state["failures"], 5))
            state["cool_down_until"] = time.time() + backoff
        else:
            state["weight"] -= 10.0

slot_manager = SlotManager()

# --- 2. 仿真逻辑 (Local: 禁用随机化) ---
def get_ja3_perturbed_impersonate(base_impersonate: str) -> str:
    # [Local] 强制返回原始值，便于 Windows 本地调试
    return base_impersonate

# --- 3. 核心流式处理 (Local: aiohttp 版) ---
async def smart_frame_processor(resp: aiohttp.ClientResponse, session_id: str) -> AsyncGenerator[str, None]:
    buffer = b""
    # [Local] 使用 aiohttp 的 iter_chunked
    iterator = resp.content.iter_chunked(1024).__aiter__()
    
    while True:
        try:
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=15.0)
            buffer += chunk
            if len(buffer) > MAX_BUFFER_SIZE: raise HTTPException(500, "Response too large")

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
            break
            
    if buffer.strip(): yield f"data: {buffer.decode('utf-8')}\n\n"
    yield "data: [DONE]\n\n"

# --- 4. FastAPI Setup ---
app = FastAPI(title="Gemini Tactical Gateway (Local)")

async def get_redis():
    global REDIS_CLIENT
    if not REDIS_CLIENT:
        REDIS_CLIENT = AsyncRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    return REDIS_CLIENT

@app.on_event("startup")
async def startup():
    slot_manager.load_config()
    await get_redis()

@app.post("/v1/chat/completions")
async def tactical_proxy(request: Request):
    if GATEWAY_SECRET:
        if not secrets.compare_digest(request.headers.get("Authorization") or "", f"Bearer {GATEWAY_SECRET}"):
            raise HTTPException(401, "Unauthorized")

    try: body = await request.json()
    except: raise HTTPException(400, "Bad JSON")

    redis = await get_redis()
    slot_idx = await slot_manager.get_best_slot(redis)
    slot = slot_manager.slots[slot_idx]
    session_obj = None

    try:
        # [Local] Logic
        key = slot["key"]
        proxy = slot.get("proxy")
        
        url_with_key = f"{UPSTREAM_URL}?key={key}"
        headers = {"Content-Type": "application/json"}
        if "headers" in slot: headers.update(slot["headers"])

        logger.info(f"Slot {slot_idx} Active (Local)")
        
        session_obj = aiohttp.ClientSession()
        # [Unified] 透传 body
        resp = await session_obj.post(
            url_with_key,
            headers=headers,
            json=body,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=120)
        )

        # [Unified] 错误处理
        if resp.status != 200:
            error_text = await resp.text()
            await slot_manager.report_status(slot_idx, resp.status)
            if resp.status in [403, 429, 400]:
                 raise HTTPException(status_code=resp.status, detail=f"API Error ({resp.status}): {error_text}")
            raise HTTPException(status_code=resp.status, detail=f"Upstream Error: {error_text}")

        await slot_manager.report_status(slot_idx, resp.status)
        await slot_manager.release_slot(slot_idx, redis)
        
        return StreamingResponse(
            smart_frame_processor(resp, "sess_id"),
            media_type="text/event-stream"
        )

    except Exception as e:
        await slot_manager.release_slot(slot_idx, redis)
        await slot_manager.report_status(slot_idx, 500)
        if session_obj: await session_obj.close()
        logger.error(f"Proxy failed: {e}")
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=502, detail="Gateway Error")
