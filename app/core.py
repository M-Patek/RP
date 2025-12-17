import json
import os
import time
import hashlib
import random
import logging
import asyncio
from typing import Dict, List, Optional, Any
from collections import OrderedDict
from fastapi import HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis as AsyncRedis

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeminiTactical-Core")

# --- 常量定义 ---
CONFIG_PATH = "config.json"
DEFAULT_CONCURRENCY = 5
MAX_BUFFER_SIZE = 1024 * 1024 
FRAME_DELIMITER = b"\n"
# [Unified] 统一上游地址
UPSTREAM_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

# --- Lua 脚本：原子性并发控制 ---
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

# --- 数据模型 (Pydantic) ---
class GenerationPart(BaseModel):
    text: Optional[str] = None

class GenerationContent(BaseModel):
    parts: List[GenerationPart]
    role: Optional[str] = "user"

class ProxyRequest(BaseModel):
    contents: List[GenerationContent]
    # 允许透传其他字段，如 generationConfig, safetySettings
    class Config:
        extra = "allow"

# --- 核心调度类 ---
class SlotManager:
    def __init__(self):
        self.slots = []
        self.states = {} 

    def load_config(self):
        """
        加载配置文件，支持环境变量展开和 JSON 容错
        """
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    raw_content = f.read()
                    # 解析环境变量占位符 (如 ${API_KEY})
                    expanded_content = os.path.expandvars(raw_content)
                    new_slots = json.loads(expanded_content)
                
                if not isinstance(new_slots, list):
                    raise ValueError("Config must be a list of slots")

                self.slots = new_slots
                
                # 初始化状态
                for idx, slot in enumerate(self.slots):
                    if 'key' not in slot or not slot['key'] or slot['key'].startswith("$"): 
                        logger.warning(f"[Config] Slot {idx} missing 'key' or env var not set, skipping.")
                        continue
                        
                    key_hash = hashlib.md5(slot['key'].encode()).hexdigest()
                    if idx not in self.states:
                        self.states[idx] = {
                            "failures": 0,
                            "weight": 100.0,
                            "concurrency_key": key_hash,
                            "cool_down_until": 0
                        }
                logger.info(f"[Config] Successfully loaded {len(self.slots)} slots.")
            else:
                logger.warning("[Config] No config found.")
                self.slots = []

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
            state["cool_down_until"] = now + (30 * (2 ** min(state["failures"], 5)))
        elif status_code == 403: 
             logger.error(f"Slot {idx} Hit 403 (Possbile Ban).")
             state["weight"] = 0
             state["cool_down_until"] = now + 3600
        else:
            state["weight"] -= 10.0
            state["failures"] += 1

# 单例模式
slot_manager = SlotManager()
