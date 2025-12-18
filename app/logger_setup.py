import json
import logging
import time
from datetime import datetime, timezone
from contextvars import ContextVar
from typing import Optional

# [核心黑科技] ContextVars
# 用于在异步请求中传递 Request ID，实现无感注入
request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)

class JSONFormatter(logging.Formatter):
    """
    [OTel 风格] 结构化日志格式化器
    """
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        # 1. 基础字段
        log_object = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "line": record.lineno,
        }

        # 2. [自动注入] 追踪 ID
        req_id = request_id_ctx.get()
        if req_id:
            log_object["trace_id"] = req_id

        # 3. 处理 extra_data (上下文参数)
        if hasattr(record, "extra_data"):
             log_object.update(record.extra_data)
        
        # 4. 异常堆栈
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_object, ensure_ascii=False)

def setup_logging(service_name: str = "SWARM-Gateway", level: str = "INFO"):
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter(service_name))
    
    root_logger.setLevel(level.upper())
    root_logger.addHandler(handler)
    
    # 压制部分库的日志
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("httpx").setLevel(logging.WARNING)
