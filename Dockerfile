# --- Stage 1: Builder (构建层) ---
FROM python:3.11-slim as builder

WORKDIR /app

# 安装构建依赖 (编译 curl_cffi 等 C 扩展需要)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    curl

COPY requirements.txt .

# 将依赖安装到指定目录 /install
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --- Stage 2: Final (运行层) ---
FROM python:3.11-slim

WORKDIR /app

# 仅安装运行时必需的工具 (如 curl 用于健康检查)，不包含 gcc 等构建工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && \
    rm -rf /var/lib/apt/lists/*

# 从 Builder 层复制编译好的 Python 包
COPY --from=builder /install /usr/local

# 复制应用代码
COPY app/ app/

# 创建非 root 用户并切换，实施最小权限原则
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# 暴露 FastAPI 端口
EXPOSE 8000

# 启动 Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
