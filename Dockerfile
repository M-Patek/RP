FROM python:3.11-slim

设置工作目录

WORKDIR /app

复制依赖文件

COPY requirements.txt .

合并安装与清理步骤以减小镜像体积

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    curl && \
    pip install --no-cache-dir -r requirements.txt && \
    # 清理构建依赖和缓存
    apt-get purge -y --auto-remove build-essential libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

复制应用代码

COPY app/ app/

创建非 root 用户并切换，实施最小权限原则

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

暴露 FastAPI 端口

EXPOSE 8000

启动 Uvicorn

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
