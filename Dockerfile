# -------------------------------------------------------------------
# 🌩️ S.W.A.R.M. Gateway - Cloud Production Dockerfile
# 基于 Python 3.11 Slim 版本，兼顾体积与兼容性
# -------------------------------------------------------------------

# 第一阶段：构建依赖 (Builder Stage)
# 作用：在一个临时环境中安装所有依赖，避免把编译工具带入最终镜像
FROM python:3.11-slim as builder

WORKDIR /app

# 设置环境变量：
# PYTHONDONTWRITEBYTECODE=1: 不生成 .pyc 文件，节省空间
# PYTHONUNBUFFERED=1: 日志直接输出，方便 Docker 收集
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 安装系统级依赖 (如果 requirements.txt 里有需要编译的库，如 numpy/pandas)
# 对于纯网关应用，通常只需要基础库，但为了保险安装 build-essential
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖清单并安装到 /install 目录
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt


# -------------------------------------------------------------------
# 第二阶段：运行环境 (Runtime Stage)
# 作用：最终的纯净镜像，只包含 Python 环境和已安装的包
# -------------------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# 再次设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # 将第一阶段安装的包路径加入 Python 搜索路径
    PYTHONPATH=/usr/local/lib/python3.11/site-packages

# 安装 curl (用于 Docker Healthcheck 健康检查)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 从构建阶段复制已安装的依赖
COPY --from=builder /install /usr/local

# 复制源代码
# 注意：这里直接 COPY，而不是挂载 Volume，这是生产环境的最佳实践
COPY ./app ./app
# 如果有 prometheus 配置或 config.json 也可以根据需要复制
# COPY ./config.json . 

# 创建非 root 用户运行 (安全加固)
# 防止黑客攻破容器后直接获得 root 权限
RUN useradd -m swarmuser && chown -R swarmuser /app
USER swarmuser

# 暴露端口
EXPOSE 8000

# 启动命令
# 1. host 0.0.0.0: 允许外部访问
# 2. workers 4: 生产环境开启多进程，提高并发处理能力 (建议设置为 CPU 核心数 x 2 + 1)
# 3. --proxy-headers: 告诉 Uvicorn 它运行在 Cloudflare/Nginx 后面，要信任转发头
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--proxy-headers"]
