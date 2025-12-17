# 🛡️ Gemini Tactical Gateway (Reversed-Proxy)

**High-Performance, Fingerprint-Obfuscated Reverse Proxy for Google Gemini API**

Now supporting official Google Gemini API (v1beta) with dual-engine architecture.

Gemini Tactical Gateway 是一个专为 Google Gemini API 设计的高级反向代理网关。它不仅支持多账号（Slot）负载均衡和并发控制，还独创了双引擎架构，同时满足云端生产环境的高隐蔽性需求和本地开发环境的兼容性需求。

---

## ✨ 核心特性 (Key Features)

### 🚀 双引擎架构 (Dual-Engine)
* **Cloud Engine (Docker/Linux):** 基于 `curl_cffi`，支持 TLS/JA3 指纹模拟（Chrome/Safari/Edge），有效对抗云端风控。
* **Local Engine (Windows/Mac):** 基于 `aiohttp`，彻底解决 Windows 下 C 扩展编译难题，提供流畅的本地调试体验。

### 🧠 智能战术调度 (Tactical Scheduling)
* **多 Slot 轮询:** 支持配置多个 API Key/Proxy 组合，基于权重的概率调度算法。
* **自动熔断与恢复:** 自动检测 `429 (Rate Limit)` 和 `403 (Ban)`，智能降低故障节点权重或触发 Webhook 报警。
* **原子级并发控制:** 使用 Redis + Lua 脚本实现严格的并发限制，防止超额调用。

### 🔒 安全与合规
* **官方 API 对接:** 全面对接 Google 官方 `generativelanguage.googleapis.com` 接口。
* **隐私保护:** 敏感信息（API Keys, Secrets）通过环境变量注入，杜绝硬编码。
* **DoS 防御:** 内置流式响应缓冲区限制 (1MB)，防止恶意大包攻击。

---

## 🛠️ 快速开始 (Quick Start)

### 方式一：Docker 部署 (生产环境推荐)
> 适用于服务器部署，自动启用抗指纹模式。

1.  **克隆仓库:**
    ```bash
    git clone [https://github.com/your-repo/gemini-tactical-gateway.git](https://github.com/your-repo/gemini-tactical-gateway.git)
    cd gemini-tactical-gateway
    ```

2.  **配置环境变量:**
    ```bash
    cp .env.example .env
    # 编辑 .env 文件，设置 REDIS_PASSWORD 和 GATEWAY_SECRET
    vim .env
    ```

3.  **配置代理池 (config.json):**
    修改 `config.json`，支持使用 `${ENV_VAR}` 引用环境变量：
    ```json
    [
      {
        "comment": "Slot 1: US-LAX",
        "key": "${GEMINI_API_KEY_1}",
        "proxy": "[http://user:pass@proxy-us.com:7890](http://user:pass@proxy-us.com:7890)",
        "impersonate": "chrome110",
        "max_concurrency": 5
      }
    ]
    ```

4.  **启动服务:**
    ```bash
    docker-compose up -d --build
    ```

---

### 方式二：本地开发 (Windows/Mac)
> 适用于本地调试，使用 `aiohttp` 引擎，无需编译复杂依赖。

1.  **安装依赖:**
    ```bash
    # Windows 用户无需安装 curl_cffi
    pip install aiohttp redis fastapi uvicorn python-dotenv prometheus-fastapi-instrumentator
    ```

2.  **启动本地 Redis:**
    确保本地运行了 Redis (默认端口 6379)。

3.  **运行本地版网关:**
    ```bash
    # 注意：运行的是 main_local.py
    uvicorn app.main_local:app --reload --port 8000
    ```

---

## 📡 API 调用示例

网关启动后，您可以像调用 OpenAI/Gemini 官方接口一样使用它。

**Endpoint:** `POST /v1/chat/completions`

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer <YOUR_GATEWAY_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{
    "contents": [{
      "parts": [{"text": "Hello, who are you?"}]
    }]
  }'
