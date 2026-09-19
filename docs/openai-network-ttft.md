# OpenAI 网络首帧统计

“OpenAI Responses 首 token 统计口径”选择“网络首帧（对齐 CPA）”后，OpenAI OAuth/SetupToken 账号的 HTTP/SSE 与 WS 上游都生效，不依赖“强制 OpenAI 上游 WebSocket”、强制 WS 的分组范围或 502/503 重试开关。

- 收到 `response.created`、`response.in_progress` 等非终止事件，即记录首帧时间并转发给客户端，不等待回答正文。HTTP 在完整 SSE 事件边界刷新，保持事件格式完整。
- 仅有 `response.completed`、`response.failed` 等终止事件时，不把终止时间当作首 token 时间。
- HTTP 首帧通知发送后，响应已经开始，后续失败沿用已输出响应的错误处理，不再重放该 HTTP 响应。
- 计时起点沿用现有请求入口，仍包含请求预处理与连接等待。网络首帧不表示已经产生回答。
- 历史兼容、真实可见输出两种口径，以及 API Key 和其他平台账号，保持原处理方式。WS 调度和原有首帧转发逻辑不变。
