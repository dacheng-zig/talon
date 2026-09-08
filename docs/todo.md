# 待办与演进方向

当前实现与未实现能力以 [架构设计](talon-architecture.md#11-实施路线图talon-侧) 和 [开发者指南](developer-guide.md) 为准。本页只索引已有规划，不表示排期或新增实现承诺。

- core：完整劫持资源转移、feature 查询、Unix listener、指标与生命周期钩子。
- 传输与性能：SO_REUSEPORT、多规格 buffer pool、sendFile。
- UDP：DatagramServer 与 SessionTable，由真实协议需求驱动。
- HTTP：服务端 TLS 中间件、AutoProtocol / HTTP/2，后续 QUIC / HTTP/3 另行设计。

HTTP/HTTPS 客户端、SSE/WebSocket、body 速率防御、写入超时与 arena 保留限制已经实现，不再属于上述待办。性能和长稳验收须以执行记录为依据。
