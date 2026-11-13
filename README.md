# talon

基于 Zig 0.17 和 zio 的网络开发引擎。`talon.core` 提供协议无关的流式连接引擎、TCP/内存传输、连接中间件和拆帧工具；`talon.http` 提供 HTTP/1.1 服务端、客户端、WebSocket 与 SSE。

```sh
zig build test
zig build run-http
# 在另一终端访问 http://127.0.0.1:8080/
```

当前使用 Zig 0.16.0 和 zio v0.18.0；依赖版本与包哈希见 [build.zig.zon](build.zig.zon)，首次构建需要下载依赖。开发本地 zio 时可使用 `zig build --fork=../zio`，无需修改依赖声明。

- [使用指南](docs/user-guide.md)：引入模块、API、所有权和资源限制。
- [开发指南](docs/developer-guide.md)：实现结构和当前能力状态。
- [客户端微基准](examples/http_client_bench.zig)：内存传输上的连接池复用测量，不代表 TCP/TLS 服务端性能。
- [TCP/TLS 网络 bench](docs/benchmarks/README.md)：复用、冷连接、有界到达与空闲回收的复现方法。
- [当前架构](docs/talon-architecture.md)：模块责任、设计取舍及所有权。
- [项目现状与生产级差距](docs/project-status.md)：目标、能力清单、已修问题及生产验证优先级。
- [审查结论](docs/architecture-review.md)：本地变更的保留与清理依据。

客户端 HTTPS 已提供；服务端可选 [OpenSSL TLS adapter](docs/design/tls.md)，默认构建无需 OpenSSL。当前尚不提供 UnixListener、UDP 引擎或完整连接所有权转移；HTTP/2、HTTP/3 也不在当前实现范围。路由、参数提取等 Web 框架能力不在本仓库范围。

当前包版本为 0.1.0，已有协议与资源边界回归；生产使用前仍需目标平台 CI、协议互操作和实际负载下的容量、长稳及运维验证。已实现能力不等于通用生产就绪承诺。
