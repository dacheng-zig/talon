# talon

基于 Zig 0.16 和 zio 的网络开发引擎。`talon.core` 提供协议无关的流式连接引擎、TCP/内存传输、连接中间件和拆帧工具；`talon.http` 提供 HTTP/1.1 服务端、客户端、WebSocket 与 SSE。

```sh
zig build test
zig build run-http
# 在另一终端访问 http://127.0.0.1:8080/
```

当前使用 Zig 0.16.0 和 zio v0.18.0；依赖版本与包哈希见 [build.zig.zon](build.zig.zon)，首次构建需要下载依赖。开发本地 zio 时可使用 `zig build --fork=../zio`，无需修改依赖声明。

- [使用指南](docs/user-guide.md)：引入模块、API、所有权和资源限制。
- [开发指南](docs/developer-guide.md)：实现结构和当前能力状态。
- [客户端微基准](examples/http_client_bench.zig)：内存传输上的连接池复用测量，不代表 TCP/TLS 服务端性能。
- [架构目标](docs/talon-architecture.md)：长期规划，不能当作已实现能力清单。

当前尚不提供服务端 TLS、UnixListener、UDP 引擎或完整连接所有权转移；HTTP/2、HTTP/3 也不在当前实现范围。客户端 HTTPS 已提供。路由、参数提取等 Web 框架能力属于独立的 wing 规划。
