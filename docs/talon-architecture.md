# Talon 架构

Talon 是基于 Zig 0.16.0 / zio v0.18.0 的流式网络引擎，包含 HTTP/1.1、WebSocket、SSE 和 HTTP/HTTPS 客户端。本文描述当前实现；依赖与包哈希以 [build.zig.zon](../build.zig.zon) 为准。

## 模块与依赖

```text
talon
├── core：StreamServer、Connection、TCP/Memory listener/dialer
│         chain、framing、Limits、BufferPool、AllocationBudget、Pipe
└── http
    ├── codec：HTTP 解析/编码、body、SSE、WebSocket Session
    ├── server：Http1Protocol、Request/Response、SSE/WS 升级
    └── client：Client、Connector、Connection、Pool、TLS、Cookie

talon_tls（可选）→ talon.core + zio + OpenSSL
```

core 不引用 http。talon.core 与 talon.http 通过相对 import 组成一个模块，core 常用入口另有顶层别名；这不靠目录强制隔离，须在代码审查中维持方向。RESP 示例证明自定义协议不需要 HTTP policy。talon_tls 只有显式启用后才链接 OpenSSL。

## 责任边界

| 边界 | 唯一责任 | 调用方需要知道 |
|---|---|---|
| StreamServer | 许可、连接任务、包装顺序、建立/drain 期限、关闭和窗口归还 | Proto/adapter 契约，固定配置和对象寿命 |
| Connection/transport | 稳定 I/O、完整 flush、期限能力、受检 raw 关闭责任移交 | 借用/移交边界，无未消费预读时才能 detach |
| AllocationBudget/BufferPool | 路由分配的额度、缓存窗口所有权 | allocator 寿命/线程安全，预算不等于 RSS |
| Http1Protocol | HTTP limits、请求 arena、解析/响应/升级提交 | Request 借用和 Response 提交规则 |
| Client/Pool/ClientConnection | 请求政策、统一准入/空闲缓存、响应存储/解码 | Response/SSE/WS 释放责任，各预算作用域 |
| WebSocket Session | 单 reader、共享帧写锁、数据准入、出站分片和终止错误 | 应用任务/payload 生命周期，无控制优先保证 |
| TLS provider | 身份、TLS 状态和 BIO 生命周期 | 显式验证政策、Context 寿命、可选系统依赖 |

选择这些较深的边界，让调用方表达“服务/发送/申请连接”，无需重建计数、取消回滚和部分输出政策。共享代码仅在规则由同一所有者决定时收敛；HTTP 两端各自的 arena 释放时机不强行抽象为通用对象。

## 连接与内存

accept 前等待许可，accept 后任务承担 raw 的全部责任。adapter、中间件、可选握手及首个 HTTP 头共用 accept 时刻的绝对建立期限。成功会话正常结束时可执行有限 finish；失败/受迫取消直接释放。shutdown 停止 accept，drain 上限保证剩余任务进入取消清理。

HTTP arena 属于协议，Connection 只提供 gpa。池窗口、请求 arena、响应头、解码器、拥有型帧分别由其真实所有者释放。闲置容量继续占 allocator 预算，只有 free/缩小归还；空闲回收由 owner 显式执行。活跃连接数、wire 大小、decoded 输出、分配字节和保留容量限制不同风险，不能把其中任一限额称为全进程内存上限。

固定地址 I/O 和 Session 在操作期间不可移动，应用必须先 join 子任务再释放借用数据。部分响应、101 或 WS 帧一旦输出失败，不可继续 fallback 或重放；取消既是控制流，也是资源释放边界。

## 设计索引

- [传输与连接生命周期](design/transport-lifecycle.md)：协议映射、adapter、期限、关闭责任、dial 和运行指标。
- [资源所有权](design/resource-ownership.md)：allocator 预算、锁范围、arena、显式 idle 回收。
- [分帧](design/framing.md)：借用与拥有、碎片不变性和错误终止。
- [HTTP 服务端](design/http-server.md)：输入限额、输出提交与升级。
- [HTTP 客户端](design/http-client.md)：准入、累计头预算、解码/收集和复用。
- [WebSocket](design/websocket.md)：并发、准入、分片和应用停机作用域。
- [TLS](design/tls.md)：DNS/IP 验证、可选 OpenSSL、SNI/ALPN 和 BIO。

## 验证与演进依据

[开发指南](developer-guide.md) 给出编译/回归入口，[bench 方法](benchmarks/README.md) 给出独立网络负载。MemoryListener/小 Pipe 提供可控制的背压与错误边界，真实 TCP/TLS 检查平台 I/O 和协议互操作；微基准或短时成功不能证明服务容量、公平性或长期驻留。

当前没有 datagram 引擎、Unix listener、HTTP/2/3、路由框架或动态证书更新。只有真实消费者和相应边界测试出现后才扩展这些能力。性能优化须先定位现有责任边界中的瓶颈，再比较失败行为和维护成本；系统分配器、内核 slab/BPF 观察不进入 Talon 核心构建。
