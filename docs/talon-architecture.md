# talon：基于 zio 的网络服务引擎架构设计

本文说明当前实现的边界、设计取舍及尚未实现的演进方向。操作入口见 [使用指南](user-guide.md)，实现细节与测试位置见 [开发者指南](developer-guide.md)。适用依赖以 [build.zig.zon](../build.zig.zon) 为准：Zig 最低版本 0.16.0，zio 固定提交 `34510ecd0e41192eb4d379a047226269c4a1a56f`。

早期设计以 M0→M4 描述路线图；当前已包含客户端 TLS、SSE、WebSocket 和资源限制，不能再用“仅实现 M1”概括。本文的规划不构成现有 API、发布日期或验收通过记录。wing 是独立项目，本仓库没有 `wing-architecture.md`。

## 1. 目标与非目标

talon 提供协议无关的连接服务引擎，以及基于它的 HTTP/1.1 服务端；HTTP 包还提供共享编解码和出站客户端。自定义 TCP 协议可只使用 `talon.core`，复用传输、连接中间件、限额和停机机制。

设计目标包括：利用 comptime 组合减少协议和中间件分发开销，以显式所有权管理内存，通过内存传输测试完整协议循环，并在合适的稳定工作负载中复用请求内存。性能目标需要基准证据，不能由这些机制直接推导出吞吐或每请求零分配保证。

UDP 双引擎、Unix listener、服务端 TLS、HTTP/2 和 HTTP/3 仍属演进方向。talon 不承诺兼容 zio 之外的运行时，也不计划强行统一 stream 的字节流生命周期与 datagram 的报文/会话生命周期。

## 2. 总体架构与对外契约

```text
talon（唯一导出模块）
├── core：StreamServer、Connection、Tcp/MemoryListener
│         chain、middleware、framing、Limits、BufferPool、Pipe
└── http
    ├── codec：双向 HTTP/1.1 解析/编码、body、SSE、WebSocket
    ├── server：Http1Protocol、Request/Response、SSE、WebSocket
    └── client：Client、Connector/TLS、Connection、Pool、Cookie
                     ↓
              zio 运行时与网络 I/O
```

`talon.core` 与 `talon.http` 通过相对 import 组成同一个模块，core 常用 API 另有顶层别名。模块根目录不会阻止 core 引用 http；依赖方向需要代码审查维持，RESP 示例验证 core 的使用路径。

当前公开入口包括：

1. `talon.http.Server(App)` / `ServerWith(App, middlewares)`，App 提供 `handle(req, res)`。
2. `talon.http.Request` / `Response`，共享词汇 `Method` / `Version` / `Header` / `Status`；编解码机制位于 `talon.http.codec`。
3. `req.upgrade.accept()`、`talon.http.ws.upgrade()` 和 `res.startEventStream()`。
4. `talon.http.client.Client(Connector)` / `ClientWith`，以及 `TcpClient` / `TlsClient`。
5. `talon.core` 的协议引擎、传输、拆帧与 `chain`，以及 core `conn.hijack()` 原语。

`conn.has/get` 与动态 feature 槽尚不存在。早期设计将公开契约视为 semver 管理边界；维护时须识别破坏性变更，但不能把未实现的设想列为调用方可用契约。

## 3. 借鉴与设计取舍

早期设计参考 Kestrel、Netty、tower 和 actix 的问题划分。以下记录 talon 的选择，不构成这些项目当前实现、性能或优劣的比较结论。

| 关注点         | talon 的选择与现状                                               |
| -------------- | ---------------------------------------------------------------- |
| 传输与协议分离 | comptime Listener 与 Proto 契约，TCP / memory 已实现             |
| 中间件组合     | `chain(Ctx, middlewares)`，around 调用，编译期顺序依赖校验       |
| 拆帧           | Reader 上的 LengthPrefixed、Delimited、Accumulator，返回借用切片 |
| 内存复用       | 单规格 buffer pool、请求 arena 保留上限、Debug 借出追踪          |
| stream 背压    | 连接许可限制 accept；写入等待挂起协程，避免另建无界发送队列      |
| 慢速正文防御   | 累计上游等待预算及逐次读 deadline；未使用后台心跳巡检            |
| 连接能力查询   | 静态 feature 合成仍为规划；目前只有远端身份覆盖                  |
| UDP            | 保留独立引擎方向，需求驱动；不模拟 stream 的 accept/Connection   |

显式所有权与 arena 可以明确释放边界，但不会自动消除 use-after-free；借用切片逃逸时仍须复制到足够长寿命的 allocator。comptime 消除了部分运行时分发，`std.Io.Reader/Writer` 本身仍有接口调用。

## 4. zio 依赖边界

talon 直接使用 zio 的 Runtime、Group、网络连接、Timeout、Semaphore、Channel 和同步原语，对外读写接口保持 `std.Io.Reader/Writer`。服务端示例显式将协程 committed 栈设为 64 KiB；应用需按实际调用栈验证容量，不能将此配置直接当作每连接总内存。

旧设计引用 zio v0.14.0 的源码行号与调度特性，不能作为当前固定依赖的证据。当前依赖升级时，应重新检查超时、取消、读写进展、栈及调度语义；不沿用旧版本行号推断能力。客户端 TLS 位于 talon 的 `http/client/tls.zig`，通过 `std.crypto.tls.Client` 包装 zio 传输。

## 5. StreamServer 与 HTTP 协议层

### 5.1 分层与二开契约

`StreamServer(Proto, App)` 调用 `Proto.serve(conn, app)`。协议实现使用 `conn.reader()` / `writer()`，自行组织线性连接循环；`waitReadable` 与 `isShuttingDown` 用于请求边界的可中断等待与退出。HTTP 构造函数将 `Http1Protocol(App)` 交给 core 引擎。

### 5.2 Listener 抽象

Listener 提供 `RawConnection` 类型、`accept()` 和 `close()`；raw 提供 Reader/Writer、`reader(buffer)` / `writer(buffer)`、`shutdown()` / `close()` 和 `remoteInfo()`。当前内置 `TcpListener` 与 `MemoryListener`，没有 Unix listener 或 SO_REUSEPORT 配置。

`MemoryListener` 使用 Channel 与双向配对的 Pipe，支持无 socket 集成测试。多个监听端点可使用多个 server 实例共享 App；共享应用状态的同步由应用负责。

### 5.3 连接中间件链

```zig
const Srv = talon.http.ServerWith(App, .{
    talon.middleware.proxy_protocol,
    talon.middleware.conn_log,
});
var server = try Srv.init(gpa, &app, .{});
// listener 独立创建，传给 server.serve(&listener)。
```

中间件在 `next.call(conn)` 前后执行，不调用 next 即短路。PROXY v2 中间件覆盖远端身份，日志中间件记录连接生命周期。TLS 流替换、IP 限流和 sendFile 的 TLS 降级均为规划，当前不能通过 init 的 `.conn_middleware` 或 `.listener` 字段配置。

### 5.4 连接生命周期与内存模型

每个连接一个任务，从读写 buffer pool 借出缓冲，并建立 arena。读缓冲容量为 `max(max_header_size, 1024) + 1024`，默认 17 KiB，写缓冲默认 4 KiB；池实例各自只有一个规格。Debug 模式追踪借出点和未归还块。

HTTP 每次进入请求循环先重置 arena，默认最多保留 64 KiB，`null` 保留高水位、`0` 释放全部。请求头复制到 arena，再由解析器返回借用切片，避免读取 body 时 reader 缓冲移动使 header 失效。只在需求能被已保留容量满足时复用内存；不保证任意 handler 或大请求零分配。

Connection 原地构造，Reader/Writer 接口依赖稳定地址。请求结束后的数据保存必须使用更长寿命的 allocator；复制到 `req.arena` 不延长生命周期。

### 5.5 HTTP/1.1 协议层

服务端使用自研请求解析器，客户端使用响应解析器，共享 body 与编码机制；不再使用早期 M0 的 `std.http.Server`。请求解析拒绝冲突 CL/TE、重复 CL、非法 header、缺失/重复 Host 等输入；body decoder 拒绝 chunk extension 和 trailer。这是当前支持范围，不应表述为支持所有 HTTP 扩展。

HTTP/1.1 支持 keep-alive 与 pipelining；HTTP/1.0 请求按非持久连接处理。响应写入缓冲，协议循环在阻塞读取前和关闭时 flush，缓冲不足也会输出，不保证单响应只有一次 syscall。Date 缓存按连接维护。没有静态文件 sendFile API。

HEAD 不输出正文；204/205/304 不输出正文；普通响应 API 拒绝 1xx 最终状态。需要复用时排空未读 body，关闭响应则直接 flush 并退出。handler 未写普通响应时通常返回 500，记录 BodyTooSlow 时返回 408；已写响应后不追加替代错误响应。

### 5.6 Server limits 与慢速攻击防御

当前字段及默认值以 [limits.zig](../src/core/limits.zig) 为准：连接上限 65536，请求头 16 KiB，正文 16 MiB，header/keep-alive/drain/write 超时分别为 10/75/30/30 秒，arena 保留上限 64 KiB，正文最低速率 240 bytes/s、宽限 5 秒。

请求头使用完整 head deadline；新连接包含首次等待，复用连接在首字节前使用 keep-alive 超时。body 只累计等待上游数据的时间，handler 计算、主动等待和输出背压不计入；正文提供后续时间额度，chunk 元数据不提供额度。实现使用逐次读取期限，不需要 server 心跳协程。write timeout 约束无写入进展的等待，适用于 HTTP/SSE/WS。

自定义传输缺少超时 setter 时，必须自行提供等价等待约束。完整字段禁用方式与错误语义见 [使用指南](user-guide.md#12-资源限制与回归验证)。

### 5.7 协议分发与连接劫持

HTTP `req.upgrade.accept()` 发送并 flush 101，handler 随后在当前任务内使用升级后的 reader/writer。handler 返回后 HTTP 循环退出，引擎仍负责关闭；该 API 不转移底层连接与池缓冲的所有权。

core `conn.hijack()` 则把 raw 的关闭责任交给调用者，缓冲与 arena 仍在引擎作用域内，完整资源移交尚未实现。两种 API 的生命周期不可混淆。

SSE 和 WebSocket 服务端与客户端均已内置。WebSocket 服务端 helper 的握手检查不完整，所需条件与 subprotocol 选择仍需调用方检查，见 [升级说明](user-guide.md#8-连接劫持升级到自定义协议)。AutoProtocol、ALPN/h2 分发与 QUIC 仍为规划。

### 5.8 生命周期与优雅停机

`shutdown()` 标记停机并触发 stop event；`serve()` 取消 accept task、关闭 listener，然后 drain 连接 Group，超时后 cancel。取消 accept 用于打断关闭 fd 不一定能唤醒的等待。空闲 HTTP 连接通过 `waitReadable` 的短超时检查停机；持续 handler、SSE 或 WebSocket 可占用 drain 窗口，最后由取消结束。

`on_start` / `on_shutdown` 和 server 后台任务注册口尚未实现。应用自行创建的 Group 和客户端回收任务应由应用自行结束，不能假定自动加入 server 的连接 Group。

## 6. Feature 机制（规划）

早期方向是按 Listener 与中间件声明在 comptime 合成连接能力，例如 `conn.has(TlsInfo)` / `conn.get(TlsInfo)`，必要时提供单一动态扩展槽。当前 Connection 没有这些 API，也没有动态槽；只有 `remoteInfo` / `setRemoteInfo`。

`chain` 已支持 `provides` / `requires` 声明及查询，但声明能力不会自动为 Connection 增加字段。应由具体能力需求驱动合成设计，不把声明检查等同于运行时数据已存在。

## 7. chain 组合器

`chain(Ctx, middlewares)` 组合函数或带 `run` 的 struct 中间件，签名接受 `*Ctx` 与 next；终端可以是函数或带 `call` 的有状态值。`next.call(ctx)` 前后分别用于进入与退出处理。

struct 可声明 `provides` / `requires`，后者必须由链中更早的中间件提供，顺序错误在编译期拒绝。组合器已用于连接级及 HTTP 客户端请求级；wing 请求级和 datagram 可复用此机制，但其独立实现不由本仓库证明。

## 8. framing 工具箱

公开入口为 `talon.core.framing`，不是 `talon.framing`。三种组件建立在 `*std.Io.Reader` 上：

```zig
var framed = talon.core.framing.LengthPrefixed(.{
    .length_type = u32, .endian = .big,
    .max_frame = 1024 * 1024, .includes_header = false,
}).init(conn.reader());
const frame: ?[]const u8 = try framed.next();

var lines = talon.core.framing.Delimited(.{
    .delimiter = "\r\n", .max_frame = 64 * 1024,
}).init(conn.reader());

var decoder = talon.core.framing.Accumulator(MyDecoder).init(conn.reader(), max_frame);
```

以上为 API 片段，完整协议见 [RESP 示例](../examples/resp.zig)。`null` 表示干净 EOF；帧中间结束返回 PartialFrame，帧超过限额或缓冲容量返回 FrameTooLarge。帧必须连同 framing 所需字节放进 reader 缓冲；只调高 `max_frame` 不会扩容默认 17 KiB 的服务端读缓冲。返回切片借用缓冲，到下一次读取前有效。

## 9. DatagramServer：UDP 引擎（规划）

UDP 没有 accept，报文天然分帧。保留独立 sibling 引擎的方向，复用适合报文语义的 chain、池和生命周期策略，不复用 stream 的连接模型。

候选模式包括无状态的 packet/reply 处理，以及按 SessionKey 分组的 SessionTable、有界 mailbox、idle 驱逐和会话上限。mailbox 满时的丢弃或等待策略必须由协议需求明确。SO_REUSEPORT 多 socket、会话任务和停机次序需要单独验证。

当前没有 `DatagramServer`、`SessionTable`、Packet 或 Replier 类型，也没有占位源文件；待真实 UDP 协议需求出现后设计与实现。它们不是现有 QUIC 支持的证据。

## 10. 性能策略与项目结构

现有优化机制是 comptime 协议/中间件组合、缓冲读写、arena 复用、单规格池和 Date 缓存。多规格池、sendFile、SO_REUSEPORT 及专用 SIMD 扫描仍为候选；不宣称具体吞吐、每连接总内存或与其他框架的排名。

[build.zig](../build.zig) 导出一个模块并构建九个示例；源码位于 `src/core/`、`src/http/codec/`、`src/http/server/`、`src/http/client/`，测试由 `tests/all.zig` 汇总。完整目录见 [开发者指南](developer-guide.md#2-分层与模块地图)。没有旧设计中的 datagram 占位文件或独立 `bench/` 目录。

现有 [客户端基准示例](../examples/http_client_bench.zig) 用于客户端测量。服务端对照基准可固定请求负载、连接数、keep-alive 策略、构建模式与运行环境；只有实际测量及原始记录才能支持性能结论。历史服务端 TCP 基准工具已删除，仓库无 `performance.md` 可引用。

## 11. 实施路线图（talon 侧）

| 阶段         | 当前状态与剩余方向                                                                                       |
| ------------ | -------------------------------------------------------------------------------------------------------- |
| M0/M1 基础   | core、HTTP 自研解析、池、连接中间件、framing 已实现；M0 的 std.http 已替换。历史基准目标不代表已通过验收 |
| 已有后续能力 | HTTP/HTTPS 客户端、SSE/WS 双向支持、body 速率防御、写入超时、arena 保留限制                              |
| M3 剩余方向  | 完整劫持资源转移、指标/生命周期钩子、SO_REUSEPORT、多规格池；UDP/SessionTable 需求驱动                   |
| M4 方向      | 服务端 TLS 中间件、AutoProtocol/h2 评估、QUIC/HTTP3 预研，需另行设计                                     |

M2 原指 wing 侧工作，本仓库不跟踪其完成情况。历史路线图中的吞吐改进、fuzz 无 crash、24 小时长稳无泄漏等是验证目标，不是本文件确认的结果；body 速率防御已通过逐次 deadline 实现，不能继续将“实现心跳”列为其必要前提。

## 12. 风险与权衡

- **运行时耦合**：直接依赖 zio 的超时与任务模型有利于表达生命周期，但需要在升级固定依赖时重验行为。
- **内存与生命周期**：协程栈、池缓冲、arena 与应用分配共同构成连接成本；借用数据和升级句柄不能越过其所有者生命周期。
- **comptime 复杂度**：泛型组合可能拉长编译时间与报错路径，入口应尽量给出明确诊断。当前没有 `talon.DefaultServer` 别名或专用编译耗时跟踪承诺。
- **TLS 边界**：客户端 TLS 已实现，服务端 TLS 仍缺位，入站 HTTPS 需由外部终止层处理。
- **客户端资源约束**：解压后大小检查目前在完整读取后进行；池并发限制默认关闭。调用方不能把默认选项当作所有工作负载的内存与并发硬上限。
- **规划范围**：feature、UDP、多协议分发需由具体消费者推动，避免先建立缺乏验证场景的抽象。调度亲和性与多核扩展的选择应依赖当前 zio 与实际测量。
