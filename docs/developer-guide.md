# talon 开发者指南

> 面向参与 talon 开发的工程师：讲清楚架构怎么分层、关键实现怎么落地、如何在此基础上继续开发。
> 配套文档：使用方式见 [使用指南](user-guide.md)，设计与规划见 [架构设计](talon-architecture.md)。
> 适用版本：当前仓库实现，Zig `0.16.0`，固定 zio 依赖见 [build.zig.zon](../build.zig.zon)。

---

## 1. 实现现状速览

当前实现已超过早期 M0/M1 范围，包含 HTTP 客户端、客户端 TLS、SSE 和 WebSocket；M3/M4 中仍有未实现项目。下表按能力区分现状：

| 能力                                                              | 状态      | 位置 / 说明                                                               |
| ----------------------------------------------------------------- | --------- | ------------------------------------------------------------------------- |
| `chain` 组合器                                                    | ✅        | `src/core/chain.zig`，含 `provides`/`requires` comptime 校验              |
| `StreamServer` + 优雅停机                                         | ✅        | `src/core/stream_server.zig`                                              |
| `TcpListener` / `MemoryListener`                                  | ✅        | `src/core/listener.zig`                                                   |
| 连接 `Connection` + hijack                                        | ✅        | `src/core/connection.zig`                                                 |
| 连接中间件 `proxy_protocol` / `conn_log`                          | ✅        | `src/core/middleware.zig`                                                 |
| `framing`（LengthPrefixed/Delimited/Accumulator）                 | ✅        | `src/core/framing.zig`                                                    |
| `BufferPool` + Debug 借出追踪                                     | ✅        | `src/core/buffer_pool.zig`                                                |
| 内存管道 `Pipe`                                                   | ✅        | `src/core/pipe.zig`                                                       |
| HTTP/1.1 自研解析器 + 走私防御                                    | ✅        | `src/http/codec/request_parser.zig`                                       |
| `BodyReader`（CL + chunked，流式）                                | ✅        | `src/http/codec/body.zig`                                                 |
| 响应编码（定长 + chunked + Date 缓存）                            | ✅        | `src/http/codec/response_encode.zig`                                      |
| `Http1Protocol` keep-alive 循环                                   | ✅        | `src/http/server/protocol.zig`                                            |
| HTTP 客户端与连接池                                               | ✅        | `src/http/client/`；重定向、重试、压缩解码、Cookie 中间件                 |
| 客户端 TLS                                                        | ✅        | `src/http/client/tls.zig`；不等于服务端 TLS                               |
| SSE / WebSocket 双向支持                                          | ✅        | `src/http/server/`、`src/http/client/`，共享 `codec/`                     |
| `UnixListener`                                                    | ❌ 未落地 | 架构 §5.2 预留，`listener.zig` 仅 tcp/memory                              |
| 类型化 feature 查询 `conn.has/get`（§6）                          | ❌ 未落地 | `Connection` 仅有 `remoteInfo` 覆盖；`chain.provides` 已就绪但未接入      |
| 慢速 body 速率防御                                                | ✅        | 按上游读取等待时间累计预算；用 per-read deadline 执行，不需要独立心跳协程 |
| `DatagramServer` / `SessionTable`（§9）                           | ❌ M3     | 文件尚未创建；底座按双消费者设计但未实现 UDP                              |
| `ip_rate_limit` 中间件、`SO_REUSEPORT`、多 size class、`sendFile` | ❌ M3     | 架构提及，代码未落地                                                      |
| TLS 中间件、AutoProtocol/h2、QUIC                                 | ❌ M4     | 另立设计                                                                  |

---

## 2. 分层与模块地图

```
talon (src/talon.zig)        module "talon"   ← 唯一导出模块
  ├─ core (src/core/core.zig) talon.core       ← 协议无关引擎（相对 import，非独立模块）
  ├─ http (src/http/http.zig) talon.http       ← HTTP/1.1 协议层
  └─ zio                                        ← 协程运行时（per-op 超时、Group、net、sync）
```

`build.zig` 只导出 `talon` 一个 module。core/http 都在同一模块根目录内，方向边界依靠 import 约定与审查；相对 import 不会自动阻止 core 引用 http。当前没有独立的编译隔离守卫。

### 目录

```text
src/
├── talon.zig                 # 唯一模块入口
├── core/
│   ├── core.zig              # core 导出面
│   ├── chain.zig             # comptime 组合器
│   ├── stream_server.zig     # accept、连接任务与 drain
│   ├── connection.zig        # reader/writer、超时、hijack
│   ├── listener.zig          # TCP / memory transport
│   ├── middleware.zig        # PROXY v2、连接日志
│   ├── framing.zig           # 三种拆帧组件
│   ├── limits.zig            # 服务端限制
│   ├── buffer_pool.zig       # 单规格池与 Debug 借出追踪
│   └── pipe.zig              # 内存传输
└── http/
    ├── http.zig              # codec/client 与服务端 API 导出
    ├── codec/               # head、request/response parse/encode、body、SSE、WS
    ├── server/              # HTTP 循环、Request/Response、SSE、WS
    └── client/              # 策略、connector、TLS、connection、pool、cookies
examples/                    # HTTP、HTTPS 客户端、SSE、WS、RESP 及客户端基准
tests/                      # 集成、属性和 fuzz 测试；入口 all.zig
```

---

## 3. 核心设计与关键实现

### 3.1 comptime Proto 契约（零虚调用）

引擎与协议解耦的方式是 comptime 泛型，而非 vtable。`StreamServer(Proto, App)` 在编译期校验 `Proto.serve` 存在，并把 App 类型烤进服务器类型：

以下为结构示意，省略校验消息与实现：

```zig
// stream_server.zig
pub fn StreamServerWith(comptime Proto: type, comptime App: type, comptime middlewares: anytype) type {
    comptime if (!@hasDecl(Proto, "serve")) @compileError(...);
    return struct { ... };
}
```

HTTP 层只是把 `Http1Protocol(App)` 当成一个 `Proto` 喂进去：

```zig
// http/http.zig
pub fn Server(comptime App: type) type {
    return core.StreamServer(Http1Protocol(App), App);
}
```

`Proto.serve(conn: anytype, app: *App)` 是普通协程函数，内部写线性的连接循环。

### 3.2 chain：可复用的中间件组合器

`chain(Ctx, middlewares)`（`chain.zig`）泛型于上下文类型，comptime 把元组展开成嵌套 inline 调用，优化后等价手写大函数，没有中间件链的运行时间接分发；底层 `std.Io.Reader/Writer` 仍使用接口。当前用于连接级与客户端请求级；datagram 和 wing 请求级是可复用方向。

实现要点：

- 中间件可以是函数 `fn (ctx: *Ctx, next: anytype) !void`，也可以是带 `run` 的 struct 类型。
- around 式：`next.call(ctx)` 之前 inbound、之后 outbound——单一抽象天然双向（拒绝 Netty 的 inbound/outbound 双链）。
- struct 中间件可声明 `pub const provides = .{Cap}` 与 `pub const requires = .{Cap}`；`requires` 在 comptime 对照「链中更早的 provides」校验，**把中间件顺序错误变成编译错误**。
- `provides(F)` 是 comptime 查询，规划用于 §6 的 Connection 能力合成（当前尚未接入）。

终端 handler 既可是 `fn (*Ctx)`，也可是带 `call` 方法的有状态值——`StreamServer` 用后者把 `Proto.serve` 包成终端（`ProtoTerminal`）。

### 3.3 连接生命周期（StreamServer）

`serve()` 的编排（`stream_server.zig`）：

1. 把 accept 循环 spawn 成内部 task——因为关闭 listener fd 在某些平台（macOS/kqueue）**唤不醒** parked 的 accept，需要靠 task cancel 打断。
2. `stop_event.wait()` 阻塞，直到 `shutdown()` 或 accept 自行退出。
3. 停机序列（架构 §5.8）：置 `shutting_down` → cancel accept task → 关 listener → `drain()`。
4. `drain()`：spawn 一个 waiter 等 `group.wait()`，`done.timedWait(drain_timeout)` 超时则 `group.cancel()` 硬取消滞留连接。

每连接一个协程（`ConnTask`）：

- 从 `read_pool` / `write_pool` 各租一块 buffer，建一个每连接 `ArenaAllocator`。
- 原地构造 `Connection`（**不可移动**，见 3.7），跑 `ConnChain.run(&conn, ProtoTerminal)`。
- `defer`：非劫持连接由引擎 `shutdown()` + `close()`；劫持连接交给 hijacker。
- 例行终止（`Canceled`/`ReadFailed`/`WriteFailed`/`EndOfStream`）不算服务器故障，其余记 `warn`。

`max_connections` 由 `conn_sem`（Semaphore）在 accept 前 `wait` 实现背压。

### 3.4 HTTP/1.1 每请求循环（Http1Protocol）

`server/protocol.zig` 的 `serve` 单连接循环，每请求：

1. 请求边界先按 `max_retained_arena` 重置 arena，再在 reader 无缓冲字节时 flush 待发响应；pipelining 可合并输出，不保证单次 syscall。
2. `conn.waitReadable`：首请求使用 header deadline，复用连接先按 keep-alive 超时等待首字节，再设置完整 head deadline；等待可检查停机标志。
3. `findHeadEnd` 扫描 `\r\n\r\n`，按需 `fillMore`；阻塞 refill 前先 flush 待发响应。
4. 将 head 字节 `dupe` 进 arena（见 3.6）并调用 `parser.parse`。
5. 构造 `BodyReader`（无 body 的热路径跳过 body buffer）、`Request`、`Response`，调 `app.handle`。
6. 成功升级后退出 HTTP 循环；普通 handler 未写响应时通常回 500，已记录 BodyTooSlow 则回 408。决定关闭时先 flush 并退出；需要复用时才 `body.discard()`，失败则 flush 已写响应并关闭。

错误响应统一走 `respondErrorAndClose`：写完即由 `serve` 返回触发关连接。框架违规（如走私）必须关连接，绝不把后续字节当新请求解析。

### 3.5 解析器：纯函数 + 走私防御

`codec/request_parser.zig` 的 `parse(bytes, headers_storage) -> Head` 是纯函数（bytes in / struct out，零分配零 I/O，返回切片借用输入），因此可独立 fuzz。使用 `std.mem` 扫描并按 header 名处理语义字段；具体是否生成 SIMD 指令取决于工具链与目标，不构成独立性能结论。

走私防御（RFC 9112 严格化）是这层的重点，相关用例见 `tests/http_server_test.zig`、`tests/body_test.zig` 和解析器单元测试：

- 请求行单 SP、method 必须是 token、version 精确匹配。
- header 名 token-only（顺带拒绝 `Name :`）、拒绝 obs-fold、拒绝 bare CR/LF。
- `Content-Length` 纯数字、任何重复即拒、溢出拒。
- `Transfer-Encoding` 只接受精确的 `chunked`，HTTP/1.0 上的 TE 拒。
- **CL + TE 同时出现拒**（走私基石）、重复 Host 拒、HTTP/1.1 缺 Host 拒。

body 侧 `codec/body.zig` 是对称的严格化：chunk size 纯 hex（无 extension、无 `0x`）、精确 CRLF、拒绝 trailer。

### 3.6 内存模型：arena + 借用 + buffer pool

- **每请求 arena**：`Connection.arena` 在请求边界使用 `reset(.{ .retain_with_limit = max })`，默认最多保留 64 KiB；配置 `max_retained_arena = null` 时保留高水位。请求需求超过保留量时可能重新分配。
- **借用切片**：header/target 借用 arena 里的 head 拷贝，生命周期 = 当前请求。
- **为什么拷一份 head**：header 切片要在 handler 读 body 期间保持有效，而 body 走同一个 `std.Io.Reader`，其缓冲会在 refill 时 rebase。拷几百字节的 head 进 arena，用一次 memcpy 换取生命周期正确性，同时保住零拷贝解析（见 `server/protocol.zig` 文件头注释）。
- **BufferPool**（`buffer_pool.zig`）：当前每个池实例单一 size class（server 跑两个池：read 容量为 `max(max_header_size, 1024) + 1024`，默认 17 KiB；write 4 KiB）。临界区 O(1) 无挂起点，用自旋锁而非协程 mutex，保持池与运行时无关。
  - Debug build 用 `@returnAddress` 记录每次借出点，`deinit` 报告「租了没还」的 buffer——GPA 泄漏检测只覆盖 malloc 路径，池内借出未还是它的盲区。Release build comptime 全裁掉。

### 3.7 Connection 合成与不可移动约束

`Connection(Raw)` 在 comptime 由 listener 的 raw 连接类型合成——raw 类型静态确定。它持有 `Raw.Reader` / `Raw.Writer` 状态值。

> **不可移动**：reader/writer 的 `std.Io` 接口通过 `@fieldParentPtr` 反查父结构，因此 `Connection` init 后不能移动。必须在连接协程里**原地构造**并传指针（见 `connection.zig` 文件头注释）。同理 `PipeReader`/`BodyReader`/`ChunkedBodyWriter` 都靠 `@fieldParentPtr("interface", ...)` 自指。

`waitReadable` 值得注意：transport 支持 setTimeout 时，用 1s 短超时 tick 轮询 + 中间检查停机标志；没有读超时的自定义 transport 退化为普通阻塞 `fill(1)`；内存管道已支持超时。没有它，空闲 keep-alive 连接只能等 drain-timeout 才死，停机会拖满整个 drain 窗口。

### 3.8 内存传输：Pipe + MemoryListener

`pipe.zig` 是单向字节环形缓冲，读写在空/满时挂起协程（zio Mutex + Condition）。`PipeReader`/`PipeWriter` 把它包成标准 `std.Io.Reader/Writer`，错误落在 `err` 字段、接口报 `ReadFailed`/`WriteFailed`（镜像 zio Stream.Reader 形状）。

`MemoryListener.connect()` 造一对 pipe，server 侧塞进 `zio.Channel` 给 `accept()`，返回 client 侧。连接内存（pipe 环）活到 `deinit()`，所以测试客户端可以比单条连接活得久。这让整条栈无 socket 可测——是完整的协议测试入口。

---

## 4. 开发工作流

### 构建与测试

```bash
zig build                # 编译并安装 examples，编译它们引用的库代码
zig build test           # 运行库单元测试与 tests/all.zig 集成测试
zig build run-http       # 手动验证 HTTP
zig build run-resp       # 手动验证自定义协议路径
```

`build.zig` 建一个 `addTest`（`talon_mod`，整个 talon 模块 = core + http，覆盖全部单元测试）与一个 `tests/all.zig` 集成测试二进制，`test` step 依赖两者。新增源文件后，记得在对应入口文件（`core/core.zig` 或 `http/http.zig`）的 `test {}` 块里 `_ = @import("...")`，确保测试可达；新增集成测试需加入 `tests/all.zig`。

### 测试约定

- 单元测试与被测代码同文件，文件尾 `// ── Tests ──` 分隔。
- 集成测试在 `tests/`，由 `tests/all.zig` 汇总：用 `MemoryListener`（或少数 `TcpListener`）+ `zio.Group` 双协程（server + client）跑真实请求循环，断言后 `s.shutdown()`，最后 `try std.testing.expect(!group.hasFailed())`。
- 涉及网络/协程的测试必须先 `zio.Runtime.init`。

### Fuzz

请求/响应解析器的 fuzz 入口位于 `tests/parser_fuzz_test.zig` 与 `tests/response_parser_fuzz_test.zig`，另有 body、SSE、WebSocket 的确定性变异与属性测试：

- `std.testing.fuzz` 标准入口（`zig build test --fuzz`）。
- 确定性的 in-process fuzz：请求/响应 head 各 200k 变异，body/SSE/WebSocket 各 100k。测试注释记录了 Zig 0.16 工具链 fuzz runner 重建问题；常规 `zig build test` 包含这些确定性用例。存在用例不等于本次已执行，也不能证明任意输入都不会出错。

### 调试陷阱

- **不要用 `zio.debug_io` 重定向 std.log**：stderr 是普通文件时，日志写经 zio loop 会在 task 上下文外 panic（见两个 example 的注释）。example 用默认阻塞 stderr。
- **协程栈**：仓库服务端示例显式设置 `stack_pool.committed_size = 64 * 1024`。栈是每协程成本，调小前应验证 handler 的栈需求；此数值不等于实际每连接总内存。

---

## 5. 扩展点：如何往里加东西

### 5.1 加一个自定义协议（Proto）

以下为协议骨架，省略 App 定义与帧处理；使用 1 MiB 帧时还需提供能容纳完整帧的 reader 缓冲：

```zig
const MyProto = struct {
    pub fn serve(conn: anytype, app: *App) anyerror!void {
        // 用 framing 拆帧，不要手写缓冲管理
        var frames = core.framing.LengthPrefixed(.{ .length_type = u32, .max_frame = 1 << 20 })
            .init(conn.reader());
        while (true) {
            conn.waitReadable(conn.limits.keep_alive_timeout) catch return; // 可被停机打断
            const frame = (frames.next() catch return) orelse return;
            // ... 处理 frame，写 conn.writer()，flush ...
            if (conn.isShuttingDown()) return; // 请求边界退出
        }
    }
};
const Server = core.StreamServer(MyProto, App);
```

要点：请求边界用 `waitReadable` + 检查 `isShuttingDown`，让停机能及时打断空闲连接，而不是拖到 drain-timeout。参照 `examples/resp.zig`。

### 5.2 加一个连接中间件

下面为示意片段，`rejected` 代表应用自己的拒绝条件：

```zig
fn my_mw(conn: anytype, next: anytype) anyerror!void {
    // inbound：next 之前
    if (rejected) return;          // 不调 next = 拒绝连接
    try next.call(conn);
    // outbound：next 之后
}
// 用法：talon.http.ServerWith(App, .{ my_mw, talon.middleware.conn_log })
```

要发布能力给下游、或声明顺序依赖，用 struct + `provides`/`requires`（见 `chain.zig` 测试）。要改写远端身份调 `conn.setRemoteInfo`（参照 `proxy_protocol`）。

### 5.3 加一个 framing 组件

照 `framing.zig` 三件套的形状：构造在 `*std.Io.Reader` 上、`next()` 返回 `Error!?[]const u8`（`null` = 干净 EOF）、frame 是借用切片（下次 `next` 失效）、自带 `max_frame` 防御且完整帧必须放进 reader 缓冲、用 `peek/take/toss/fillMore` 管缓冲。自定义状态机优先用 `Accumulator(Decoder)`，只写 `decode(window) !DecodeResult`。

### 5.4 加一个 listener

满足 comptime 契约（`listener.zig` 文件头 + `stream_server.zig` 的 `validateListener`）：

- `pub const RawConnection: type`
- `accept(self) !RawConnection`、`close(self) void`
- RawConnection 提供：`Reader`/`Writer` 类型（带 `interface: std.Io.Reader/Writer` 字段）、`reader(buf)`/`writer(buf)`、`close()`/`shutdown()`/`remoteInfo()`

入口校验必要声明，具体签名由泛型实例化检查；不保证所有签名错误都有定制诊断。

---

## 6. 关键约束与权衡（开发时必须记住）

- **绑定 zio native API**：talon 核心直接使用 zio 的 per-op `Timeout`、`Group` 与取消机制。牺牲可移植性，但对外 reader/writer 仍是标准 `std.Io` 接口。
- **comptime 重度使用**：中间件链、Connection 合成会放大编译错误的间接性。现有入口对部分声明或类型约束提供显式 `@compileError`；新增入口也应尽量提供明确诊断，不能假定已完整校验所有签名。
- **借用生命周期**：framing frame、HTTP header/target 都是借用切片，跨生命周期保存必须用更长寿命的 allocator `dupe`，不能复制到即将 reset 的请求 arena。文档与注释里反复强调，是用户 use-after-free 的高发区。
- **停机正确性**：accept 用 task cancel 打断、空闲连接靠 `waitReadable` 打断、滞留连接靠 drain-timeout cancel——三条路径都有测试（`tests/stream_server_test.zig` + `tests/http_server_test.zig`）。改动连接循环时务必保住这三条。
- **架构按双消费者设计**：共享底座（chain、buffer pool、Limits、生命周期）从 M0 起就按 stream + datagram 双消费者审视，但 datagram 实现需求驱动（M3）。加底座能力时保持传输语义无关。

---

## 7. 客户端实现与维护边界

`http/client/client.zig` 负责请求策略、中间件、重试、重定向与 SSE/WS 高层 API；`connector.zig` 负责建连，`tls.zig` 提供 TLS transport，`connection.zig` 负责单连接请求编码、响应解析与解压，`pool.zig` 管理复用、origin 并发许可和回收。共享协议词汇与编解码来自 `http/codec/`，服务端不应依赖客户端策略。

客户端 `Response` 持有连接直到 `deinit()`，正文排空成功且可复用才回池。origin 许可状态必须覆盖等待者、持有者及空闲连接；最后一个使用者离开且无空闲连接时回收。`Client.deinit()` 前必须结束请求、Response、SSE source、WebSocket 和自行启动的 `reapLoop`。相关回归入口为 `tests/http_client_test.zig` 和 `tests/ws_client_test.zig`。

TLS 使用 `std.crypto.tls.Client` 包装 zio 传输。系统根证书由调用方加载并保持存活；测试见 `tests/http_client_tls_test.zig`。客户端 TLS 不提供入站 TLS 终止能力。

`readAllAlloc` / `json` 先完整读取，再检查 `max_decoded`；不要把参数描述为分配期间的硬限制。`max_response_body` 限制编码后的输入。维护解压路径时应同时审视正文边界与解码后资源消耗。

## 8. 路线图对照（架构 §11）

早期 M0/M1 的 core 和 HTTP/1.1 基础已实现，当前不再依赖 `std.http.Server`。慢速 body 防御、写入超时、arena 保留限额以及客户端/SSE/WS 已落地，不能继续全部归入待办 M3。

仍未实现的项目包括完整 raw 劫持资源转移、feature 查询、Unix listener、UDP/SessionTable、指标与生命周期钩子、SO_REUSEPORT、多规格 buffer pool、sendFile、服务端 TLS、AutoProtocol/h2 和 QUIC。路线图是设计方向，不是已通过性能或长稳验收的记录，见 [架构设计](talon-architecture.md)。
