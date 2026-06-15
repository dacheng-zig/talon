# talon 开发者指南

> 面向参与 talon 开发的工程师：讲清楚架构怎么分层、关键实现怎么落地、如何在此基础上继续开发。
> 配套文档：使用方式见 `user-guide.md`，设计裁决与借鉴依据见 `talon-architecture.md`（本指南以「当前代码实现现状」为准，与架构文档的 M3/M4 蓝图区分开）。
> 适用版本：M0 + M1 已落地，Zig `0.16.0`，依赖 zio。

---

## 1. 实现现状速览

架构文档描绘了 M0→M4 完整蓝图；本仓库当前**落地到 M1**。下表是代码事实，避免按蓝图误判：

| 能力 | 状态 | 位置 / 说明 |
|------|------|------|
| `chain` 组合器 | ✅ | `src/core/chain.zig`，含 `provides`/`requires` comptime 校验 |
| `StreamServer` + 优雅停机 | ✅ | `src/core/stream_server.zig` |
| `TcpListener` / `MemoryListener` | ✅ | `src/core/listener.zig` |
| 连接 `Connection` + hijack | ✅ | `src/core/connection.zig` |
| 连接中间件 `proxy_protocol` / `conn_log` | ✅ | `src/core/conn_middleware.zig` |
| `framing`（LengthPrefixed/Delimited/Accumulator） | ✅ | `src/core/framing.zig` |
| `BufferPool` + Debug 借出追踪 | ✅ | `src/core/buffer_pool.zig` |
| 内存管道 `Pipe` | ✅ | `src/core/pipe.zig` |
| HTTP/1.1 自研解析器 + 走私防御 | ✅ | `src/http/parser.zig` |
| `BodyReader`（CL + chunked，流式） | ✅ | `src/http/body.zig` |
| 响应编码（定长 + chunked + Date 缓存） | ✅ | `src/http/encode.zig` |
| `Http1Protocol` keep-alive 循环 | ✅ | `src/http/protocol.zig` |
| `UnixListener` | ❌ 未落地 | 架构 §5.2 预留，`listener.zig` 仅 tcp/memory |
| 类型化 feature 查询 `conn.has/get`（§6） | ❌ 未落地 | `Connection` 仅有 `remoteInfo` 覆盖；`chain.provides` 已就绪但未接入 |
| 慢速 body 速率防御 + 心跳（§5.6） | ❌ M3 | `Limits.min_body_data_rate` 已定义但不强制 |
| `DatagramServer` / `SessionTable`（§9） | ❌ M3 | 文件尚未创建；底座按双消费者设计但未实现 UDP |
| `ip_rate_limit` 中间件、`SO_REUSEPORT`、多 size class、`sendFile` | ❌ M3 | 架构提及，代码未落地 |
| TLS 中间件、AutoProtocol/h2、QUIC | ❌ M4 | 另立设计 |
| `bench/` 基准目录 | ❌ | 架构 §10 列出，`build.zig` 未含基准步骤 |

---

## 2. 分层与模块地图

```
talon (src/talon.zig)        module "talon"   ← 唯一导出模块
  ├─ core (src/core/core.zig) talon.core       ← 协议无关引擎（相对 import，非独立模块）
  ├─ http (src/http/http.zig) talon.http       ← HTTP/1.1 协议层
  └─ zio                                        ← 协程运行时（per-op 超时、Group、net、sync）
```

`build.zig` 只导出 `talon` 一个 module。引擎另以一个 `addObject`（`core-boundary`，根在 `src/core/`、只带 zio）单独编译一遍当 core/http 边界守卫：Zig 禁止 `@import` 模块根目录之外的文件，core 一旦误依赖 `src/http/` 就编译失败（架构 §10，原由 `resp` 示例承担）。只编译、不跑测试。

### 目录

```
src/
├── talon.zig              # module talon 的根：core 顶层导出 + talon.http 包
├── core/
│   ├── core.zig            # talon.core 的导出面（引擎根，相对 import）
│   ├── chain.zig           # 泛型中间件组合器（三处复用的地基）
│   ├── stream_server.zig   # StreamServer(Proto, App)、accept、生命周期、drain
│   ├── connection.zig      # Connection(Raw)：reader/writer/hijack/waitReadable
│   ├── listener.zig        # Listener 契约 + TcpListener/MemoryListener
│   ├── conn_middleware.zig # proxy_protocol、conn_log
│   ├── framing.zig         # LengthPrefixed / Delimited / Accumulator
│   ├── limits.zig          # Limits、DataRate
│   ├── buffer_pool.zig     # 单 size class 池 + Debug 借出追踪
│   └── pipe.zig            # 内存管道（MemoryListener 底层）
├── http/
│   ├── http.zig            # package talon.http、Server/ServerWith、集成测试
│   ├── protocol.zig        # Http1Protocol：每请求循环
│   ├── parser.zig          # 纯函数 head 解析器（独立 fuzz）
│   ├── body.zig            # BodyReader：CL + chunked 流式解码
│   ├── encode.zig          # writeHead、DateCache、ChunkedBodyWriter
│   ├── request.zig         # Request
│   └── response.zig        # Response
examples/
├── http.zig                # 链接 talon
└── resp.zig                # 链接 talon，只用 talon.core（非 HTTP 协议示例）
```

---

## 3. 核心设计与关键实现

### 3.1 comptime Proto 契约（零虚调用）

引擎与协议解耦的方式是 comptime 泛型，而非 vtable。`StreamServer(Proto, App)` 在编译期校验 `Proto.serve` 存在，并把 App 类型烤进服务器类型：

```zig
// stream_server.zig
pub fn StreamServerWith(comptime Proto: type, comptime App: type, comptime conn_middleware: anytype) type {
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

`Proto.serve(conn: anytype, app: *App)` 是普通协程函数，内部写线性的连接循环——有栈协程的核心红利，对比 Kestrel 的 async 状态机更易读易测。

### 3.2 chain：三处复用的中间件组合器

`chain(Ctx, middlewares)`（`chain.zig`）泛型于上下文类型，comptime 把元组展开成嵌套 inline 调用，优化后等价手写大函数，无间接分发。一套机制服务：talon 连接级、（未来）datagram 报文级、wing 请求级。

实现要点：

- 中间件可以是函数 `fn (ctx: *Ctx, next: anytype) !void`，也可以是带 `run` 的 struct 类型。
- around 式：`next.call(ctx)` 之前 inbound、之后 outbound——单一抽象天然双向（拒绝 Netty 的 inbound/outbound 双链）。
- struct 中间件可声明 `pub const provides = .{Cap}` 与 `pub const requires = .{Cap}`；`requires` 在 comptime 对照「链中更早的 provides」校验，**把中间件顺序错误变成编译错误**（参照系统都没有的 comptime 红利）。
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

`protocol.zig` 的 `serve` 单连接循环，每请求：

1. `r.bufferedLen() == 0` 时才 flush 待发响应——pipelining 下响应在写缓冲累积，一次 vectored syscall 发出。
2. `conn.waitReadable(keep_alive_timeout)`：可被停机打断的请求边界空闲等待。
3. `findHeadEnd`：扫 `\r\n\r\n`，按需 `fillMore`；**阻塞 refill 前先 flush 待发响应**，避免对端在排队输出上干等。
4. `arena.reset(.retain_capacity)` → 把 head 字节 `dupe` 进 arena（见 3.6 为什么要拷）→ `parser.parse` 纯函数解析。
5. 构造 `BodyReader`（无 body 的热路径跳过 body buffer）、`Request`、`Response`，调 `app.handle`。
6. handler 没写响应是契约违反，回 500;有 body 未读完则 `body.discard()` 排空;按协商结果决定 keep-alive 或关连接。

错误响应统一走 `respondErrorAndClose`：写完即由 `serve` 返回触发关连接。框架违规（如走私）必须关连接，绝不把后续字节当新请求解析。

### 3.5 解析器：纯函数 + 走私防御

`parser.zig` 的 `parse(bytes, headers_storage) -> Head` 是纯函数（bytes in / struct out，零分配零 I/O，返回切片借用输入），因此可独立 fuzz。单遍扫描（picohttpparser 思路），`std.mem` SIMD 扫描，按 `name.len` 分派的 comptime 快速路径处理语义 header。

走私防御（RFC 9112 严格化）是这层的重点，全部有测试覆盖：

- 请求行单 SP、method 必须是 token、version 精确匹配。
- header 名 token-only（顺带拒绝 `Name :`）、拒绝 obs-fold、拒绝 bare CR/LF。
- `Content-Length` 纯数字、任何重复即拒、溢出拒。
- `Transfer-Encoding` 只接受精确的 `chunked`，HTTP/1.0 上的 TE 拒。
- **CL + TE 同时出现拒**（走私基石）、重复 Host 拒、HTTP/1.1 缺 Host 拒。

body 侧 `body.zig` 是对称的严格化：chunk size 纯 hex（无 extension、无 `0x`）、精确 CRLF、拒绝 trailer。

### 3.6 内存模型：arena + 借用 + buffer pool

- **每请求 arena**：`Connection.arena` 每请求 `reset(.retain_capacity)`，稳态热路径无 malloc/free。
- **借用切片**：header/target 借用 arena 里的 head 拷贝，生命周期 = 当前请求。
- **为什么拷一份 head**：header 切片要在 handler 读 body 期间保持有效，而 body 走同一个 `std.Io.Reader`，其缓冲会在 refill 时 rebase。拷几百字节的 head 进 arena，用一次 memcpy 换取生命周期正确性，同时保住零拷贝解析（见 `protocol.zig` 文件头注释）。
- **BufferPool**（`buffer_pool.zig`）：当前每个池实例单一 size class（server 跑两个池：read 16K = `max_header_size`，write 4K）。临界区 O(1) 无挂起点，用自旋锁而非协程 mutex，保持池与运行时无关。
  - Debug build 用 `@returnAddress` 记录每次借出点，`deinit` 报告「租了没还」的 buffer——GPA 泄漏检测只覆盖 malloc 路径，池内借出未还是它的盲区。Release build comptime 全裁掉。

### 3.7 Connection 合成与不可移动约束

`Connection(Raw)` 在 comptime 由 listener 的 raw 连接类型合成——静态分发无 vtable。它持有 `Raw.Reader` / `Raw.Writer` 状态值。

> **不可移动**：reader/writer 的 `std.Io` 接口通过 `@fieldParentPtr` 反查父结构，因此 `Connection` init 后不能移动。必须在连接协程里**原地构造**并传指针（见 `connection.zig` 文件头注释）。同理 `PipeReader`/`BodyReader`/`ChunkedBodyWriter` 都靠 `@fieldParentPtr("interface", ...)` 自指。

`waitReadable` 值得注意：transport 支持 setTimeout 时，用 1s 短超时 tick 轮询 + 中间检查停机标志（与 §5.6 心跳同节奏，M3 由心跳接管唤醒职责）；没有读超时的 transport（内存管道）退化为普通阻塞 `fill(1)`。没有它，空闲 keep-alive 连接只能等 drain-timeout 才死，停机会拖满整个 drain 窗口。

### 3.8 内存传输：Pipe + MemoryListener

`pipe.zig` 是单向字节环形缓冲，读写在空/满时挂起协程（zio Mutex + Condition）。`PipeReader`/`PipeWriter` 把它包成标准 `std.Io.Reader/Writer`，错误落在 `err` 字段、接口报 `ReadFailed`/`WriteFailed`（镜像 zio Stream.Reader 形状）。

`MemoryListener.connect()` 造一对 pipe，server 侧塞进 `zio.Channel` 给 `accept()`，返回 client 侧。连接内存（pipe 环）活到 `deinit()`，所以测试客户端可以比单条连接活得久。这让整条栈无 socket 可测——是一等公民，不是事后补的脚手架（Kestrel TestServer 教训）。

---

## 4. 开发工作流

### 构建与测试

```bash
zig build                # 编译库 + examples（验证 examples 也编得过）
zig build test           # 跑 talon test 二进制（core + http）+ core-boundary 编译守卫
zig build run-http       # 手动验证 HTTP
zig build run-resp       # 手动验证自定义协议路径
```

`build.zig` 建一个 `addTest`（`talon_mod`，整个 talon 模块 = core + http，覆盖全部单元 + 集成测试），外加一个 `addObject` 边界守卫（`core-boundary`，只编译不跑）。`test` step 依赖两者。core 测试只在 `talon_tests` 里跑一遍。新增源文件后，记得在对应入口文件（`core/core.zig` 或 `http/http.zig`）的 `test {}` 块里 `_ = @import("...")`，否则 `refAllDecls` 扫不到、测试不会被编译。

### 测试约定

- 单元测试与被测代码同文件，文件尾 `// ── Tests ──` 分隔。
- 集成测试在 `src/http/http.zig`：用 `MemoryListener`（或少数 `TcpListener`）+ `zio.Group` 双协程（server + client）跑真实请求循环，断言后 `s.shutdown()`，最后 `try std.testing.expect(!group.hasFailed())`。
- 涉及网络/协程的测试必须先 `zio.Runtime.init`。

### Fuzz

解析器是纯函数，专门留了两类 fuzz（`parser.zig` 尾部）：

- `std.testing.fuzz` 标准入口（`zig build test --fuzz`）。
- 一个**确定性的 in-process fuzz**（200k 变异输入），因为当前 0.16 工具链 `--fuzz` 模式下 test_runner 重建有问题，这条直接提供架构 §11「fuzz 无 crash」证据。

### 调试陷阱

- **不要用 `zio.debug_io` 重定向 std.log**：stderr 是普通文件时，日志写经 zio loop 会在 task 上下文外 panic（见两个 example 的注释）。example 用默认阻塞 stderr。
- **协程栈**：zio 默认 committed 256KB，10k 连接即 2.6GB。`Runtime.init` 时务必把 `stack_pool.committed_size` 调到 64KB（必要时 32KB）。这是最大隐性内存成本。

---

## 5. 扩展点：如何往里加东西

### 5.1 加一个自定义协议（Proto）

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

函数式最简单：

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

照 `framing.zig` 三件套的形状：构造在 `*std.Io.Reader` 上、`next()` 返回 `Error!?[]const u8`（`null` = 干净 EOF）、frame 是借用切片（下次 `next` 失效）、自带 `max_frame` 防御、用 `peek/take/toss/fillMore` 管缓冲。自定义状态机优先用 `Accumulator(Decoder)`，只写 `decode(window) !DecodeResult`。

### 5.4 加一个 listener

满足 comptime 契约（`listener.zig` 文件头 + `stream_server.zig` 的 `validateListener`）：

- `pub const RawConnection: type`
- `accept(self) !RawConnection`、`close(self) void`
- RawConnection 提供：`Reader`/`Writer` 类型（带 `interface: std.Io.Reader/Writer` 字段）、`reader(buf)`/`writer(buf)`、`close()`/`shutdown()`/`remoteInfo()`

校验靠 duck typing + `@compileError`，签名错会在编译期给人话提示。

---

## 6. 关键约束与权衡（开发时必须记住）

- **绑定 zio native API**：per-op `Timeout`、`Group`、shield 是 `std.Io` 没有或更弱的能力，talon 核心直接用（如同 Kestrel 绑 .NET）。牺牲可移植性，但对外 reader/writer 仍是标准 `std.Io` 接口。
- **comptime 重度使用**：中间件链、Connection 合成会放大编译错误的间接性。缓解纪律：每个 comptime 入口先做显式签名校验并以短 `@compileError` 给人话（`chain.zig`/`stream_server.zig`/`framing.zig` 都这么做）。新增 comptime 入口请延续这个习惯。
- **借用生命周期**：framing frame、HTTP header/target 都是借用切片，逃逸必 `dupe`。文档与注释里反复强调，是用户 use-after-free 的高发区。
- **停机正确性**：accept 用 task cancel 打断、空闲连接靠 `waitReadable` 打断、滞留连接靠 drain-timeout cancel——三条路径都有测试（`stream_server.zig` + `http/http.zig`）。改动连接循环时务必保住这三条。
- **架构按双消费者设计**：共享底座（chain、buffer pool、Limits、生命周期）从 M0 起就按 stream + datagram 双消费者审视（成本≈0），但 datagram 实现需求驱动（M3）。加底座能力时保持传输语义无关。

---

## 7. 路线图对照（架构 §11）

| 里程碑 | 状态 | 内容 |
|--------|------|------|
| M0 | ✅ | core 骨架：StreamServer + Tcp/Memory Listener + 限额 + 优雅停机 + chain；HTTP 借 std.http；RESP 示例 |
| M1 | ✅ | 自研 http1 解析器 + 零拷贝 + arena + buffer pool + 走私防御；连接中间件；framing 三组件；std.http 已完全替换 |
| M3 | ⏳ | 生产化：hijack 完善、数据速率防御 + 心跳、指标钩子、SO_REUSEPORT、多 size class；DatagramServer + SessionTable（需求驱动） |
| M4 | ⏳ | TLS 中间件、AutoProtocol/h2 评估、QUIC/HTTP3 预研 |

（M2 是 wing 侧里程碑，依赖 talon M1。）

> 注：架构文档把数据速率防御等列在 M3，把 `bench/` 与 datagram 占位文件列在目录树里；当前代码尚未创建这些文件，按本指南第 1 节的「状态」列为准。
</content>
