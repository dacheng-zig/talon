# talon 使用指南

> 面向使用 talon 的开发者：把 talon 当依赖用起来，写 HTTP 服务、HTTP/HTTPS 客户端或自定义 TCP 协议服务。
> 配套文档：架构与实现见 [开发者指南](developer-guide.md)，设计依据见 [架构设计](talon-architecture.md)。
> 适用版本：当前仓库实现，Zig `0.16.0`。

talon 是基于 [zio](https://github.com/lalinsky/zio) 协程运行时的网络服务引擎。只导出一个模块 `talon`，内部分两层：

- `talon.core`：协议无关的 stream 网络引擎 + 共享底座（listener、连接中间件、限额、优雅停机、buffer pool、拆帧工具箱）。
- `talon.http`：HTTP/1.1 服务端、客户端及共享编解码层，含 SSE 和 WebSocket。

你既可以用现成的 HTTP 服务器（`talon.http`），也可以只用 `talon.core` 写任意 TCP 协议服务（RPC、redis-like、消息网关）。

---

## 1. 前置条件

- Zig `0.16.0`（`build.zig.zon` 中 `minimum_zig_version = "0.16.0"`）。
- 依赖 zio 运行时。[build.zig.zon](../build.zig.zon) 使用 zio v0.18.0 版本归档，并校验包哈希；首次构建会自动下载，无需相邻的 zio 仓库。
- 开发 zio 本地改动时可用 `zig build --fork=../zio` 临时覆盖。去掉 `--fork` 即恢复固定依赖；提交 talon 改动前应使用固定依赖验证。

> talon 刻意绑定 zio 原生能力（per-op 超时、`Group` 结构化并发与取消机制），不能跑在其他 `std.Io` 运行时上。对外暴露的 reader/writer 仍是标准 `std.Io.Reader/Writer` 接口。

### 引入模块

talon 只导出一个模块：

| 模块名  | 来源            | 用途                                                  |
| ------- | --------------- | ----------------------------------------------------- |
| `talon` | `src/talon.zig` | 唯一入口；引擎在 `talon.core`，HTTP 层在 `talon.http` |

写自定义协议时用 `talon.core`（引擎），不需要单独依赖；HTTP 服务用 `talon.http`。

在你自己的 `build.zig` 里把对应模块加进去（与本仓库 `examples` 同样的接法）：

```zig
const talon_dep = b.dependency("talon", .{ .target = target, .optimize = optimize });
// 与 talon 使用同一个固定版本的 zio，避免重复引入不同运行时实例。
const zio_dep = talon_dep.builder.dependency("zio", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("talon", talon_dep.module("talon"));
exe.root_module.addImport("zio", zio_dep.module("zio"));
```

---

## 2. 最小 HTTP 服务

App 是一个普通结构体，唯一契约是 `handle` 方法：

```zig
pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void {}
```

最小例子（对应 [HTTP 示例](../examples/http.zig)，省略日志）：

```zig
const std = @import("std");
const zio = @import("zio");
const talon = @import("talon");

const App = struct {
    pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void {
        _ = self;
        _ = req;
        try res.respond("Hello from talon!\n", .{
            .extra_headers = &.{
                .{ .name = "content-type", .value = "text/plain; charset=utf-8" },
            },
        });
    }
};

fn signalWatcher(server: *talon.http.Server(App)) !void {
    var sig = try zio.Signal.init(.interrupt);
    defer sig.deinit();
    try sig.wait();
    server.shutdown(); // Ctrl+C 触发优雅停机
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{
        // 与仓库服务端示例一致；实际栈容量须按 handler 需求评估。
        .stack_pool = .{ .maximum_size = 8 * 1024 * 1024, .committed_size = 64 * 1024 },
    });
    defer rt.deinit();

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);
    var listener = try talon.TcpListener.listen(addr, .{});

    var app: App = .{};
    var server = try talon.http.Server(App).init(init.gpa, &app, .{});
    defer server.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(signalWatcher, .{&server});

    try server.serve(&listener); // 阻塞到 shutdown，然后 drain 退出
}
```

运行与验证：

```bash
zig build run-http
# 另开终端：
curl -v http://127.0.0.1:8080/
```

要点：

- **必须在 zio 运行时内运行**，示例将 `committed_size` 设为 64 KiB；调整前应验证应用的栈需求。
- `talon.http.Server(App)` 是 comptime 泛型，每个 App 生成一份专用服务器类型，零虚调用。
- 生命周期：`init(gpa, &app, options)` → `serve(&listener)` → 别处调 `shutdown()` → `serve` 返回 → `deinit()`。
- `serve` 会接管 listener 的关闭。

---

## 3. Request：读取请求

`*talon.http.Request` 提供：

```zig
req.method()           // talon.http.Method 枚举（GET/POST/.../other）
req.target()           // []const u8，原始请求目标（含 query）
req.header("name")     // ?[]const u8，大小写不敏感查找
req.bodyReader()       // *std.Io.Reader，流式 body
```

> **借用语义（重要）**：header、target 等切片借用「本次请求」的内存，生命周期到当前请求结束为止。要在请求之后还用，必须复制到生命周期覆盖实际使用范围的 allocator，例如应用持有的 allocator，并由接收数据的一方负责释放。`req.arena.dupe(...)` 只适用于本次请求内的副本：`req.arena` 在下个请求会 reset，复制到它不会延长生命周期。

### 读取 body

body 统一是 `std.Io.Reader`，自动处理 `Content-Length` 限读与 `chunked` 解码，**流式、不整体缓冲**：

```zig
pub fn handle(self: *EchoApp, req: *talon.http.Request, res: *talon.http.Response) !void {
    _ = self;
    var collected: std.Io.Writer.Allocating = .init(req.arena);
    _ = try req.bodyReader().streamRemaining(&collected.writer);
    try res.respond(collected.written(), .{});
}
```

- 需要复用连接时，引擎会 drain 未读完的 body；已决定关闭的连接直接发送响应并退出，不等待上传完成。
- body 帧错误（坏的 chunk、提前截断、超过 `max_body_size`）会终止连接。chunk 长度声明超出剩余纯载荷预算时，在读取或等待该 chunk 载荷前拒绝；handler 可通过 `bodyError()` 选择 413。
- 不支持 chunk extension 与 trailer（按策略拒绝，属走私防御）。

---

## 4. Response：写回响应

`*talon.http.Response` 支持定长、chunked 和 SSE 响应。

### 定长响应

```zig
try res.respond(body, .{
    .status = .ok,                       // 默认 .ok；用 std.http.Status
    .extra_headers = &.{
        .{ .name = "content-type", .value = "application/json" },
    },
    .keep_alive = true,                  // 设 false 则本次响应后关连接
});
```

- `respond` 将 head 和 body 写入连接缓冲；协议循环在等待读取前或关闭时 flush，缓冲满时也会输出。pipelining 可合并多个响应，不保证每个响应恰好一次 syscall。
- `date`、`content-length`、`connection`（需要时）由引擎自动补，不要手写。
- HEAD 请求会自动抑制 body，只写 head。
- 每个请求只能 `respond` 一次（重复调用会断言失败）。

### 流式 chunked 响应

body 大小未知或要边算边发时：

```zig
const buf = try req.arena.alloc(u8, 4096); // chunk 缓冲，从请求 arena 取
var cw = try res.startChunked(.{
    .extra_headers = &.{.{ .name = "content-type", .value = "text/plain" }},
}, buf);
try cw.interface.print("part {d}\n", .{1});
try cw.interface.writeAll("more data\n");
try cw.finish(); // 必须调用：发出收尾的 0 长度 chunk
```

---

## 5. Limits：限额与超时

HTTP 通过 `init` 的 options 传 `talon.http.Limits`（`talon.Limits` 是便捷别名，定义见 `src/http/server/limits.zig`）：

```zig
var server = try talon.http.Server(App).init(gpa, &app, .{
    .limits = .{
        .max_connections = 65536,
        .handshake_timeout = .fromSeconds(10),    // accept 后建立阶段总期限
        .max_header_size = 16 * 1024,
        .max_body_size = 16 * 1024 * 1024,   // null = 不限
        .header_read_timeout = .fromSeconds(10),  // 防头部慢发
        .keep_alive_timeout = .fromSeconds(75),   // keep-alive 空闲超时
        .drain_timeout = .fromSeconds(30),        // 停机排空上限
    },
});
```

行为：

- `max_connections`：用 `Semaphore` 在 accept 前背压，达到上限即停止接受新连接。
- `handshake_timeout`：accept 时确定绝对期限，覆盖连接中间件及可选 `Proto.handshake(conn, app)`；HTTP 首个请求头也使用剩余预算。阶段可以设置更短期限，不能延长总期限。正常会话解除建立期限；`.none` 禁用。启用时传输必须同时支持读写 `setTimeout`，否则 `serve` 返回 `HandshakeTimeoutUnsupported`。该机制约束连接 I/O 和阶段完成，不强制中断任意 CPU/外部等待。
- `max_header_size`：head 超过即回 `431` 并关连接。
- `read_buffer_size`：`null` 按头部上限加余量推导，默认 17 KiB；显式值必须严格大于 `max_header_size`，否则初始化返回 `HeaderBufferTooSmall`。`write_buffer_size` 默认 4 KiB，必须非零。
- 连接数及读写容量由初始化固定，启动前修改会返回 `ResourceLimitsChanged`；重新构造服务器才能调整。运行中不要修改配置。
- `max_body_size`：CL 在解析期预检（回 `413`），chunked 在读取期累计检查。
- `header_read_timeout` / `keep_alive_timeout`：走 zio 内核级超时。
- `min_body_data_rate` 已启用：通过累计网络等待预算和逐次读取期限执行，不依赖心跳巡检。完整语义见文末「资源限制与回归验证」。

---

## 6. 连接中间件

连接级中间件在协议开始说话「之前」运行，可改写远端身份、拒绝连接或在协议调用前后处理连接。静态传输包装使用 `serveAdapted`，中间件在包装后运行；可选服务端 TLS 使用此入口。用 `ServerWith` 传入一个 comptime 中间件元组：

```zig
const Srv = talon.http.ServerWith(App, .{
    talon.middleware.proxy_protocol, // 解析 PROXY v2 头，改写真实客户端地址
    talon.middleware.conn_log,       // 打印连接开/关与存活时长
});
var server = try Srv.init(gpa, &app, .{});
```

当前内置中间件（[middleware.zig](../src/core/middleware.zig)）：

| 中间件           | 作用                                                                              |
| ---------------- | --------------------------------------------------------------------------------- |
| `proxy_protocol` | 解析 HAProxy PROXY protocol v2 二进制前导，发布真实客户端地址；前导畸形即拒绝连接 |
| `conn_log`       | 记录连接打开/关闭与生命周期                                                       |

配置 `proxy_protocol` 后每条连接必须带 PROXY v2 前导，不能同时接收无前导的直连 HTTP。

中间件签名：`fn (conn: anytype, next: anytype) !void`。在 `try next.call(conn)` 之前的代码是 inbound、之后是 outbound；不调 `next` 即短路（拒绝连接）。可以自己写中间件传进同一个元组。

> **尚未提供**：动态 TLS 中间件、内置 IP 限流中间件。服务端可使用可选 OpenSSL adapter，或在 LB / 反向代理终止 TLS；出站 HTTPS 已由 `TlsClient` 支持。

`try server.serveAdapted(&listener, &adapter)` 支持包装后静态传输类型。adapter 声明 `Connection(comptime Raw) type` 和 `wrap(raw, gpa, accept_deadline) !Wrapped`；包装在受许可和停机追踪的任务内完成，沿用原始绝对建立期限。成功后 Wrapped 拥有原 raw；失败时 adapter 释放部分状态，引擎关闭原 raw。可选 `validateTransport` 在 accept 前检查包装阶段的底层能力。支持普通 value I/O 和固定地址的 `ioReader/ioWriter`，协议 flush 会刷新整个写栈。adapter 配置须保持到 serve 返回，详见 [静态传输组合](design/transport-lifecycle.md)。

可选 `talon_tls` 模块提供 TCP/TLS 1.2/1.3 adapter，支持默认身份和静态 SNI 多身份；启用依赖时传入 `.@"tls-server" = true`，再将 `dependency.module("talon_tls")` 加到应用 imports。需要 pkg-config 和系统 OpenSSL，默认构建不依赖它。配置 Context 后才能接收连接，配置期间不得并发修改；Context 必须活到全部连接结束。

```zig
const tls = @import("talon_tls");
var identity = try tls.Context.init("chain.pem", "key.pem");
defer identity.deinit();
var server = try talon.http.Server(App).init(gpa, &app, .{});
defer server.deinit(); // 比 identity.deinit 先执行
var adapter: tls.Adapter = .{ .context = &identity };
try server.serveAdapted(&tcp_listener, &adapter);
```

需要正常结束时发送本端 TLS alert，可设置 `adapter.close_notify_timeout = .fromMilliseconds(200)`；这是显式启用示例，默认 null。它只在链成功且未移交、未受迫取消时执行，保留更早的现有写期限，不等待 peer alert。协议和中间件返回前须 join 所有连接使用者。自定义协议仍可显式调用 `conn.raw.closeNotify()`，自行配置写期限与收敛并发任务。见[正常结束契约](design/transport-lifecycle.md)。

Context 支持可选 ALPN 和静态 SNI 多身份；当前没有 mTLS 或自动 ALPN 协议分派。OpenSSL 内部分配不计入 AllocationBudget；Transport.close 始终是无 I/O 释放。发送本端 alert 不证明完整双向 TLS 关闭或应用交付。设计和真实验收见[服务端 TLS](design/tls.md)。

---

## 7. 写自定义协议（只用 talon.core）

不写 HTTP，而是自定义 TCP 协议时，直接用 `talon.core` 的 `StreamServer`（`const core = @import("talon").core;`）。你只需实现一个 `Proto`：

```zig
pub fn serve(conn: anytype, app: *App) anyerror!void
```

`conn` 在协议视角提供：

```zig
conn.reader()                  // *std.Io.Reader，已穿过连接中间件链
conn.writer()                  // *std.Io.Writer
conn.isShuttingDown()          // 优雅停机已开始？请在请求边界退出
conn.waitReadable(budget)      // 可被停机打断的空闲等待（请求边界用）
conn.remoteInfo()              // 远端身份（可能被中间件改写）
try conn.setReadTimeout(t)     // 有限期限不支持时返回错误
try conn.detachRaw()           // 受检移交 raw 的关闭责任，不转移引擎资源
conn.limits                    // *const talon.core.Limits，仅引擎策略
conn.gpa                       // Allocator，协议自行管理分配与释放
```

### 拆帧工具箱

不要手写缓冲管理，用 `talon.core.framing`（`src/core/framing.zig`）：

```zig
// 1) 长度前缀：RPC / 私有二进制协议
var framed = talon.core.framing.LengthPrefixed(.{
    .length_type = u32, .endian = .big, .max_frame = 1 << 20,
}).init(conn.reader());
const frame: ?[]const u8 = try framed.next(); // 借用切片，下次 next 失效

// 2) 分隔符：行协议（RESP、SMTP、memcached text）
var lines = talon.core.framing.Delimited(.{ .delimiter = "\r\n", .max_frame = 64 * 1024 })
    .init(conn.reader());

// 3) 累积模板：自定义状态机兜底（ByteToMessageDecoder 等价物）
var dec = talon.core.framing.Accumulator(MyDecoder).init(conn.reader(), max_frame);
```

LengthPrefixed／Delimited 的 max_frame 计载荷；Accumulator 则按 Decoder 返回的 consumed 计完整编码帧，包括 framing 开销，排除后续 pipeline 字节。需要纯载荷限制时由自定义 Decoder 校验，详见[Accumulator 限额](design/framing.md)。

帧还必须放得进底层 reader 缓冲；配置 `max_frame` 不会自动扩容。自定义引擎使用 `talon.core.Limits`，默认读写各 4 KiB，可独立配置：

```zig
var server = try talon.core.StreamServer(Proto, App).init(gpa, &app, .{
    .limits = .{ .read_buffer_size = (1 << 20) + 4, .write_buffer_size = 4096 },
});
```

长度前缀的读容量至少容纳前缀本身，并按最大帧加前缀配置；分隔符协议需为完整帧和分隔符留空间。三者都构建在 reader 的 `peek/fill` 之上：frame 是借用切片，自带 `max_frame` 防御，超时由 reader 透传。`next()` 返回 `null` 表示干净 EOF；`error.PartialFrame` 表示流在帧中间断了；`error.FrameTooLarge` 表示超限。协议空闲策略不在 core.Limits 内，按自身语义传给 `waitReadable`。请求级 arena 由协议自行创建和释放，HTTP handler 仍用 `req.arena`。

大帧长度协议可使用独立拥有型入口，保留小连接窗口：

```zig
var frames = talon.core.framing.OwnedLengthPrefixed(.{
    .length_type = u32, .max_frame = 1 << 20,
}).init(conn.reader(), conn.gpa);
while (try frames.next()) |owned| {
    var frame = owned;
    defer frame.deinit();
    // frame.payload 可跨后续读取保存；这里在本帧处理结束时释放。
    try conn.writer().writeAll(frame.payload);
    try conn.writer().flush();
}
```

该入口在分配前校验长度，正文不必进入读窗口。`OwnedFrame` 是单一所有者，不可复制后重复释放；allocator 必须活到帧释放。`max_frame` 只限制一个正文，不限制多个保留帧或连接的总内存。任何错误都会释放未返回的正文，后续 `next` 返回 `FrameReaderFailed`；协议应结束连接，不能从半帧重试。设计依据见 [受限拥有型长度帧](design/framing.md)。

### 服务端运行快照

`StreamServer` 和 `http.Server` 提供 `server.stats()`，返回该 Server.Stats 类型的一致快照，可在 serve 运行期间从任务或线程读取；deinit 前必须结束读取。无需安装 exporter 或协议中间件。

```zig
const snapshot = server.stats();
std.log.info("accepted={d} active_tasks={d} finished_tasks={d} peak={d}", .{
    snapshot.accepted, snapshot.active_tasks, snapshot.finished_tasks,
    snapshot.peak_active_tasks,
});
```

accepted 只在 listener 返回 raw 后增加；active_tasks 包含待创建任务、建立、协议、关闭及缓冲归还，finished_tasks 在这些责任结束后增加，快照满足 accepted=finished_tasks+active_tasks。`max_connections` 满载时暂停 accept；TCP connect 成功不代表已经进入 accepted。detached_connections 记录任务结束时已移交的 raw，外部 raw 后续关闭不由该统计追踪。

waiting_for_permit 表示 accept loop 正在等待/取得许可，permit_wait_ns 仅在该调用结束时累计（包含取消和立即取得），不表示当前等待时长或等待客户端数。accept_fd_backoffs、accept_failures、task_spawn_failures、forced_drains 和 drain_timeouts 区分监听及停机路径；没有请求吞吐、RSS 或内核 backlog 指标。

transport_failures、buffer_failures 和 connection_errors 按引擎观察阶段计错误，后者包含正常 EOF/reset 等，不能等同于 HTTP 5xx 或服务端故障。canceled_returns 只认直接 Canceled，handshake_timeouts 只认明确 HandshakeTimeout；底层取消/超时可能被 Io.Reader 包装为 ReadFailed。设计与验证范围见 [服务端运行指标](design/transport-lifecycle.md)。

### 共享分配预算

需要累计内存上限时，将同一个预算 allocator 传给相关 server、client、listener、TLS 配置及应用：

```zig
var budget = talon.core.AllocationBudget.init(gpa, 64 * 1024 * 1024);
defer budget.deinit(); // 必须在所有分配及组件释放之后
const bounded = budget.allocator();
var server = try talon.core.StreamServer(Proto, App).init(bounded, &app, .{});
defer server.deinit(); // serve 返回后再释放
// conn.gpa、拥有型帧及该 server 的池分配共用额度。
const usage = budget.stats();
std.log.info("reserved={d}, peak={d}, denied={d}", .{
    usage.reserved_bytes, usage.peak_reserved_bytes, usage.denied_requests,
});
```

预算对象必须保持地址稳定，活到所有 allocator 句柄和分配释放；不得复制或修改上限。0 拒绝非零分配，额度不足使用已有 `OutOfMemory` 契约。`reserved_bytes` 包括实际分配请求长度和正在执行的增长预留；闲置池、arena、已保留 frame 和 TLS 状态仍计费，只有释放或成功缩小才归还。扩容回退可能暂时需要同时容纳新旧分配，因此上限应包含这一峰值。`denied_requests` 统计拒绝的 allocator 调用，一次逻辑分配可能触发多次；多个统计字段不是并发一致快照。

仅通过该 allocator 的分配被覆盖。它不计 allocator 内部余量、独立 mmap 栈、内核缓冲或其他 allocator 的内存，不是 RSS 硬上限；底层 allocator 仍需满足消费者的线程安全要求。设计和具体计量范围见 [共享分配预算](design/resource-ownership.md)。

额度被空闲缓存占用时，可显式调用 `server.trimIdleBuffers()`（返回释放的窗口 payload 字节数）及 `client.trimIdleConnections()`（返回关闭的空闲连接数）。前者保留借出窗口；后者释放空闲连接拥有的 arena、解码器与 transport 状态，保留活跃 Response/SSE/WS 和准入等待者。两者只回收调用时取得的快照，并发归还可形成新缓存；不会禁用后续保留，也不保证新分配成功。

应在尚未发送请求或消费帧的安全边界协调回收，再尝试新的操作；allocator 不自动调用回收，也不自动重试失败协议操作。已失败的拥有型帧 decoder、部分输出或不可重放请求不能由回收恢复。客户端回收可能在关闭 transport 时挂起，必须在 `deinit()` 前结束；服务端回收也不得与 `deinit()` 并发。活跃 keep-alive 的请求 arena 和 MemoryListener 拥有的管道不属于这些快照。见 [闲置资源回收](design/resource-ownership.md)。

### 通用出站连接

非 HTTP 协议可直接使用 core dialer，成功后由调用方关闭 raw 并提供 I/O 缓冲：

```zig
const dialer: talon.core.TcpDialer = .{};
const raw = try dialer.dial(.{ .host = "127.0.0.1", .port = 6380 }, .fromSeconds(5));
defer raw.close();
var read_buffer: [1024]u8 = undefined;
var reader = raw.reader(&read_buffer);
var writer = raw.writer(&.{});
reader.setTimeout(.fromSeconds(5));
writer.setTimeout(.fromSeconds(5));
try writer.interface.writeAll("PING\r\n");
const reply = try reader.interface.takeDelimiterInclusive('\n');
std.log.info("RESP reply: {s}", .{reply});
```

`MemoryDialer{ .listener = &listener }.dial({}, timeout)` 使用 void 目标，适合连接同一内存 listener。两者声明 `Target`、`RawConnection` 和 `dial`；TCP 目标是借用 host 与明确 port，没有 HTTP scheme 或默认端口。duration 转换为一次绝对期限，覆盖可取消解析、候选地址尝试或 memory backlog 等待；取消和超时不重试下一地址。成功后的读写期限需另行设置。平台 resolver 的不可取消系统调用仍可能延迟超时返回；更多边界见 [协议无关出站连接](design/transport-lifecycle.md)。

HTTP 的 `TcpConnector`、`MemoryConnector` 和 `TlsConnector` 保留现有构造与 `connect(origin, timeout)` API；前两者调用 core dialer，TLS 的 TCP 阶段也共用它。MemoryConnector 的有限 timeout 会约束满 backlog 等待，TLS TCP 与握手读写共用原绝对期限。

### 完整示例：RESP echo

`examples/resp.zig` 是一个只用 `talon.core` 的 redis-like 服务（用 `Delimited` 拆行）：

```bash
zig build run-resp
redis-cli -p 6380 ping     # +PONG
redis-cli -p 6380 echo hi  # +hi
```

它演示了完整的引擎契约：listener、连接限额、优雅停机、拆帧——零 HTTP 依赖。

---

## 8. 连接劫持（升级到自定义协议）

HTTP handler 使用 `req.upgrade.accept(.{ .protocol = "my-protocol" })` 发送并 flush 101，然后在 handler 内使用 `req.upgrade.reader` / `writer` 完成新协议循环。该低层 API 不校验应用协议的握手条件，调用方负责检查。成功升级后不要再用 `res` 输出 HTTP 响应；HTTP 循环在 handler 返回后退出并释放自己的 arena，引擎关闭连接、回收缓冲。不要把这些指针交给超过 handler 生命周期的任务。

WebSocket 已内置，见 [WebSocket 示例](../examples/ws.zig)：

```zig
var socket = try talon.http.ws.upgrade(req, .{});
while (try socket.read()) |msg| switch (msg) {
    .text => |data| try socket.writeText(data),
    .binary => |data| try socket.writeBinary(data),
};
```

默认最大消息为 64 KiB，读取超时为 `.none`；支持分片重组及自动回复 ping/close，消息切片借用内部缓冲，下一次读取前需要完成使用。`upgrade` 检查 GET、HTTP/1.1、Host、Upgrade/Connection token、唯一的版本 13 与能解码为 16 字节的 key；所选 subprotocol 必须是客户端提供的 token，区分大小写。消息缓冲在发送 101 前分配，非法握手或分配失败不会提交协议切换。handler 可捕获错误并返回合适的 HTTP 错误响应。Origin 许可、认证与授权由业务侧决定。

一个 `read()` 可与多个 Session 发送操作并行，自动 pong/close 与业务发送内部串行化；同时第二次 read 返回 `ConcurrentRead`。会话运行期间不可复制或移动，也不能绕过 Session 写其底层 Writer。发送等待锁时取消返回 `Canceled` 且不改变流；开始写帧后失败返回 `WriteFailed`，后续发送禁止继续，调用方须结束连接。成功 close 后业务发送及主动 ping 返回 `Closed`，read 仍可等待 peer close。退出 handler 或客户端 deinit 前必须结束并 join 所有会话任务；没有后台发送队列或自动任务注册。详细边界见 [WebSocket 写入所有权](design/websocket.md)。

`writeText` 拒绝非法 UTF-8（`InvalidUtf8`）；`writePing` 最多 125 字节，超限返回 `ControlPayloadTooLarge`。`close` 拒绝非法状态码（`InvalidCloseCode`），原因最多 123 字节（`CloseReasonTooLarge`）且必须是 UTF-8；原因不再自动截断。参数错误先于关闭、取消和写失败状态检查，不产生输出，可修正参数后继续使用健康会话。详见 [出站参数边界](design/websocket.md)。

多生产者可以显式限制未完成业务发送的数量和 payload 总字节：

```zig
var socket = try talon.http.ws.upgrade(req, .{
    .send_limits = .{ .max_data_sends = 8, .max_data_bytes = 1024 * 1024 },
});
```

客户端 `client.webSocket` 的参数也接受同一个 `.send_limits`。两个上限默认 `null`（不限制）；数量 0 拒绝所有业务发送，字节 0 允许空 payload。上述容量只是配置示例，应按工作负载规划。`writeText/writeBinary` 在进入写锁前同时预留两种额度，包括正在输出和等待锁的调用；超限返回 `SendLimitExceeded`，本次没有输出，不改变会话状态。应用决定有限重试、丢弃或结束会话，库不自动排队或重放。

额度到方法返回才归还，覆盖完整帧及 flush；等待取消、Closed 拒绝和帧中 WriteFailed 都归还，但 WriteFailed 仍要求结束连接。参数错误先执行，然后检查取消和准入，最后在写锁内检查 Closed/WriteFailed，因此容量错误可能先于这些写入状态错误。payload 借用到调用返回，不能提前释放或并发修改；同一片内存被多个调用借用时分别收费。

`socket.sendStats()` 返回在途/峰值数量和字节及拒绝调用数的一致快照（`in_flight_data_sends`、`in_flight_data_bytes`、`peak_data_sends`、`peak_data_bytes`、`rejected_data_sends`）。它不计帧头、Writer 缓冲、应用分配或任务栈，不是完整内存预算。自动 pong、主动 ping 和 close 不占业务额度，仍等待完整帧写锁；没有控制优先级或控制任务上限。调用前已创建的任务/分配仍由应用约束，handler 返回/deinit 前须 join；操作期间不可修改 send_limits。完整取舍见 [数据发送准入](design/websocket.md)。

较大消息可以显式启用数据分片，让控制帧在完整分片之间获得发送机会：

```zig
var socket = try talon.http.ws.upgrade(req, .{
    .send_options = .{ .max_frame_payload = 16 * 1024 },
});
```

客户端 `client.webSocket` 接受相同 `.send_options`。`max_frame_payload` 默认 null（一条消息一个帧），正数限制每个数据帧的 payload，零返回 `InvalidFrameSize`，高层在升级/拨号前拒绝。此大小只是配置示例，不是推荐容量或时间上限；控制帧不分片，消息接收大小仍由 max_message_size 控制。分片之间释放帧锁并 yield，不同业务消息不会交错，整个 payload 的准入和借用持续至调用返回。

首帧前取消不输出，健康会话可继续使用；非最终帧已发送后，在帧间取消返回 Canceled，并终止后续写入（WriteFailed），不能重试消息。此清理可能等待正在发送的控制帧，额度不会提前归还。close 在完整分片之间成功后，剩余发送返回 Closed，不继续输出 continuation；read 仍可回答 ping 和等待对端 close。分片不提供优先级或墙钟延迟保证，内核排队、慢读者和调度仍会影响控制等待；操作期间也不可修改 send_options。设计及行为对照见 [出站分片](design/websocket.md)。

自定义 core 协议若只在当前任务内切换协议，继续使用 `conn.reader()` / `conn.writer()`，无需移交 raw。确实需要接管 raw 的关闭责任时：

```zig
const raw = try conn.detachRaw();
// raw 由调用方负责 close；引擎不再 shutdown/close 它。
```

`detachRaw()` 拒绝建立阶段、重复移交及未消费的预读输入；先 flush 待写输出，成功后才移交 raw 的关闭责任。失败时引擎仍负责关闭。成功后不要再使用原 Connection 的 I/O 接口，调用方负责 raw.close()、新缓冲和期限设置。它不转移池缓冲、arena、连接许可或协程生命周期；独立后台任务也不会自动受服务端停机追踪。旧 `hijack()` 保留相同的受检行为，调用方须添加 `try`，推荐迁移到 `detachRaw()`。HTTP `Request` 没有 `hijack()` 方法。

---

## 9. 测试：MemoryListener（无 socket）

`MemoryListener` 是一等公民，让你不开真实端口就能端到端测服务器：

```zig
var listener = try talon.MemoryListener.init(gpa, .{});
defer listener.deinit();

var server = try talon.http.Server(App).init(gpa, &app, .{});
defer server.deinit();

// 一个协程跑 server.serve(&listener)，另一个 listener.connect() 当客户端，
// 两端都是标准 std.Io.Reader/Writer。
const conn = try listener.connect();
defer conn.close();
```

可直接参考 [HTTP 服务端集成测试](../tests/http_server_test.zig) 和 [停机测试](../tests/stream_server_test.zig)（keep-alive、POST body、走私拒绝、停机、proxy_protocol）。

---

## 10. 已实现 / 暂未提供 一览

便于你判断当前能不能用上某能力。

**已实现**

- HTTP/1.1 子集：keep-alive、pipelining、定长 + chunked 响应、CL/chunked body 流式读、HEAD、`Expect: 100-continue`、严格 framing/字段字符校验与请求走私防御；不代表完整 RFC 合规。
- `talon.core`：`StreamServer`、`TcpListener`、`MemoryListener`、连接限额、优雅停机、`chain` 中间件、借用及拥有型 `framing`、buffer pool（含 Debug 借出泄漏追踪）。
- 连接中间件：`proxy_protocol`、`conn_log`。
- core 受检 raw 移交 `conn.detachRaw()`、HTTP `req.upgrade.accept()`、服务端 SSE / WebSocket。
- HTTP/HTTPS 客户端、连接池、重定向、重试、gzip/deflate/zstd 解压、可选 Cookie 中间件、SSE 重连与 WebSocket 客户端。
- 慢速 body 防御、写入无进展超时和请求 arena 保留上限。

**暂未提供**

- 动态服务端 TLS 中间件；静态 OpenSSL adapter 与客户端 TLS 已实现。
- UDP / DatagramServer。
- Unix domain socket listener。
- 内置 IP 限流中间件、`SO_REUSEPORT`、buffer pool 多 size class、`sendFile` 零拷贝、HTTP/2 / AutoProtocol。

已实现能力与生产验证是不同状态，详见 [项目现状与生产级差距](project-status.md)。

---

## 11. 常用命令

```bash
zig build               # 编译并安装 examples 及其引用的库代码
zig build test          # 跑全部单元/集成测试
zig build run-http      # 跑 HTTP 示例（127.0.0.1:8080）
zig build run-resp      # 跑 RESP 示例（127.0.0.1:6380）
```


## 12. 资源限制与回归验证

HTTP 服务默认启用以下限制，可通过 `Server.init` 的 `limits` 配置：

- `header_read_timeout`：整个请求头的期限。新连接同时受 accept 时刻的建立绝对期限约束；复用连接先按 `keep_alive_timeout` 等待首字节，再开始请求头期限。碎片数据不会重置期限。
- `min_body_data_rate`：正文最低平均速率，默认 240 bytes/s，初始宽限 5s；只累计等待上游数据的时间，handler 计算、主动等待及输出背压不计入。已读取的正文提供后续等待额度，chunk 元数据不提供正文额度。`null` 禁用；速率和宽限期都必须大于零，否则初始化返回 `error.InvalidDataRate`。超限可从 `req.bodyError()` 返回的可选错误中识别 `error.BodyTooSlow`；未写响应且 handler 未处理该错误时返回 408。
- `write_timeout`：响应写入无进展的等待期限，默认 30s；duration 在取得写入进展后重新计时，绝对 deadline 不会重置；适用于 HTTP、SSE 和 WebSocket 的底层写入。空闲 SSE 不执行写入，因此不会因没有事件而超时。`.none` 禁用。
- `max_retained_arena`：每次请求结束后保留的 arena 容量上限，默认 64 KiB。它限制请求之间的保留量，不限制 handler 的峰值分配；`null` 保留高水位，`0` 全部释放。超出保留上限的工作负载可能在后续请求重新分配。

TCP 和内存管道都支持读写超时。`Connection.timeout_capabilities` 描述各方向能力，有限期限 setter 返回 `UnsupportedReadTimeout` / `UnsupportedWriteTimeout`，调用需加 `try`；`.none` 显式禁用。HTTP 服务在 accept 前检查策略需要的能力。无期限自定义传输须明确禁用 `handshake_timeout`、`header_read_timeout`、`keep_alive_timeout`、`write_timeout` 并设 `min_body_data_rate = null`，否则启动报错；启用预算时不能以自行添加外部计时器代替所声明的 transport setter。

客户端按最终请求的 read/write/total 配置在 checkout 前检查能力，total 要求双向期限；不支持时尚未 dial 或发送请求。`connect` 仍由 Connector 契约执行。启用池 `validate_on_checkout` 还要求读期限能力。Upgrade 的有限读期限同样在写出 101 前检查；自定义 timeout thunk 需返回 `talon.core.TimeoutError!void`。

HEAD 的固定长度、chunked 和 SSE 响应只输出响应头。204、205、304 不输出正文；1xx 不能作为 `respond` / `startChunked` 的最终状态，升级应使用专门的 upgrade API。

客户端池的每 origin 并发状态在有等待者、持有者或空闲连接时保留；最后一个使用者退出且没有空闲连接后回收。调用 `Client.deinit()` 前必须结束所有请求、Response、WebSocket 和后台回收任务。

测试入口为 `zig build test`。独立 TCP/TLS 负载见 [bench 方法](benchmarks/README.md)；内存传输微基准见 [http_client_bench.zig](../examples/http_client_bench.zig)，不能据此宣称服务端性能已达标。

## 13. SSE 服务端

在 handler 内打开事件流，保持 `stream` 地址稳定，并在有限流正常结束时调用 `finish()`，以便 HTTP 连接继续复用：

```zig
const buffer = try req.arena.alloc(u8, 4096);
var stream = try res.startEventStream(buffer);
try stream.send(.{ .event = "update", .id = "1", .data = "ready" });
try stream.comment("heartbeat");
try stream.finish();
```

默认响应头为 `text/event-stream`、`cache-control: no-cache` 和 `x-accel-buffering: no`。`send` / `comment` 每次都会 flush；心跳由应用主动发送，没有后台定时器。写失败后应退出发送循环。持续流示例见 [sse.zig](../examples/sse.zig)。

## 14. HTTP/HTTPS 客户端

客户端 API 位于 `talon.http.client`，与服务端使用相同 zio 运行时。下面片段应在 zio task 内执行，完整入口见 [http_get.zig](../examples/http_get.zig)：

```zig
var client = talon.http.client.TcpClient.init(gpa, .{}, .{});
defer client.deinit();
var response = try client.getUrl("http://127.0.0.1:8080/");
defer response.deinit();
const body = try response.readAllAlloc(gpa, 1024 * 1024);
defer gpa.free(body);
```

`Response` 持有池连接，header/reason 切片在 `deinit()` 前有效。`deinit()` 尝试排空未读正文，只有可复用且正文边界完整的连接才归还池，否则关闭；排空可能等待网络。必须先释放所有 Response、SSE source、WebSocket 并结束回收任务，再释放 Client。

- `request` 接收 `origin` 和 `target`；`requestUrl` / `getUrl` / `postUrl` 接收完整 URL。请求 body 支持 `.bytes`、定长 `.reader` 和 `.chunked` 流式上传。
- 默认 connect/read/write/total 超时分别为 10/30/30/60 秒；请求可通过 `timeouts` 覆盖。total 通过底层超时限制阶段操作及正文读取，并非能抢占任意应用代码的计时器；自定义 transport 必须支持超时才能执行这些限制。
- 默认最多跟随 10 次重定向，跨 origin 默认移除 Authorization/Cookie；复用连接失败时符合条件的幂等请求最多重试一次，流式 body 不自动重放。
- 303 保持 HEAD，其余改用 GET；301/302 只改写 POST。丢弃正文时移除 Content-Type/Encoding/Language/Location，后续跳转不恢复这些字段；307/308 保留可重放的 method/body。Location 仅支持基础相对引用，并非完整 URI 解析器。
- 默认解码 gzip、deflate、zstd。`max_response_body` 默认 16 MiB，限制编码后的正文；`readAllAlloc` / `json` 在读取期间限制解码后正文，收集缓冲容量不超过 `max_decoded`，超限返回 `ResponseTooLarge`。达到上限后会探测一个额外字节，以识别恰好到达上限的正文。读取或分配失败后，`response.deinit()` 直接关闭连接，不再 drain 或复用。固定解码工作区（flate 64 KiB，zstd 约 8 MiB）、4 KiB 读取暂存、扩容时旧新缓冲的短暂重叠及 JSON 解析对象不包含在该限额内。`json` 返回值另需 `deinit()`。
- HEAD 和 304 的合法 `Content-Length` 是资源大小元数据，不占 `max_response_body`；即使正文预算为零也可接收。它们仍受响应头预算与语法限制，不读取正文或创建解压器，详见[无正文响应预算](design/http-client.md)。
- `pool.max_decoder_bytes` 默认 64 MiB，限制全 Client 的解码器对象与窗口预留，跨活跃和空闲连接共享；不足返回 `DecoderBudgetExceeded` 并关闭该响应连接。`null` 禁用，0 拒绝透明解码。`max_idle_decoder_bytes` 默认 1 MiB，限制其中的空闲缓存；超限释放解码器但保留健康 TCP 连接，0 禁用空闲解码缓存。两项在 init 固定；`poolStats()` 的 `decoder_bytes` / `idle_decoder_bytes` 提供计量。预算不包含 TLS、arena、正文或分配器开销，详见 [解码工作区预算](design/http-client.md)。
- `pool.max_retained_response_arena` 默认 4 KiB，限制响应释放后每条空闲连接保留的头部 arena 容量；0 全部释放，`null` 保留高水位。收缩失败释放全部存储，健康连接仍可复用。`poolStats().idle_response_arena_bytes` 汇总空闲 arena 可用容量，不含分配器元数据或活跃响应。连续 1xx 在 Reader 缓冲中验证并丢弃，只固定最终头；详见 [响应头存储边界](design/http-client.md)。
- `max_response_head_bytes` 默认 64 KiB，累计一个逻辑请求的全部 1xx、最终头和自动重定向返回头，包含状态行及 CRLF；换连接或重试不重置。超限返回 `ResponseHeadersTooLarge`，当前失败连接关闭且不重试。0 在 checkout 前拒绝；`null` 禁用累计限制，单头 15 KiB 上限仍保留。正文与 total 期限保持独立，详见 [累计响应头交换预算](design/http-client.md)。
- 连接池默认每 origin 最多保留 8 条空闲连接、全局 256 条，连接最长存活 5 分钟、空闲期限 90 秒。过期回收在取连接时执行，也可调用 `reapIdle` 或自行托管 `reapLoop`；`trimIdleConnections` 不看年龄，显式释放当前空闲快照。
- 普通请求设置 `keep_alive=false` 后，响应释放必定关闭连接，即使对端省略 `Connection: close`；健康正文和响应的 keep-alive 意图仍是复用的其他条件。101 或 CONNECT 2xx 后普通 `response.deinit()` 关闭连接，专用 WebSocket 会话继续持有新协议连接；失败 CONNECT 或未接受的升级仍按 HTTP 规则复用，详见[协议切换归属](design/http-client.md)。
- `max_active` 默认 256，覆盖全 Client 的 dial、Response 正文、SSE 和 WebSocket 借用；`max_per_origin` 默认 `null`，可独立限制每 origin。`max_waiters` 默认 256，满队列返回 `PoolQueueFull`；设置 0 在满载时立即拒绝。三个上限均可用 `null` 明确禁用，活跃上限 0 返回 `InvalidPoolLimit`。`pool_wait` 默认 10 秒，并受请求 total 的剩余期限约束，超时返回 `PoolWaitTimeout`。零等待只允许立即获取空闲容量。
- `poolStats().active/waiting` 提供准入计数，原有 created/reused/evicted 保留。正文或会话未释放时继续占 active；空闲连接单独计入 idle 上限。这些数量上限不能代替全连接字节预算。默认并发行为有变化，需要更多容量时显式调整配置；不要在使用中移动 Client 或并发修改配置。设计依据见 [客户端准入预算](design/http-client.md)。
- `ClientWith(Connector, middlewares)` 提供请求中间件组合；Cookie 需显式配置调用方持有的 `CookieJar` 并加入 `cookies` 中间件，不是默认自动启用。
- CookieJar 仅按 host 存储，处理 Secure/过期，但忽略 Domain/Path/SameSite、没有条目总量预算，只保存最终响应的 Cookie；复杂登录或不可信多 host 场景应使用符合需求的 Cookie 实现和资源限额。

HTTPS 使用 `TlsClient`，它也支持明文 HTTP；`TcpClient` 只支持明文。参照 [https_get.zig](../examples/https_get.zig) 加载 `RootStore`，将 `.verification = .{ .system = &store }` 交给 connector，store 必须活到所有连接结束。客户端 TLS 使用固定 Zig std 兼容源码；服务端可选 OpenSSL adapter，动态 TLS 中间件仍未提供。

DNS 目标按主机名匹配证书并发送 SNI；IP literal 只匹配证书的二进制 iPAddress SAN，不发送 SNI，不把数字 DNS SAN 或 CN 当作 IP 身份。两者仍执行所选 CA 策略；IP 不匹配返回 `CertificateHostMismatch`，不会自动降级验证。IPv6 URL 用方括号，如 `https://[::1]:8443/`；低层 Origin.host 使用裸地址，HTTP Host 由客户端补回方括号。分类沿用 core 的 literal parser，带 IPv4 十进制尾段的 IPv6 文本及 scope/zone 当前不支持；设计、验收与工具链边界见 [TLS IP 身份](design/tls.md)。

`client.sseSource(.{ .origin = origin, .target = "/events" })` 返回需 `deinit()` 的事件源；`next()` 支持 Last-Event-ID 和 retry 延迟，默认持续重连，204 结束。事件借用内部缓冲，有效期到下次 `next()`；流禁用 read/total 超时，存活检测依赖服务端心跳。

`client.webSocket(.{ .origin = origin, .target = "/ws" })` 返回需 `deinit()` 的 WebSocket；默认消息上限 64 KiB、读取超时 `.none`。TLS connector 支持安全连接。完整用法见 [ws_client.zig](../examples/ws_client.zig)。

```bash
zig build run-http_get -- http://127.0.0.1:8080/
zig build run-https_get -- https://example.com/
zig build run-sse
zig build run-ws
zig build run-ws_client
```

服务端示例共用 8080 端口，应分别运行。客户端示例依赖目标服务可达。
