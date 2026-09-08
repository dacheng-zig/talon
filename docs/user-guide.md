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
- 依赖 zio 运行时。`build.zig.zon` 已固定到提交 `34510ecd0e41192eb4d379a047226269c4a1a56f`（zio 0.17.0），并校验包哈希；首次构建会自动下载，无需相邻的 zio 仓库。
- 开发 zio 本地改动时可用 `zig build --fork=../zio` 临时覆盖。去掉 `--fork` 即恢复固定依赖；提交 talon 改动前应使用固定依赖验证。

> talon 刻意绑定 zio 原生能力（per-op 超时、`Group` 结构化并发与取消机制），不能跑在其他 `std.Io` 运行时上。对外暴露的 reader/writer 仍是标准 `std.Io.Reader/Writer` 接口。

### 引入模块

talon 只导出一个模块：

| 模块名   | 来源            | 用途                                                  |
| -------- | --------------- | ----------------------------------------------------- |
| `talon`  | `src/talon.zig` | 唯一入口；引擎在 `talon.core`，HTTP 层在 `talon.http` |

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
- body 帧错误（坏的 chunk、提前截断、超过 `max_body_size`）会终止连接。
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

通过 `init` 的 options 传 `Limits`（定义见 `src/core/limits.zig`）：

```zig
var server = try talon.http.Server(App).init(gpa, &app, .{
    .limits = .{
        .max_connections = 65536,
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
- `max_header_size`：head 超过即回 `431` 并关连接。
- `max_body_size`：CL 在解析期预检（回 `413`），chunked 在读取期累计检查。
- `header_read_timeout` / `keep_alive_timeout`：走 zio 内核级超时。
- `min_body_data_rate` 已启用：通过累计网络等待预算和逐次读取期限执行，不依赖心跳巡检。完整语义见文末「资源限制与回归验证」。

---

## 6. 连接中间件

连接级中间件在协议开始说话「之前」运行，可改写远端身份、拒绝连接或在协议调用前后处理连接；通用流替换与服务端 TLS 仍属规划。用 `ServerWith` 传入一个 comptime 中间件元组：

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

> **尚未提供**：TLS 中间件（规划 M4）、内置 IP 限流中间件。服务端需要 TLS 时可在 LB / 反向代理终止 TLS；出站 HTTPS 已由 `TlsClient` 支持。

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
conn.setReadTimeout(t)         // per-read 内核超时
conn.hijack()                  // 劫持原语：交还 raw 连接，引擎不再关它
conn.limits                    // *const Limits
conn.arena                     // *ArenaAllocator，请求级分配
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

帧还必须放得进底层 reader 缓冲；配置 `max_frame` 不会自动扩容。使用服务端连接 reader 时，其容量为 `max(max_header_size, 1024) + 1024`，默认 17 KiB。三者都构建在 reader 的 `peek/fill` 之上：零拷贝（frame 是借用切片）、自带 `max_frame` 防御、超时由 reader 透传。`next()` 返回 `null` 表示干净 EOF；`error.PartialFrame` 表示流在帧中间断了；`error.FrameTooLarge` 表示超限。

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

HTTP handler 使用 `req.upgrade.accept(.{ .protocol = "my-protocol" })` 发送并 flush 101，然后在 handler 内使用 `req.upgrade.reader` / `writer` 完成新协议循环。该低层 API 不校验应用协议的握手条件，调用方负责检查。成功升级后不要再用 `res` 输出 HTTP 响应；HTTP 循环在 handler 返回后退出，引擎关闭连接并回收缓冲与 arena。不要把这些指针交给超过 handler 生命周期的任务。

WebSocket 已内置，见 [WebSocket 示例](../examples/ws.zig)：

```zig
var socket = try talon.http.ws.upgrade(req, .{});
while (try socket.read()) |msg| switch (msg) {
    .text => |data| try socket.writeText(data),
    .binary => |data| try socket.writeBinary(data),
};
```

默认最大消息为 64 KiB，读取超时为 `.none`；支持分片重组及自动回复 ping/close，消息切片借用内部缓冲，下一次读取前需要完成使用。当前 helper 检查 Upgrade token、版本与 key 是否存在，但不完整校验握手（例如 GET、Connection token 和 key 格式）；业务侧还须校验所需条件，所选 subprotocol 必须由客户端提供。不要将其描述为完整握手验证器。

自定义 core 协议另有底层劫持原语：

```zig
const raw = conn.hijack();
// raw 由调用方负责 close；引擎不再 shutdown/close 它。
```

`conn.hijack()` 只转移 raw 的关闭责任，不转移引擎的读写缓冲、arena 或协程生命周期。若连接需在 `Proto.serve` 返回后继续使用，调用方须自行管理缓冲，并处理旧 reader 中已预读的数据。HTTP `Request` 没有 `hijack()` 方法。

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

- HTTP/1.1：keep-alive、pipelining、定长 + chunked 响应、CL/chunked body 流式读、HEAD、`Expect: 100-continue`、严格 RFC 9112 解析与请求走私防御。
- `talon.core`：`StreamServer`、`TcpListener`、`MemoryListener`、连接限额、优雅停机、`chain` 中间件、`framing`（三组件）、buffer pool（含 Debug 借出泄漏追踪）。
- 连接中间件：`proxy_protocol`、`conn_log`。
- core 劫持原语 `conn.hijack()`、HTTP `req.upgrade.accept()`、服务端 SSE / WebSocket。
- HTTP/HTTPS 客户端、连接池、重定向、重试、gzip/deflate/zstd 解压、可选 Cookie 中间件、SSE 重连与 WebSocket 客户端。
- 慢速 body 防御、写入无进展超时和请求 arena 保留上限。

**暂未提供**

- 服务端 TLS 中间件（规划 M4）；客户端 TLS 已实现。
- UDP / DatagramServer（规划 M3，需求驱动）。
- Unix domain socket listener（架构预留，代码未落地）。
- 内置 IP 限流中间件、`SO_REUSEPORT`、buffer pool 多 size class、`sendFile` 零拷贝、HTTP/2 / AutoProtocol（M3/M4）。
- 类型化 feature 查询（`conn.has/get`，架构 §6）尚未在 `Connection` 上落地；当前只有 `remoteInfo` 覆盖机制。

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

- `header_read_timeout`：整个请求头的期限。新连接从首次等待读取开始计时；复用连接先按 `keep_alive_timeout` 等待首字节，再开始请求头期限。碎片数据不会重置期限。
- `min_body_data_rate`：正文最低平均速率，默认 240 bytes/s，初始宽限 5s；只累计等待上游数据的时间，handler 计算、主动等待及输出背压不计入。已读取的正文提供后续等待额度，chunk 元数据不提供正文额度。`null` 禁用；速率和宽限期都必须大于零，否则初始化返回 `error.InvalidDataRate`。超限可从 `req.bodyError()` 返回的可选错误中识别 `error.BodyTooSlow`；未写响应且 handler 未处理该错误时返回 408。
- `write_timeout`：响应写入无进展的等待期限，默认 30s；duration 在取得写入进展后重新计时，绝对 deadline 不会重置；适用于 HTTP、SSE 和 WebSocket 的底层写入。空闲 SSE 不执行写入，因此不会因没有事件而超时。`.none` 禁用。
- `max_retained_arena`：每次请求结束后保留的 arena 容量上限，默认 64 KiB。它限制请求之间的保留量，不限制 handler 的峰值分配；`null` 保留高水位，`0` 全部释放。超出保留上限的工作负载可能在后续请求重新分配。

TCP 和内存管道都支持读写超时。自定义传输若没有 `setTimeout` 方法，必须自行提供等价的等待限制；引擎无法替它中断阻塞读取。

HEAD 的固定长度、chunked 和 SSE 响应只输出响应头。204、205、304 不输出正文；1xx 不能作为 `respond` / `startChunked` 的最终状态，升级应使用专门的 upgrade API。

客户端池的每 origin 并发状态在有等待者、持有者或空闲连接时保留；最后一个使用者退出且没有空闲连接后回收。调用 `Client.deinit()` 前必须结束所有请求、Response、WebSocket 和后台回收任务。

测试入口为 `zig build test`。仓库没有 `performance.md` 或历史服务端 TCP 基准工具；现有客户端基准示例见 [http_client_bench.zig](../examples/http_client_bench.zig)，不能据此宣称服务端性能已达标。

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
- 默认解码 gzip、deflate、zstd。`max_response_body` 默认 16 MiB，限制编码后的正文；`readAllAlloc` / `json` 的 `max_decoded` 在完整读取后才检查，**不限制读取期间的峰值内存**。需要严格内存边界时，应自行实现有界流式消费。`json` 返回值另需 `deinit()`。
- 连接池默认每 origin 最多保留 8 条空闲连接、全局 256 条，连接最长存活 5 分钟、空闲期限 90 秒。`max_per_origin` 默认 `null`（不限制并发），启用后 `pool_wait` 默认 10 秒。过期回收在取连接时执行，也可调用 `reapIdle` 或自行托管 `reapLoop`。
- `ClientWith(Connector, middlewares)` 提供请求中间件组合；Cookie 需显式配置调用方持有的 `CookieJar` 并加入 `cookies` 中间件，不是默认自动启用。

HTTPS 使用 `TlsClient`，它也支持明文 HTTP；`TcpClient` 只支持明文。参照 [https_get.zig](../examples/https_get.zig) 加载 `RootStore`，将 `.verification = .{ .system = &store }` 交给 connector，store 必须活到所有连接结束。TLS 基于 `std.crypto.tls.Client`，不代表服务端 TLS 中间件已实现。

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
