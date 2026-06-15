# talon 使用指南

> 面向使用 talon 的开发者：把 talon 当依赖用起来，写 HTTP 服务或自定义 TCP 协议服务。
> 配套文档：架构与实现见 `developer-guide.md`，设计依据见 `talon-architecture.md`。
> 适用版本：当前仓库状态（M0 + M1 已落地），Zig `0.16.0`。

talon 是基于 [zio](https://github.com/lalinsky/zio) 协程运行时的网络服务引擎。只导出一个模块 `talon`，内部分两层：

- `talon.core`：协议无关的 stream 网络引擎 + 共享底座（listener、连接中间件、限额、优雅停机、buffer pool、拆帧工具箱）。
- `talon.http`：在引擎之上的 HTTP/1.1 协议层。

你既可以用现成的 HTTP 服务器（`talon.http`），也可以只用 `talon.core` 写任意 TCP 协议服务（RPC、redis-like、消息网关）。

---

## 1. 前置条件

- Zig `0.16.0`（`build.zig.zon` 中 `minimum_zig_version = "0.16.0"`）。
- 依赖 zio 运行时。当前 `build.zig.zon` 用本地路径依赖：

```zig
.dependencies = .{
    .zio = .{ .path = "../zio" },
},
```

即默认假设 `talon` 与 `zio` 仓库相邻。若改用包管理拉取，替换为对应的 `url` + `hash`。

> talon 刻意绑定 zio 原生能力（per-op 超时、`Group` 结构化并发、shield），不能跑在其他 `std.Io` 运行时上。对外暴露的 reader/writer 仍是标准 `std.Io.Reader/Writer` 接口。

### 引入模块

talon 只导出一个模块：

| 模块名 | 来源 | 用途 |
|--------|------|------|
| `talon` | `src/talon.zig` | 唯一入口；引擎在 `talon.core`，HTTP 层在 `talon.http` |

写自定义协议时用 `talon.core`（引擎），不需要单独依赖；HTTP 服务用 `talon.http`。

在你自己的 `build.zig` 里把对应模块加进去（与本仓库 `examples` 同样的接法）：

```zig
const talon_dep = b.dependency("talon", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("talon", talon_dep.module("talon"));
exe.root_module.addImport("zio", zio_dep.module("zio"));
```

---

## 2. 最小 HTTP 服务

App 是一个普通结构体，唯一契约是 `handle` 方法：

```zig
pub fn handle(self: *App, req: *talon.http.Request, res: *talon.http.Response) !void {}
```

完整例子（即 `examples/http.zig`）：

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
        // 关键：zio 默认 committed 栈 256KB，1 万连接就是 2.6GB。
        // 64KB 是 talon 的工作点。
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

- **必须在 zio 运行时内运行**，并自行设置 `committed_size`（64KB 是推荐值，内存吃紧可降到 32KB）。
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

> **借用语义（重要）**：header、target 等切片借用「本次请求」的内存，生命周期到当前请求结束为止。要在请求之后还用，必须 `req.arena.dupe(...)` 拷出来。`req.arena` 是每请求 arena，下个请求会 reset。

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

- 没读完的 body 由引擎自动 drain，连接可继续复用（keep-alive）。
- body 帧错误（坏的 chunk、提前截断、超过 `max_body_size`）会终止连接。
- 不支持 chunk extension 与 trailer（按策略拒绝，属走私防御）。

---

## 4. Response：写回响应

`*talon.http.Response` 两种写法。

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

- head + body 一次 flush，由 zio writer 合并成单次 vectored syscall。
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
- `min_body_data_rate` 字段已存在，但**慢速 body 速率防御（含心跳巡检）尚未启用**（规划在 M3）。

---

## 6. 连接中间件

连接级中间件在协议开始说话「之前」运行，可改写远端身份、拒绝连接、包装 reader/writer。用 `ServerWith` 传入一个 comptime 中间件元组：

```zig
const Srv = talon.http.ServerWith(App, .{
    talon.middleware.proxy_protocol, // 解析 PROXY v2 头，改写真实客户端地址
    talon.middleware.conn_log,       // 打印连接开/关与存活时长
});
var server = try Srv.init(gpa, &app, .{});
```

当前内置中间件（`src/core/conn_middleware.zig`）：

| 中间件 | 作用 |
|--------|------|
| `proxy_protocol` | 解析 HAProxy PROXY protocol v2 二进制前导，发布真实客户端地址；前导畸形即拒绝连接 |
| `conn_log` | 记录连接打开/关闭与生命周期 |

中间件签名：`fn (conn: anytype, next: anytype) !void`。在 `try next.call(conn)` 之前的代码是 inbound、之后是 outbound；不调 `next` 即短路（拒绝连接）。可以自己写中间件传进同一个元组。

> **尚未提供**：TLS 中间件（规划 M4）、内置 IP 限流中间件。需要 TLS 时，当前建议把 talon 放在 LB / 反向代理之后跑明文。

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

三者都构建在 reader 的 `peek/fill` 之上：零拷贝（frame 是借用切片）、自带 `max_frame` 防御、超时由 reader 透传。`next()` 返回 `null` 表示干净 EOF；`error.PartialFrame` 表示流在帧中间断了；`error.FrameTooLarge` 表示超限。

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

handler 里拿到底层连接、让引擎不再管它（WebSocket 升级等场景的原语）：

```zig
const raw = conn.hijack(); // 之后引擎不会 shutdown/close 这条连接
// raw 归你所有，自己负责 close
```

> M0/M1 劫持契约：劫持后**你拥有 close 责任**；`Connection` 的 reader/writer 缓冲只在 `serve` 的动态作用域内有效，连接若要活得更久需自己重新 buffer。完整所有权转移规划在 M3。WebSocket 等高层协议由生态包实现，talon 只给劫持原语。

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

可直接参考 `src/http/http.zig` 里的集成测试（keep-alive、POST body、走私拒绝、停机、proxy_protocol）。

---

## 10. 已实现 / 暂未提供 一览

便于你判断当前能不能用上某能力。

**已实现（M0 + M1）**

- HTTP/1.1：keep-alive、pipelining、定长 + chunked 响应、CL/chunked body 流式读、HEAD、`Expect: 100-continue`、严格 RFC 9112 解析与请求走私防御。
- `talon.core`：`StreamServer`、`TcpListener`、`MemoryListener`、连接限额、优雅停机、`chain` 中间件、`framing`（三组件）、buffer pool（含 Debug 借出泄漏追踪）。
- 连接中间件：`proxy_protocol`、`conn_log`。
- 劫持原语 `hijack()`（M0/M1 契约）。

**暂未提供**

- TLS（规划 M4，当前置于 LB/反代后跑明文）。
- UDP / DatagramServer（规划 M3，需求驱动）。
- Unix domain socket listener（架构预留，代码未落地）。
- 慢速 body 速率防御 + 心跳巡检（M3）。
- 内置 IP 限流中间件、`SO_REUSEPORT`、buffer pool 多 size class、`sendFile` 零拷贝、HTTP/2 / AutoProtocol（M3/M4）。
- 类型化 feature 查询（`conn.has/get`，架构 §6）尚未在 `Connection` 上落地；当前只有 `remoteInfo` 覆盖机制。

---

## 11. 常用命令

```bash
zig build               # 编译库 + examples
zig build test          # 跑全部单元/集成测试
zig build run-http      # 跑 HTTP 示例（127.0.0.1:8080）
zig build run-resp      # 跑 RESP 示例（127.0.0.1:6380）
```
