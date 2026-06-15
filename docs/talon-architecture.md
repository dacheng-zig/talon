# talon：基于 zio 的网络服务引擎架构设计

> 范围：talon 引擎的架构设计与实现思路；Web 框架见配套文档 `wing-architecture.md`

**命名**：talon（隼之利爪）承接 Kestrel（红隼）的猛禽血统而更锋利；与 Web 框架 wing（翅）同属一只猛禽——爪司力量与击杀（引擎性能），翅司扬升与优雅（框架 DX）。*Claws for the kill, wings for the flight.* 名字经 GitHub 撞名扫描：网络/服务框架领域无活跃同名旗舰（2026-06）。

## 1. 目标与非目标

### 目标

- 定位 ≈ **Kestrel 的工程品质 + Netty 的协议通用性**：
  - `talon-core`：协议无关的网络服务引擎，**stream（TCP/Unix）与 datagram（UDP）双引擎** + 共享底座。可二开为：HTTP 服务、自定义 TCP 协议服务（RPC、redis-like、消息网关）、UDP 私有协议服务（游戏、自定义可靠传输）、反向代理数据面
  - `talon-http`：HTTP/1.1 协议层（StreamServer 上的一个 Proto 实现）
- 性能：HTTP/1.1 keep-alive 明文场景达到同类 Zig 框架（zap、http.zig）同一量级；热路径每请求零堆分配
- Zig native：comptime 组合替代运行时多态；显式内存所有权；`std.Io.Reader/Writer` 贯通
- 易扩展且可测试：传输、连接中间件、协议三个正交扩展轴；内存传输层支撑无 socket 测试

### 非目标（首期）

- HTTP/2、HTTP/3（预留协议扩展点；datagram 引擎 + SessionTable 是未来 QUIC 的地基）
- 内置 TLS 实现（预留连接中间件扩展点）
- SCTP 等小众传输；**stream/datagram 统一 Proto 抽象**（刻意不做，见 §3 Netty 节）
- 兼容 zio 以外的运行时（理由见 §12-3）

## 2. 总体架构与对外契约

```
┌──────────────────────────────────────────────────────┐
│ wing (独立项目，见 wing-architecture.md)                │
├────────────────── 公开契约（见下） ─────────────────────┤
│ talon                                                  │
│  ┌────────────────────────────────────────────────┐   │
│  │ talon-http: HTTP/1.1 解析/编码/keep-alive/upgrade│   │
│  │             (StreamServer 的 Proto 之一，可替换)  │   │
│  ├────────────────────────────────────────────────┤   │
│  │ talon-core: 协议无关网络服务引擎                  │   │
│  │  ┌──────────────────┐  ┌──────────────────┐    │   │
│  │  │ StreamServer      │  │ DatagramServer   │    │   │
│  │  │ Listener 抽象      │  │ recv 循环 ×N      │    │   │
│  │  │ (tcp/unix/memory) │  │ SessionTable(可选)│    │   │
│  │  │ 连接中间件链        │  │ 报文中间件链       │    │   │
│  │  └──────────────────┘  └──────────────────┘    │   │
│  │  ───────────── 共享底座 ─────────────            │   │
│  │  chain 组合器 / buffer pool / framing /          │   │
│  │  Limits·心跳 / 生命周期·停机 / feature / 指标      │   │
│  └────────────────────────────────────────────────┘   │
├──────────────────────────────────────────────────────┤
│ zio: Runtime / Executor / coroutine / net / sync       │
├──────────────────────────────────────────────────────┤
│ io_uring / epoll / kqueue / IOCP / poll                │
└──────────────────────────────────────────────────────┘
```

**模块**：包 `talon` 只导出一个模块 `talon`（默认入口），依赖 zio。协议无关引擎在 `talon.core`（源码 `src/core/`，相对 import 拉入，不再是独立可依赖模块），HTTP 层在 `talon.http`。

**对上层（wing 及任何使用方）的公开契约**——semver 边界，破坏性变更走 major：

1. `Server(App)` comptime 泛型（App 提供 `handle(req, res)`）
2. `Request` / `Response` / `BodyReader` 类型（Reader/Writer 保持 `std.Io` 接口）
3. `req.hijack()` 连接劫持原语
4. feature 查询（§6）
5. `chain` 组合器（wing 的请求级中间件复用它，权威定义在 §7）

**双引擎而非统一抽象**：stream 与 datagram 的生命周期语义差异过大（连接 vs 会话表、字节流 vs 离散报文、协程阻塞背压 vs 丢包语义），强行统一只会得到最小公分母接口。talon-core 的做法是两个 sibling 引擎共享正交底座——`chain` 组合器泛型于上下文类型，buffer pool / Limits / 生命周期天然与传输语义无关。

## 3. 借鉴矩阵（引擎侧）

设计不是平移，每项都过"是否符合 Zig/zio 的约束与哲学"这一关。框架侧借鉴（ASP.NET Core 路由、axum 提取器等）见 wing 文档。

### 来自 Kestrel

| 机制 | 裁决 | Zig 化形态 |
|------|------|-----------|
| Transport 抽象（`IConnectionListenerFactory`，socket/命名管道/QUIC 可插拔） | **采纳** | comptime `Listener` 契约；内置 `tcp` / `unix` / `memory`（§5.2） |
| 连接中间件（`ListenOptions.Use`，TLS 本质是连接中间件） | **采纳** | comptime 连接级 chain；TLS、PROXY protocol、IP 限流都是中间件（§5.3） |
| `IFeatureCollection`（按类型查询连接能力） | **改造** | 运行时类型字典反 Zig；改为 comptime 能力查询 + 单一运行时扩展槽（§6） |
| System.IO.Pipelines（PipeReader/Writer、背压阈值、解耦读与析） | **拒绝结构，吸收思想** | Pipelines 是为无栈 async 解耦生产/消费而生；有栈协程下"reader 即解析循环"，平移是反模式。吸收：buffer pool、背压水位、`peek/fill` 增量解析 |
| Heartbeat + `MinRequestBodyDataRate`（慢速攻击防御） | **采纳（M3）** | server 级心跳协程 + 每连接速率计数（§5.6） |
| `HttpProtocols` 同端点多协议 | **采纳** | Protocol 分发预留（§5.7） |
| `MemoryPool<byte>` slab 分配 | **采纳** | `buffer_pool`（§5.4） |
| TestServer 内存传输测试 | **采纳** | `memory` Listener；wing TestClient 构建其上 |
| 连接层后置泛化尝试（`Microsoft.AspNetCore.Connections` 抽取 + Bedrock.Framework） | **吸收教训** | Kestrel 的连接层是 HTTP 成型后回头抽的，Bedrock 始终半温不火且无 UDP 一等支持——**泛化必须在架构期发生**，这正是 talon-core 现在就做双引擎定位的理由 |

### 来自 Netty

| 机制 | 裁决 | Zig 化形态 |
|------|------|-----------|
| Codec 拆帧框架（`ByteToMessageDecoder`、`LengthFieldBasedFrameDecoder` 等） | **采纳（最高价值）** | talon-core `framing` 工具箱：`std.Io.Reader` 之上的 comptime 拆帧组件，自定义 `Proto` 作者只写协议状态机不写缓冲管理（§8） |
| 池化分配器多 size class（jemalloc 式 arena） | **采纳（M3）** | buffer_pool 从单一规格扩展为 4K/16K/64K 三级 size class（§5.4） |
| 采样式泄漏检测（ResourceLeakDetector） | **改造** | GPA 已有 debug 泄漏检测；轻量吸收为 buffer_pool debug build 记录借出点 `@returnAddress`（§5.4） |
| 泛型网络引擎定位（不限 HTTP、不限 TCP；`DatagramChannel` 一等支持 UDP） | **采纳定位，拒绝其抽象结构** | stream + datagram 双引擎；但不学 Netty 把一切收敛为统一 `Channel` + 消息对象管线（代价是运行时类型擦除），而是 sibling 引擎共享正交底座（§2、§9） |
| Channel 终生绑定 EventLoop（串行化无锁模型） | **采纳意图，受限于 zio** | 十年生产验证的连接-线程绑定价值，作为向 zio 上游提 spawn affinity 的论证素材（§12-2） |
| Pipeline 运行时可变（原地 replace handler 做协议升级） | **拒绝** | 代价是 volatile 读 + megamorphic 分发（Netty 自身性能痛点）；协议升级走 `hijack()` + AutoProtocol，comptime 静态链不牺牲 |
| inbound/outbound 双向链 | **拒绝，已有更优解** | around 式中间件 `next` 前=inbound、后=outbound，单一抽象天然双向 |
| 写水位线背压（high/low watermark + writability 事件） | **拒绝（stream 侧架构性 N/A）** | 水位线是非阻塞写内存排队的补丁；有栈协程下 `write` 挂起协程，协程阻塞本身就是背压。datagram 侧背压语义是丢包，由显式丢弃策略表达（§9） |
| HashedWheelTimer | **拒绝（N/A）** | zio 超时是内核级 per-op `Timeout`；数据速率巡检已采 Kestrel 心跳方案 |
| ByteBuf 引用计数 | **拒绝** | 引用计数是 GC 语言管理堆外内存的无奈之举，也是 Netty 用户 use-after-free 的头号来源；显式所有权 + arena 生命周期契约从设计上消除该问题域 |
| boss/worker group 分离 | **已有等价物** | 单 accept 协程 + SO_REUSEPORT 演进项 |

### 来自 tower / actix（引擎相关部分）

| 机制 | 裁决 | Zig 化形态 |
|------|------|-----------|
| tower `Service`/`Layer`（中间件抽象独立于 HTTP） | **吸收思想，重塑结构** | 泛型 `chain(Ctx, ...)` 组合器：同一套机制服务连接级/报文级/（wing 的）请求级中间件（§7） |
| actix 极致零拷贝文化（`Bytes` 贯穿） | **吸收** | header/param 切片借用缓冲区贯穿到 handler |

> 五个参照系统看完的规律：它们最复杂的机制（Pipelines、ByteBuf 引用计数、写水位线、tower trait 体操）几乎都在为宿主语言约束（无栈 async / GC / 借用检查）打补丁，而有栈协程 + 显式内存 + comptime 恰好消掉了这些约束。借鉴纪律：**抄问题清单（拆帧、背压、慢攻击、协议升级、UDP 一等支持），不抄解法结构**。

## 4. zio 能力盘点

关键事实（file:line 以 zio v0.14.0 为准）：多执行器 per-thread 事件循环（`src/runtime.zig:61`）、spawn round-robin 无亲和性（`src/task.zig:557`）、协程栈 256KB committed 默认**必须调小**（`src/coro/stack.zig`）、全 IO 带内核级 `Timeout`（`src/net.zig:1090`）、`peek/fill/takeDelimiter` 缓冲读（`src/net.zig:1137`）、`writeVec`（`src/net.zig:1122`）、`sendFile` 零拷贝（`src/net.zig:1242`）、UDP：`Socket.receiveFrom/sendTo/receiveMsg/sendMsg` 带 `Timeout`（`src/net.zig:980-1049`，`examples/udp_echo_server.zig` 可作参照）、`Group` 结构化并发（`src/group.zig:20`）、Semaphore/ResetEvent 等原语（`src/sync/`）、`std.http.Server` 可直接跑通（`examples/http_server.zig`）、无内置 TLS。

## 5. StreamServer 与 HTTP 协议层

### 5.1 分层与二开契约

核心立场：**把"服务器"与"协议"解耦，把"引擎"与"传输语义"解耦**。

```zig
// ── talon-core / StreamServer：协议无关 ──────────────
// Proto 是 comptime 契约：拿到一条就绪的连接，自己决定怎么说话
pub fn StreamServer(comptime Proto: type, comptime App: type) type {
    // comptime 校验 Proto 契约：
    //   Proto.serve(conn: *Connection, app: *App) anyerror!void
    // Connection 提供：reader()/writer()（已穿过连接中间件链）、
    //   feature 查询、deadline 控制、hijack、shutting_down 信号
}

// ── talon-http：HTTP/1.1 是 Proto 的一个实现 ─────────
pub fn Server(comptime App: type) type {
    return StreamServer(Http1Protocol(App), App);
}
```

实现一个 `Proto.serve`，就能复用 talon-core 的全部基建——Listener 抽象、连接中间件（含未来 TLS）、连接数限额、超时、优雅停机、buffer pool、framing 工具箱、指标钩子。`Proto.serve` 是普通协程函数，内部就是该协议的连接循环——有栈协程让协议实现者写线性代码，这是对比 Kestrel `ConnectionDelegate`（async 状态机）更友好的地方。

### 5.2 Listener 抽象

```zig
// comptime 契约（duck typing + @compileError 校验）：
//   accept(self) !RawConnection      — 挂起直到新连接
//   close(self) void
//   RawConnection: reader()/writer()/close()/shutdown()/remote_info()
pub const TcpListener = ...;     // zio.net 实现，支持可选 SO_REUSEPORT（走低层 Socket API）
pub const UnixListener = ...;    // unix domain socket
pub const MemoryListener = ...;  // 进程内队列模拟连接：测试与嵌入场景
```

- **MemoryListener 是一等公民**（Kestrel TestServer 的教训：测试基建后补会渗透改造所有层）。实现：`zio.Channel(MemoryConn)` + 内存管道，连接两端都是 `std.Io.Reader/Writer`
- Listener 类型经 comptime 注入 `StreamServer`，单一部署中 listener 类型已知 → 静态分发；多 endpoint 监听 = 多个 server 实例共享同一 App
- 演进：QUIC/HTTP3 时基于 DatagramServer + SessionTable 另行设计（YAGNI）

### 5.3 连接中间件链

```zig
var server = try talon.http.Server(App).init(gpa, &app, .{
    .listener = .{ .tcp = .{ .address = addr } },
    .conn_middleware = .{
        talon.middleware.proxy_protocol,   // 解析 PROXY v2 头，改写 remote_info
        talon.middleware.ip_rate_limit(.{ .max_per_ip = 100 }),
        // 未来: talon.middleware.tls(.{ .cert = ..., .key = ... }),
        talon.middleware.conn_log,
    },
    ...
});
```

- 每个连接中间件签名：`fn (conn: *Connection, next: anytype) !void`，可在 `next` 前后做事、包装 reader/writer（TLS 即在此处替换为加密流）、或直接拒绝连接
- **TLS 是中间件而非 Transport variant**：TLS + PROXY protocol + 限流任意组合顺序自然成立，无组合爆炸；sendFile 零拷贝在 TLS 中间件存在时自动降级（comptime 检测链中流替换型中间件，静态决定 sendFile 路径）

### 5.4 连接生命周期与内存模型

- 每连接一协程，线性 keep-alive 循环；超时全走 zio 内核级 `Timeout`（首请求 header 超时 / keep-alive idle 超时分别设置）
- `Semaphore` 实现 max_connections backpressure（accept 前 wait）
- **协程栈 committed_size 调至 64KB**（zio 默认 256KB 在 10k 连接下即 2.6GB，是最大隐性成本）；读写缓冲走 buffer pool 不放栈
- 请求 arena 每请求 `reset(.retain_capacity)`；header/param 全部是借用读缓冲的切片，生命周期 = 当前请求，逃逸需 `dupe`
- 稳态热路径零 malloc/free

buffer pool（源自 Netty 池化分配器经验）：

- **多 size class（M3）**：4K / 16K / 64K 三级 free-list，按用途取用（写缓冲 4K、读缓冲 16K、压缩/聚合缓冲 64K）；不做 jemalloc 式完整 arena——三级定长对 server 工作负载已够，复杂度不值
- **debug 泄漏追踪（M1，随 buffer pool 首次实现）**：debug build 记录每块借出点 `@returnAddress`，停机时报告未归还块及借出位置；release 零成本（comptime 裁剪）。GPA 泄漏检测只覆盖 malloc 路径，池内"借出未还"是它的盲区

### 5.5 HTTP/1.1 协议层

- M0 借 `std.http.Server` 拿正确性参照与性能基线；M1 替换为自研单遍零拷贝解析器（picohttpparser 思路：`peek()` 连续缓冲上单遍扫描，`std.mem` SIMD 原语，常见 header comptime 快速路径，严格 RFC 9112 走私防御）
- body 统一暴露 `std.Io.Reader` 接口（CL 限读 / chunked 解码），流式不缓冲
- 写路径：定长响应 `writeVec` 单 syscall；流式 chunked Writer；静态文件 `sendFile` 零拷贝；`Date` header 每秒缓存
- HTTP/1.1 pipelining 自然支持（Reader 缓冲区残留字节下一轮直接命中）
- 解析器是纯函数（bytes in / struct out），独立 fuzz

### 5.6 Server limits 与慢速攻击防御

```zig
pub const Limits = struct {
    max_connections: u32 = 65536,
    max_header_size: u32 = 16 * 1024,
    max_body_size: ?u64 = 16 * 1024 * 1024,   // null = 不限
    header_read_timeout: zio.Timeout = .fromSeconds(10),
    keep_alive_timeout: zio.Timeout = .fromSeconds(75),
    drain_timeout: zio.Timeout = .fromSeconds(30),
    // M3：慢速攻击防御（Slowloris / slow body）
    min_body_data_rate: ?DataRate = .{ .bytes_per_sec = 240, .grace = .fromSeconds(5) },
};
```

- 读超时防"头部慢发"；`min_body_data_rate` 防"body 慢发"——server 级 1s 心跳协程巡检各连接字节计数窗口（Kestrel Heartbeat 同款），不在热路径加锁：每连接计数器单写者（连接协程写、心跳读），monotonic 原子即可
- M3 落地，但 `Limits` 结构与计数点 M0 就预留，避免后插改连接循环

### 5.7 协议分发与连接劫持

- 同端点多协议（HTTP/1.1 + 未来 h2）：`Http1Protocol` 之上加 `AutoProtocol`——按 TLS ALPN 或 h2c preface 嗅探分发，仍是 comptime 合成的 tagged 分支
- `req.hijack()`：移交 `Connection`（已含中间件包装后的 reader/writer）给 handler，server 循环退出不关连接；WebSocket 作为 wing 生态包实现，talon 只提供劫持原语

### 5.8 生命周期与优雅停机

- 停机序列：关 listener → 置 shutting_down（请求边界退出 + `Connection: close`）→ `Group.wait` 带 drain_timeout → 超时 `Group.cancel`（写出中 shield 保护）
- 生命周期钩子（IHostedService 的减法版）：`on_start` / `on_shutdown` 回调 + 由 server `Group` 托管的后台协程注册口，保证后台任务参与结构化停机

## 6. Feature 机制（Kestrel `IFeatureCollection` 的 Zig 化）

```zig
// 一级（默认）：comptime 能力查询——零成本
// Connection 类型由 (Listener, conn_middleware 链) comptime 合成，
// 能力即类型上的 decl，存在性编译期可知：
if (comptime conn.has(talon.features.TlsInfo)) {
    const tls = conn.get(talon.features.TlsInfo); // 直接字段访问，无查找
}

// 二级（逃生舱）：每连接一个可选的 *anyopaque 扩展槽 + tag，
// 给动态插件场景；文档注明非热路径专用
```

中间件向链下游"发布"能力：中间件类型声明 `pub const provides = .{ TlsInfo };`，`chain` 组合时 comptime 聚合成 Connection 的能力集。等价于 IFeatureCollection 的静态版本——能力缺失在编译期就报错，而不是运行时 null。

## 7. chain 组合器（tower 思想的 Zig 重塑，权威定义）

tower 的核心洞见：中间件抽象不该绑定 HTTP。Zig 的等价物不是 trait 对象，而是**泛型于上下文类型的 comptime 组合器**：

```zig
// 同一台机器，三处复用：
pub fn chain(comptime Ctx: type, comptime mws: anytype) type {
    // comptime 校验每个 mw: fn (ctx: *Ctx, next: anytype) !void
    // inline 递归展开为嵌套调用，优化后等价手写大函数
}

// talon 连接级： chain(talon.Connection(...), .{ proxy_protocol, tls, conn_log })
// talon 报文级： chain(talon.Packet, .{ ip_rate_limit, pkt_log })
// wing  请求级： chain(wing.Context(State), .{ logger, recover, cors, auth })
```

- 一个中间件若不依赖 Ctx 的特定 decl，天然跨层/跨应用复用（限流、日志骨架）——tower 生态效应的 comptime 版本
- 中间件可声明 `provides`（§6）与 `requires`（comptime 校验链中顺序依赖），把"中间件顺序错误"这类经典运行时 bug 变成编译错误——**五个参照系统都没有的能力，Zig comptime 独有红利**
- 逃生舱：`chain` 末端允许挂运行时 `*const fn` 中间件数组，动态注册场景用，文档注明间接调用成本
- **实施提示**：chain 是两个项目共用的地基、返工成本最高，应作为 M0 第一个模块单独写透并配足测试

## 8. framing 拆帧工具箱（Netty Codec 框架的 Zig 化）

Netty 能长出 HTTP/2、gRPC、Redis、MQTT 全家桶，根本原因是 Codec 框架让协议作者只写状态机、不写缓冲管理。talon-core 提供同等能力，形态是 **`std.Io.Reader` 之上的 comptime 组件**而非运行时 handler：

```zig
// 1) 长度前缀拆帧 —— RPC / 私有二进制协议的标准件
var framed = talon.framing.LengthPrefixed(.{
    .length_type = u32, .endian = .big,
    .max_frame = 1 * 1024 * 1024,
    .includes_header = false,
}).init(conn.reader());
const frame: []const u8 = try framed.next();   // 借用缓冲，下次 next 失效

// 2) 分隔符拆帧 —— 行协议（RESP、SMTP、memcached text）
var lines = talon.framing.Delimited(.{ .delimiter = "\r\n", .max_frame = 64 * 1024 })
    .init(conn.reader());

// 3) 增量累积模板 —— 自定义状态机的兜底件（ByteToMessageDecoder 等价物）
//    decode(buffered: []const u8) → .{ .need_more }, .{ .frame, consumed }, .{ .err }
var dec = talon.framing.Accumulator(MyDecoder).init(conn.reader());
```

- 三组件全部构建在 zio Reader 的 `peek/fill` 之上，零拷贝（frame 是借用切片）、自带 max_frame 防御、超时由 Reader 的 `Timeout` 透传；HTTP/1.1 解析器自身就是 `Accumulator` 模式的手写特化
- 二开故事的关键放大器：写 RESP 服务 = `Delimited` + 命令分发；写 RPC 网关 = `LengthPrefixed` + 路由。RESP echo 示例（§10）改用 framing 实现，同时验证 core 契约与工具箱
- 编码侧暂不做对称抽象：写帧就是 `writeVec(&.{header, payload})`（YAGNI；出现真实重复再提取）
- **framing 仅服务 stream 侧**：datagram 天然就是帧（§9）

## 9. DatagramServer：UDP 引擎

stream 侧的核心抽象（accept、Connection、字节流、协程阻塞背压）对 UDP 全部不成立：UDP 没有 accept、报文是离散消息、背压语义是丢包。因此 DatagramServer 是 sibling 引擎而非 StreamServer 的变体，复用全部共享底座（chain、buffer pool、Limits、生命周期、feature、指标）。

```zig
// 模式 A：无状态（DNS-like、探测、日志收集）
// Proto 契约：onPacket(pkt: Packet, reply: *Replier, app: *App) !void
// N 个 recv 协程（SO_REUSEPORT 多 socket）直接调用，零会话开销

// 模式 B：会话式（游戏、KCP、DTLS-like、自定义可靠传输）
// Proto 契约：
//   sessionKey(pkt) SessionKey   — comptime 注入：默认对端地址；可自定义
//                                   （如 QUIC 风格 connection ID，支持对端地址迁移）
//   Session.serve(mailbox: *zio.Channel(Packet), reply: *Replier, app: *App) !void
pub fn DatagramServer(comptime Proto: type, comptime App: type) type { ... }
```

设计要点：

- **模式 B 把 stream 侧的核心体验复制到 UDP**：core 维护 `SessionTable`（按 SessionKey 哈希），首包创建会话协程 + 有界 mailbox（`zio.Channel(Packet)`），idle 超时驱逐，`max_sessions` 限额。每会话一个协程写线性代码，会话被 `Group` 托管、参与结构化停机——对 Netty 的差异化优势（它的 UDP 会话管理要用户自己拿 map 拼）
- **丢弃策略显式化**：mailbox 满时 `drop_newest / drop_oldest / block`，由 Proto 声明——UDP 语义允许丢，但必须是协议作者的显式决定
- **报文中间件链**：`chain` 的 datagram 实例（per-IP 限速、日志、统计），签名 `fn (pkt: *Packet, next: anytype) !void`
- recv 循环 ×N（SO_REUSEPORT 多 socket）分摊收包；zio 地基：`receiveFrom/sendTo/receiveMsg/sendMsg` 带 `Timeout`（`src/net.zig:980-1049`）
- 优雅停机：停 recv 循环 → 会话协程 mailbox 排干或 idle 后退出 → `Group.wait` 带 drain_timeout → 超时 cancel
- **排期纪律**：架构与共享底座接口从 M0 起按双消费者设计（成本≈0），DatagramServer 实现排 M3 占位、**需求驱动启动**，避免无真实协议驱动的空造抽象。额外收益：SessionTable + datagram 引擎是未来 HTTP/3（QUIC）的地基

## 10. 性能策略与项目结构

| 策略 | 机制 |
|------|------|
| 零虚调用分发 | `Server(App)` + comptime 中间件链 |
| 每请求零堆分配 | arena reset + header 借用切片 + buffer pool |
| 最少 syscall | writeVec 合并响应、缓冲 Reader、TCP_NODELAY |
| 零拷贝文件 | sendFile（io_uring/sendfile/TransmitFile），TLS 链下 comptime 静态降级 |
| 内核级超时 | zio `Timeout`，无用户态 timer wheel |
| 多核扩展 | `.auto` 执行器 + per-executor event loop |
| 内存密度 | 64KB committed 栈 + 池化 buffer ≈ 84KB/连接 |
| SIMD 解析 | std.mem SIMD 原语 + 可选 @Vector 专用扫描 |
| Date 缓存 | 每秒刷新 |
| 心跳巡检不进热路径 | 数据速率计数单写者 + monotonic 原子，1s 心跳协程读 |

基准方法：`wrk`/`oha` 固定场景（plaintext、json、64 与 4096 连接、keep-alive 与 close），三条对照线（M0 std.http 基线 / talon 当前 / zap、http.zig 参照），结论只来自测量。

```
talon/                             # dacheng-zig/talon
├── build.zig / build.zig.zon      # 依赖 zio；只导出模块 talon
├── src/
│   ├── talon.zig                  # module: talon（core 顶层 + talon.http 包）
│   ├── core/
│   │   ├── core.zig               # talon.core（引擎根，相对 import）
│   │   ├── stream_server.zig      # StreamServer(Proto, App)、accept、生命周期
│   │   ├── datagram_server.zig    # DatagramServer(Proto, App)、recv 循环（M3 占位）
│   │   ├── session_table.zig      # SessionTable、mailbox、idle 驱逐（M3 占位）
│   │   ├── connection.zig         # Connection、feature 合成、hijack
│   │   ├── listener.zig           # Listener 契约 + tcp/unix/memory
│   │   ├── conn_middleware.zig    # proxy_protocol、ip_rate_limit、conn_log
│   │   ├── chain.zig              # 泛型 chain 组合器（wing 复用）
│   │   ├── framing.zig            # LengthPrefixed / Delimited / Accumulator
│   │   ├── limits.zig             # Limits、心跳、数据速率
│   │   └── buffer_pool.zig        # 多 size class + debug 借出追踪
│   ├── http/
│   │   ├── http.zig               # package: talon.http（Server/Request/Response/…）
│   │   ├── protocol.zig           # Http1Protocol、keep-alive 循环
│   │   ├── parser.zig             # 纯函数解析器（独立 fuzz）
│   │   ├── body.zig / encode.zig
│   │   └── request.zig / response.zig
│   └── ...
├── examples/                      # http hello、自定义协议(RESP echo) 各一个
└── bench/
```

仓库自带一个非 HTTP 的示例协议（RESP echo），用真实代码钉住 core/http 边界不腐化——边界只靠文档约束必然漂移。

## 11. 实施路线图（talon 侧）

| 里程碑 | 内容                                                                                                                                                                         | 验收 |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| **M0** | talon core 骨架：StreamServer + TcpListener + MemoryListener + Semaphore 限额 + 优雅停机 + chain 组合器（接口按 stream/datagram 双消费者审视）；HTTP 协议层借 `std.http.Server`；RESP echo 示例验证 core 契约 | 功能正确；wrk 基线；RESP 示例只用 core 契约、不触 http/ 代码（边界由隔离的 `core-boundary` 编译守卫强制） |
| **M1** | 自研 http1 解析器 + 零拷贝 + arena + buffer pool（含 debug 借出追踪）+ writeVec，替换 std.http；连接中间件链（先 conn_log + proxy_protocol）；framing 工具箱（RESP 示例改用 Delimited）                          | 吞吐显著超 M0；fuzz 无 crash；走私用例全拒绝；framing 三组件有独立测试 |
| **M3** | 生产化：hijack 原语完善、数据速率防御 + 心跳、指标钩子、SO_REUSEPORT、buffer pool 多 size class；**DatagramServer + SessionTable（需求驱动，无真实 UDP 协议需求则顺延）**                                             | 24h 长稳无泄漏（GPA leak check + pool 借出追踪零未还）；Slowloris 用例防御生效 |
| **M4** | TLS 连接中间件、AutoProtocol/h2 评估、QUIC/HTTP3 预研（基于 DatagramServer + SessionTable）                                                                                               | 另立设计文档 |

（M2 是 wing 侧里程碑，依赖 talon M1，见 wing 文档。）

## 12. 风险与权衡

1. **协程栈内存 vs 状态机**（已裁决）：有栈协程的代码可维护性收益巨大，但每连接固定栈成本高于 async 状态机。缓解：committed_size 调到 64KB（必要时 32KB）、buffer 出栈入池。残余风险：定位万级并发连接，不对标 C1M。
2. **zio spawn 无执行器亲和性**：round-robin + 远程调度存在跨线程唤醒开销，SO_REUSEPORT 优化吃不满。缓解：`enable_task_migration` 默认开启可部分自愈；中期向 zio 上游提 spawn affinity（issue #460 有 work-stealing 讨论；Netty 的 Channel 绑定模型是论证素材）。
3. **绑定 zio native API vs `std.Io` 可移植**（已裁决）：talon 核心用 native API（per-op `Timeout`、`Group`、shield 都是 std.Io 没有或更弱的能力），如同 Kestrel 绑定 .NET；对外 Reader/Writer 保持 `std.Io` 接口。牺牲：talon 不能跑在其他 `std.Io` 实现上。
4. **comptime 重度使用的编译时间与报错体验**：中间件链、Connection 类型合成会放大编译错误的间接性。缓解：每个 comptime 入口先做显式签名校验并以短名 `@compileError` 给出人话提示；提供 `talon.DefaultServer` 等常用特化别名；CI 跟踪编译耗时。
5. **`std.http.Server` 在 M0 的依赖**：若 std 接口变动，仅影响 M0 脚手架，M1 后无此依赖。
6. **TLS 缺位**：明文 HTTP 生产通常置于 LB/反代之后，可接受；直接暴露公网的场景 M4 前不支持，README 明示。
7. **Feature 二级扩展槽的滥用风险**：运行时槽位绕开 comptime 保证。缓解：API 命名带 `dynamic` 前缀 + 文档定位为插件逃生舱，内置代码一律走 comptime 一级。
8. **定位升级的范围风险**：从"HTTP server（类 Kestrel）"扩展到"网络引擎（类 Netty）"，Netty 用了十几年才把泛型引擎做扎实。缓解：**架构上立刻泛化（共享底座按双消费者设计，成本≈0），实现上严格按需求排期**——DatagramServer 需求驱动，无真实 UDP 协议不动工。
9. **与 wing 的协调成本**：公开契约（§2）视为 semver 边界，破坏性变更走 major；开发期用 `zig build --fork` 本地联调两仓。
