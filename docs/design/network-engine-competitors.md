# Talon 网络引擎：跨语言竞品与架构参考

资料读取日期：2026-10-09。用途：为 [设计评估与新模型](network-engine-remodel.md) 提供可核查的比较依据。本文分析架构和适用边界，不做跨项目性能排名，不将官方宣传或某次 benchmark 数字转化为 Talon 的容量承诺。

## 1. 选型边界与比较方式

Talon 同时包含流式连接引擎、协议 codec、服务端和长寿命客户端。没有一个竞品与它的全部边界一一对应，因此分三类取材：代理产品 Nginx/Envoy；协议与服务框架 Netty/Hyper/net-http/fasthttp/Kestrel；底层运行时与循环 Tokio/libuv/Seastar/libxev。它们是竞争或架构参照，不全是可直接替换的同层库。

选择覆盖 C、C++、Java、Rust、Go、C#、Zig 七种语言，并体现事件回调、有栈协程、Future、托管异步、每核分片和标准 I/O 接口的差异。重点是哪些设计规则能降低 Talon 的复杂度，以及哪些机制因语言或部署模型不同不能直接移植。

“官方事实”来自本轮读取的文档或源码；“架构代价”是本报告的判断，不代表已知故障或某版本漏洞。功能描述以所引用的项目层和版本为限；第三方扩展不当作原生内建能力。以下比较没有在相同机器上运行这些项目。

## 2. 总览

| 项目与语言               | 对应 Talon 层              | 核心执行/所有权模型                                    | 主要借鉴                                       | 主要适用边界或代价                                                    |
| ------------------------ | -------------------------- | ------------------------------------------------------ | ---------------------------------------------- | --------------------------------------------------------------------- |
| Nginx / C                | 高层代理、HTTP 服务        | master/worker，事件循环，请求/配置 pool，buffer chain  | 生命周期作用域、分阶段处理、增量发送           | 模块需手工维护回调和请求引用；产品配置模型重于库                      |
| Envoy / C++              | 代理服务、连接/HTTP filter | main/worker dispatcher，线程局部状态，连接/stream 过滤 | transport/network/HTTP 分阶段，流控、可观测性  | 控制平面与配置生态成本高；复杂 policy 不应下沉至 Talon core           |
| Netty / Java             | 通用网络协议框架           | Channel/EventLoop/Pipeline，ByteBuf 引用计数           | codec 与会话边界、watermark、阻塞任务隔离      | pipeline 传播与引用计数增加认知成本；GC 不消除 buffer 释放责任        |
| Tokio + Hyper / Rust     | runtime + HTTP codec/会话  | Future/poll，executor，泛型 trait，所有权/Pin          | runtime 与协议分离，组合接口，显式流式 body    | 异步 trait/Pin/Send 复杂；Hyper 本身不是完整客户端 policy 或 Web 框架 |
| libuv / C                | 底层跨平台 I/O             | loop、handle、request、completion callback             | 关闭完成与存储寿命，平台封装                   | 不提供 HTTP 或结构化任务作用域，使用者承担状态机                      |
| Seastar / C++            | 高性能服务 runtime         | 每核 reactor，共享无关分片，Future 与协作调度          | 数据局部性、显式跨核通信、CPU 让出             | 分片侵入应用数据模型；移植和常规部署适配成本较高                      |
| Go net/http / Go         | HTTP 服务/客户端           | goroutine、runtime netpoll、GC，Handler/Transport      | 线性会话、长寿命客户端池、deadline 与 shutdown | 分配/调度的物理成本不由库显式控制；特定升级连接需要额外停机管理       |
| fasthttp / Go            | HTTP/1 服务/客户端         | worker 复用、RequestCtx 和 buffer 复用                 | 借用作用域、显式复用、少分配                   | 不兼容 net/http API；引用不得逃出 handler，扩展需另查                 |
| Kestrel + Pipelines / C# | HTTP 服务 + 流缓冲         | .NET 异步任务、GC、PipeReader/Writer 与池化 buffer     | 有界背压、消费位置、完成/释放语义              | consumed/examined 和 AdvanceTo 易误用；依赖托管 runtime               |
| libxev / Zig             | 底层跨平台 I/O             | completion/proactor，调用者提供存储，回调              | Zig 显式存储与静态能力，低分配循环             | 不是 std.Io 实现，不提供 Talon 协议/任务生命周期；平台声明需核实      |

HTTP 版本、TLS、代理、路由等能力不能按这一张表的“框架”称谓推定。后续各节给出具体责任和边界。

## 3. Nginx：以生命周期组织事件驱动服务器

**官方事实。** [开发指南](https://nginx.org/en/docs/dev/development_guide.html) 描述 master 控制 worker，worker 承担连接处理；事件包括 I/O、timer 和 posted events。HTTP 处理包含 phases、请求 finalization、subrequest 及 body filters。内存 pool 与配置周期/请求寿命相联系，buffer/chain 可以描述内存或文件片段。线程池用于指定任务，不能推导整个 HTTP 请求都在线程池执行。

核心架构是：配置与进程生命周期在上层；连接事件驱动请求状态机；模块在定义好的阶段参与；输出通过 buffer chain/filter 逐步推进。能力覆盖静态内容、反向代理、TLS、上游转发和配置重载等产品场景，具体功能与模块/构建配置有关。

**内存与失败。** 请求 pool 集中释放适合短寿命派生数据，但异步引用仍需与 finalization 对齐。buffer 被复用前必须确认下游已经消费；不能因 pool 整体释放就忽略未完成操作。日志和配置周期是产品内建运维模型的一部分。

**架构代价。** 回调式流转与请求引用关系由 C 开发者手工维护，扩展者需要了解较多隐含约定；任意阻塞逻辑会影响负责该连接的 worker。配置层、进程层和代理生态对一个可嵌入网络库过重。这不是 Nginx 的缺陷，而是其产品定位的代价。

**Talon 归纳。** 保留请求 arena、分阶段准入/解析/提交、缓冲输出的思想；让 Zig 协程隐藏普通会话状态转换。不照搬 master/worker、模块注册宏或 Nginx 特有请求对象。配置热重载仅在真实需求出现时引入，并使旧配置活到引用它的会话结束。

## 4. Envoy：区分传输、连接与 HTTP stream 的策略

**官方事实。** [线程模型](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/intro/threading_model) 描述 main 线程协调配置、统计及管理，worker 处理监听、过滤和转发，另有日志刷盘线程。它以 dispatcher 和线程局部状态减少共享争用；这些局部优化不意味着进程中没有同步。[HTTP connection management](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/http/http_connection_management) 描述连接管理器、codec 与 HTTP stream filter 的关系。

核心架构区分 listener/filter-chain、transport socket、network filter 和 HTTP filter。连接层负责传输，HTTP codec 建立 stream 语义，HTTP policy 决定路由、转发等。TLS、HTTP 多协议、上游连接池、流量控制、统计和动态配置是代理产品重要能力；配置决定启用哪些功能。

**内存与生命周期。** C++ 对象/RAII 和 worker 局部状态承担生命周期；一个上游连接与其请求 stream 不是同一概念。背压要同时考虑连接缓冲与 HTTP stream，否则过滤器缓冲可能扩大内存。统计和 trace 有明确运行环境集成，不能只靠调试日志解释失败。

**架构代价。** xDS、线程局部配置传播、路由/重试/过滤器生态提升功能，也扩大依赖与配置理解成本；自定义 filter 必须遵守暂停/恢复和对象寿命。对只需编写自定义 TCP 协议的使用者，这些结构会造成不必要的入口复杂度。

**Talon 归纳。** 强烈借鉴“建立阶段和应用过滤阶段不同”的规则，解决 PROXY/TLS 顺序；使 transport/peer 身份与 HTTP Host policy 分离。保留少量静态 stage，不引入 xDS 或动态 filter registry。若未来 HTTP/2 多路复用进入范围，应先区分 connection permit 与 stream permit，再谈复用同一 Limits。

## 5. Netty：强协议扩展能力与 buffer 所有权代价

**官方事实。** Netty 4.1 的 [ChannelPipeline](https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html) 以 handlers 截获 inbound 事件和 outbound 操作；事件方向、顺序和 executor 选择是契约的一部分。文档提供将耗时业务 handler 交给另一 executor group 的示例，并说明按 ChannelHandlerContext 串行保序也可能成为瓶颈。[ByteBuf](https://netty.io/4.1/api/io/netty/buffer/ByteBuf.html) 提供索引、slice/duplicate、读写操作与引用计数相关 API。

核心架构是 Bootstrap 选择 transport 和 EventLoop，Channel 代表连接，Pipeline 组合 handler，codec 进行 bytes/message 转换，业务 handler 处理消息。它支持通用协议、TLS/HTTP/WS 等生态组件，并提供可写性变化等流量控制信号。

**内存与生命周期。** Java GC 只管理普通对象可达性；引用计数 buffer 仍须 retain/release 正确。slice/duplicate 可能共享底层存储，跨线程或异步保存不等于自动获得独立 payload。泄漏检测是辅助，不替代所有权规则。

**架构代价。** pipeline 中的传播方向、是否转发、谁释放消息、在哪个 executor 运行，都成为扩展者必须知道的知识；配置灵活但动态派发与多对象边界增加理解成本。EventLoop 上阻塞或不让出的计算会妨碍其他连接进展。

**Talon 归纳。** framing/codec 负责 bytes 到语义的转换；Session 负责读写并发和消息寿命。借鉴 buffer 泄漏追踪和明确背压，避免将 Channel/Pipeline/HandlerContext 逐层翻成 Zig 包装。Talon 现有 comptime chain 足以承载固定 around 中间件；改变 stream 表示的 TLS 不归入同一 chain。

## 6. Tokio + Hyper：runtime 独立与 Rust 静态约束

**官方事实。** [Tokio教程](https://tokio.rs/tokio/tutorial) 将 runtime、async 任务、network 和同步工具组织为异步应用基础；[Tokio 1.47.1 runtime文档](https://docs.rs/tokio/1.47.1/tokio/runtime/index.html) 区分多线程与当前线程 scheduler 及其行为。Hyper v1.7.0 [README](https://github.com/hyperium/hyper/blob/v1.7.0/README.md) 定位为 HTTP 实现；其 [rt模块](https://github.com/hyperium/hyper/blob/v1.7.0/src/rt/mod.rs) 明确提供 executor、timer 和 I/O transport 抽象，让协议可独立于具体 runtime。

Rust 的 Future 通过 poll 推进；编译期所有权、借用、Send/Sync 和 Pin 影响任务与 buffer 能否移动/共享。Tokio 提供执行与 I/O 基础，Hyper 负责 HTTP/1、HTTP/2 协议会话和流式 body；路由、认证、TLS 提供者及高层重定向/池 policy 并不都由 Hyper 核心完成。

**内存与生命周期。** 无 GC，所有权和 Drop 释放资源，借用 checker 阻止很多悬空引用；但被丢弃的 future 已经执行到哪里、是否消费部分输入、是否适合重试，仍需操作的 cancel-safety 契约。编译期内存安全不能自动证明协议或取消语义正确。

**架构代价。** runtime、trait 组合、Future/Pin 与跨线程约束增加类型和错误信息的复杂度；高层产品需要多个库组合。泛型有利优化，但编译时间和代码 specialization 也要衡量，不能一概称“免费”。

**Talon 归纳。** 借鉴协议与 runtime 接口分离，使用 Zig std Reader/Writer 和明确执行能力。不要自造 Future/poll/Pin 体系模拟 Rust；zio 线性会话代码已经隐藏很多调度状态。Zig 没有 borrow checker，必须用明确作用域、稳定地址、join 及 Debug 检查补足契约，而非直接引用 Rust 安全结论。

## 7. libuv：handle/request 与关闭完成分离

**官方事实。** libuv v1.x [设计文档](https://docs.libuv.org/en/v1.x/design.html) 区分 long-lived handle 和短期 request；loop 与关联对象默认不 thread-safe。网络 I/O 由非阻塞 socket 和平台机制驱动，文件等操作及特定解析工作可能使用线程池。loop 执行阶段还包括 timer 与 close callback。

核心架构是平台事件循环、handle 监听、request 提交和 completion 回调。TCP/UDP、timer、进程、文件、DNS 等是底层能力，没有 HTTP codec、连接准入或请求政策。

**内存与生命周期。** 调用者必须保持操作及 handle 存储有效，直到相关 completion/close callback 允许回收；调用关闭不是瞬间可 free。取消 request 同样不意味着所有借用立刻消失。这种规则与 Zig 自引用 I/O 存储高度相关。

**架构代价。** 使用者要建立状态机、任务组织和回调取消协调；loop 固定线程的假设让跨线程访问需要另外安排。线程池不是无限 CPU 容量，也不能自动提供上层请求背压。

**Talon 归纳。** I/O 完成前资源不能释放是核心不变量；复用 zio 事件 backend，避免第二套 epoll/kqueue/IOCP 封装。让 SessionLease 集中所有权和收敛，将 callback 细节隐藏在 runtime/transport 内，不暴露给每个 Proto。

## 8. Seastar：每核局部性不是通用的免费优化

**官方事实。** 本轮读取 Seastar 动态 master 的 [教程](https://github.com/scylladb/seastar/blob/master/doc/tutorial.md)。其概念包括 cooperative 微任务调度、shared-nothing SMP、Future/continuation，以及每 core 独立 reactor 与显式跨 core 消息。教程也讨论 CPU 工作让出、task quota 和 preemption 检查。

核心架构按 shard 划分内存、数据和 CPU 职责，以局部执行减少 cacheline 迁移与共享锁。网络/磁盘 I/O 和异步组合形成完整服务 runtime；典型适用场景是可按数据分片、对尾延迟和多核扩展高度敏感的服务。

**内存与生命周期。** shard-local 对象通常在其所属 core 使用和释放，跨核调用通过消息/submit-to 等机制组织；有界并发和资源许可仍然必要。CPU-bound 工作必须周期让出，event-driven 并不自带任意代码的抢占。

**架构代价。** 分片会影响业务数据模型、跨核一致性、负载均衡和关闭顺序；应用的全局查询可能引入额外聚合。与普通多平台嵌入库相比，硬件/部署/生态假设更强。本文不根据教程的性能叙述断言它在任何负载都更快。

**Talon 归纳。** 将每核 pool/allocator 作为可选择的部署模型，先确认 shared contention 是瓶颈。采用时必须明确 quota 是局部还是全局、连接能否迁移、跨核 payload 由谁释放。保持对象固定地址不能代替任务固定 executor，不能默认假设 zio work-stealing 会保留局部所有权。

## 9. Go net/http：简单处理函数背后的连接作用域

**官方事实。** Go 1.26.0 [server.go](https://github.com/golang/go/blob/go1.26.0/src/net/http/server.go) 描述每个接受的连接创建 service goroutine，读取请求并调用 handler；具体 HTTP/2 路径与 TLS/协议配置相关。Server 提供 deadline 配置、连接状态以及 Shutdown；Shutdown 文档对 hijacked 连接、WebSocket 等给出额外管理边界。[transport.go](https://github.com/golang/go/blob/go1.26.0/src/net/http/transport.go) 和 [client.go](https://github.com/golang/go/blob/go1.26.0/src/net/http/client.go) 明确要求复用长寿命 Transport/Client，而不是每次请求新建。

core runtime 提供 goroutine/netpoll/GC；net/http 提供 HTTP 会话、Handler 和 ResponseWriter，客户端 Transport 承担连接池。它的生态、标准协议接口和易用性是重要优势。通过 context、deadline、显式 Transport 配置和应用限制管理工作，不能仅依赖 goroutine 廉价这一印象。

**内存与生命周期。** GC 降低普通对象 free 错误，连接、body 和子任务仍需及时关闭/取消。长寿命 Transport 有利连接复用；响应 body 完整性影响能否回池。HTTP 升级改变所有者，普通 Shutdown 不会自动完成所有升级连接的协议 close。

**架构代价。** allocator 选择和 buffer 保留不像 Zig 那样直接控制，物理内存包含 runtime、栈与 GC 成本；handler 自行创建的 goroutine 可能逃出请求作用域，context 不等于强制杀线程。易用默认值也不能替代场景化超时配置。

**Talon 归纳。** 保留线性请求循环和“长寿命 Client 是正常用法”；继续让 SSE/WS 占准入到释放。明确引擎 drain 与应用子任务/升级协议 close 的责任。把 GC 隐含的寿命转换成 arena/borrow/join 契约，而不是自动缓存所有 Request。

## 10. fasthttp：复用换来严格借用寿命

**官方事实。** fasthttp 动态 master 的 [README](https://github.com/valyala/fasthttp/blob/master/README.md) 明确建议多数场景使用 net/http，并强调不能在 handler 返回后保留 RequestCtx 或其成员引用。项目通过不同 API 和对象/buffer 复用扩展优化空间；[server.go](https://github.com/valyala/fasthttp/blob/master/server.go) 包含 worker pool 及并发控制相关实现。README 链接的第三方 HTTP/2、router、WS 扩展不能计为本文引用层的内建能力。

核心架构把短寿命上下文与内存复用紧密绑定，目标是 HTTP/1 场景中减少分配和操作开销。客户端、header 处理与 buffer 复用是功能特色；与 net/http 接口并不透明兼容。

**内存与生命周期。** Go 有 GC 仍不能安全保存复用上下文的引用；复用可能使旧引用指向下一请求的数据。跨 handler 异步工作必须复制所需内容，或按明确的特殊契约处理。pool 高水位和 idle 保留仍需观察。

**架构代价。** 调用者必须接受更严格的 lifetime 规则，迁移 net/http handler 存在语义陷阱；低分配与通用协议/生态兼容之间有取舍。README 中的生产吞吐数字具有其环境条件，不能移作 Talon 性能目标。

**Talon 归纳。** 显式 RequestView/OwnedFrame 的价值来自这种真实寿命差异。arena reset 复用值得保留，但异常大请求后的 retain 上限和释放回退同样重要。不能只宣传“每请求不 malloc”，还应说明 TLS/decoder/栈/arena 增长和调用方分配范围。

## 11. Kestrel + System.IO.Pipelines：有界字节队列的正确契约

**官方事实。** ASP.NET Core 10.0 [Kestrel概述](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/servers/kestrel?view=aspnetcore-10.0) 描述 HTTP/1、HTTP/2、HTTP/3 及 TLS 等能力，实际可用性取决于配置、平台和协议依赖。[Pipelines文档](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines) 说明 PipeReader/Writer、ReadOnlySequence、consumed/examined、AdvanceTo、Complete，以及 PauseWriterThreshold/ResumeWriterThreshold 的背压关系。

架构把 socket/transport、协议解析和应用请求处理分开。异步任务由 .NET runtime 调度，Pipe 用于解耦生产与消费并提供分段 buffer 视图；HTTP/2/3 不能因此被简化为一条 HTTP/1 字节流上的相同会话。

**内存与生命周期。** Pipe 持有池化存储；AdvanceTo 报告已消费和已检查的位置，之后旧 buffer 视图可能失效；Complete 归还底层存储。GC 不保证池化 buffer 的借用安全。文档列出错误消费位置导致 hang/OOM、旧视图导致 use-after-free 语义等风险。

**架构代价。** 独立读写循环与消费坐标提升流水能力，也增加背压和生命周期规则。依赖 .NET runtime/GC，API 与 Zig 单 Reader 窗口并不等价。Talon 如果本来按需读，把 Pipe 式队列加到每个连接可能只增加 buffer 和调度成本。

**Talon 归纳。** 先维持单 reader 和明确窗口借用；确有跨任务生产/消费时使用有界 Pipe 并公开“消费后视图失效”的规则。借鉴两个 threshold 的迟滞思想，而不是把背压简化为固定大 buffer。显式区分 finish/complete 与最终释放，避免 deinit 偷偷执行对端等待。

## 12. libxev：Zig 原生不等于 std.Io backend

**官方事实。** 本轮读取动态 main [README](https://github.com/mitchellh/libxev/blob/main/README.md)。libxev 以 proactor/completion API 组织 I/O，区分高/低层接口，并宣称事件循环路径零运行时分配；调用方提供 Completion 存储。该声明不表示它上面的 HTTP 应用、用户 buffer、可选 thread pool 也无分配。

README 明确说明：当前 libxev 不实现 std.Io，也不以 std.Io 作为所有操作依赖；与 Zig 0.16 std.Io 惯用方式整合仍需要工作。因此“使用 Zig 写的循环”与“实现标准 I/O 契约”是两件事。

**资料边界。** 同一 README 开头泛称跨平台包含 Windows，features/roadmap 又写 Windows backend 计划中。这是资料内部不一致；本文不以此确认 Windows 可用，选型前必须核对选定 commit 的 backend 源码和实际 CI。不能把项目自称 stable 直接转为 Talon 平台验收结论。

**架构代价。** 用户需要安排 Completion 与 buffer 到操作结束的寿命；它不提供 HTTP、准入或 Talon 式结构化会话。换用 libxev 还需决定协程/任务层，不能视为替换一个 import 就替换 zio。

**Talon 归纳。** 显式存储、低分配和按平台能力选择值得学习。由于当前 zio 已有事件 backend、协程和 std.Io 接入，没证据支持另造组合 runtime。只有 embedding/callback 需求或栈成本实测支持时，才比较 libxev 作为独立执行模型。

## 13. 横向归纳：哪些规则可以跨语言成立

### 13.1 共同问题比接口外观更稳定

不同语言都要回答：输入 buffer 何时可复用；输出是否可能部分提交；关闭请求何时完成；CPU 任务如何不阻塞 I/O；排队和缓存如何有界；协议升级是否仍被监督。GC、RAII、borrow checker、引用计数、arena 分别解决部分问题，没有一种机制覆盖全部协议和并发契约。

| 设计规律                     | 多语言证据                                                       | Talon 应表达的模型                            |
| ---------------------------- | ---------------------------------------------------------------- | --------------------------------------------- |
| owner 与借用寿命必须清楚     | Nginx pool、Netty ByteBuf、fasthttp RequestCtx、Pipelines buffer | SessionLease、Request/MessageView、OwnedFrame |
| 运行环境与协议有不同变化原因 | Hyper rt、libuv/libxev 底层循环、Go runtime 与 net/http          | 显式 runtime 依赖，wire codec 不拥有线程池    |
| 背压必须覆盖排队与保留       | Netty 可写性、Pipelines thresholds、Pool/worker 准入             | active、waiters、queued bytes、idle 分别限额  |
| 建连阶段改变流表示           | Envoy filter/transport 分层、PROXY 规范                          | raw 前缀、TLS 和已建立 middleware 分阶段      |
| 关闭不能立即破坏在途借用     | libuv close callback、Rust future、托管异步 Complete             | cancel/join 完成后才能 free/window 归还       |
| 局部性优化会改变执行假设     | Nginx worker、Envoy 线程局部、Seastar shard                      | 单/多 executor 及分片模式显式选择             |

### 13.2 不建议机械移植

- Java 的 Channel/HandlerContext 层级不直接成为 Zig 类型层级；按被隐藏的知识评估每个边界。
- Rust 的 Future/Pin/trait stack 不直接成为 Zig 协程接口；Zig 自身没有相同的借用证明。
- Go 的 GC 和 goroutine 不能转换成无限 arena 与无限 spawn；许可和任务 scope 仍必须存在。
- C#的分段 Pipe 不是每个字节流都要有的层；单 reader 按需读可避免一条额外队列。
- Seastar 的每核分片不应成为默认线程政策；应用数据和限额需要一起建模。
- Nginx/Envoy 的产品控制平面不应进入 Talon 协议无关 core。
- libuv/libxev 的底层事件 API 不应泄露到每个 Proto；已有 runtime 负责这一变化边界。

## 14. 从调研到 Talon 设计决策

推荐的组合是：**zio 线性执行 + std 字节流接口 + 静态协议/stage 组合 + 明确 owner/borrow + 按作用域有界预算**。它在现有代码上有连续迁移路径，既不要求应用学习动态 pipeline，也不要求立刻重写底层 runtime。

标准化运行环境是值得验证的方向，因为 zio v0.18.0 已经提供 [std.Io接入](https://github.com/lalinsky/zio/blob/v0.18.0/README.md)。但 [Zig0.16发布说明](https://ziglang.org/download/0.16.0/release-notes.html) 明确区分成熟 Threaded 与实验性 Evented/Uring/Kqueue，不能预设所有 backend 已支持 Talon 的有限期限/取消/平台行为。迁移决策必须建立在 conformance suite，而不是语言偏好或 README 的支持标签。

核心不变量、API 草案、PROXY/TLS 预读模型、错误处理、资源矩阵、验证和迁移路线见 [主设计](network-engine-remodel.md)。本轮没有竞品压测、生产容量测量或实现变更；上文各项目的架构代价是用于选型的判断，不是漏洞审计结论。

## 15. 来源与版本范围

所有下列来源本轮读取成功；动态分支/版本族页面只作为 2026-10-09 的资料快照，后续选型需要固定 commit 和核对更新。教程未说明的功能不由本报告补推。

| 来源                                                                                                                                               | 版本范围                 | 支持的主要陈述                                           |
| -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ | -------------------------------------------------------- |
| [Nginx开发指南](https://nginx.org/en/docs/dev/development_guide.html)                                                                              | 官方动态指南             | process/event、pool、buffer、HTTP phases 与 finalization |
| [Envoy线程模型](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/intro/threading_model)                                             | latest 动态文档          | main/worker、dispatcher 与线程局部状态                   |
| [Envoy HTTP连接管理](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/http/http_connection_management)                              | latest 动态文档          | connection manager、codec 和 stream filter 边界          |
| [Netty ChannelPipeline](https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html)                                                            | 4.1 API 族               | 事件方向、handler 和 executor 顺序                       |
| [Netty ByteBuf](https://netty.io/4.1/api/io/netty/buffer/ByteBuf.html)                                                                             | 4.1 API 族               | 索引、共享视图、引用计数 API                             |
| [Tokio教程](https://tokio.rs/tokio/tutorial) / [runtime](https://docs.rs/tokio/1.47.1/tokio/runtime/index.html)                                    | 教程动态；runtime 1.47.1 | async 模型、单/多线程 scheduler                          |
| [Hyper README](https://github.com/hyperium/hyper/blob/v1.7.0/README.md) / [rt源码](https://github.com/hyperium/hyper/blob/v1.7.0/src/rt/mod.rs)    | v1.7.0                   | HTTP 定位及 runtime-independent 接口                     |
| [libuv设计](https://docs.libuv.org/en/v1.x/design.html)                                                                                            | v1.x 文档族              | loop、handle/request、thread pool 和 close               |
| [Seastar教程](https://github.com/scylladb/seastar/blob/master/doc/tutorial.md)                                                                     | master 动态源码          | shard、Future、reactor 和 CPU 让出                       |
| [Go net/http server](https://github.com/golang/go/blob/go1.26.0/src/net/http/server.go)                                                            | go1.26.0                 | goroutine 连接循环、Shutdown 与升级边界                  |
| [fasthttp README](https://github.com/valyala/fasthttp/blob/master/README.md) / [server](https://github.com/valyala/fasthttp/blob/master/server.go) | master 动态源码          | RequestCtx 寿命、API 差异、worker 复用                   |
| [Kestrel概述](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/servers/kestrel?view=aspnetcore-10.0)                                     | ASP.NET Core 10.0        | HTTP/TLS 服务器定位与能力条件                            |
| [System.IO.Pipelines](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines)                                                              | 动态.NET 文档            | consumed/examined、Complete、背压及误用                  |
| [libxev README](https://github.com/mitchellh/libxev/blob/main/README.md)                                                                           | main 动态源码            | completion、分配声明、std.Io 整合边界                    |
| [Zig发布说明](https://ziglang.org/download/0.16.0/release-notes.html)                                                                              | 0.16.0                   | std.Io、任务/取消、backend 成熟度                        |
| [zio README](https://github.com/lalinsky/zio/blob/v0.18.0/README.md)                                                                               | v0.18.0                  | ev/coro/runtime、迁移与 std.Io 接入                      |
| [PROXY协议规范](https://github.com/haproxy/haproxy/blob/master/doc/proxy-protocol.txt)                                                             | master 动态源码          | 连接开始前缀、有效完整 header 与可信转发边界             |

动态 README 出现范围冲突时，如 libxev 平台支持，以所选源码和运行验证为后续决策依据；本报告保留不确定性。没有以二手性能文章、星标数量或技术流行程度选定 Talon 新架构。
