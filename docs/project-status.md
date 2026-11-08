# Talon 项目现状与生产级差距

评估日期：2026-10-09。对象是当前 `main` 工作树（含已有暂存、未暂存和未跟踪内容），不是已发布版本；包版本为 `0.1.0`，依赖为 Zig 0.16.0 / zio v0.18.0。结论来自源码、文档和本机回归，不能替代独立安全审计或目标环境验收。

**Talon 已是一套具有资源控制和失败清理能力的网络库，超出了演示原型阶段；但目前证据不足以称为通用生产级网络引擎。** 可以用于受控场景接入和验证。生产发布的主要缺口是持续验证、协议互操作、目标负载下的容量与长稳证据，以及发布和运维契约，而不是再增加一批协议或框架功能。

## 项目目标与边界

项目目标是用 zio 的协程、取消、期限和结构化并发，提供可组合的流式连接引擎，并在其上实现 HTTP/1.1 服务端与客户端。调用方可以只使用 core 编写自定义 TCP 协议，也可以直接使用 HTTP、WebSocket 和 SSE。

- `talon.core` 管理连接准入、任务、读写、传输包装、连接中间件、关闭、缓冲池、分配预算与拆帧。
- `talon.http.codec` 管理双向线协议；server 管理 HTTP policy、请求作用域和响应提交；client 管理请求政策、池化、响应存储与解码。
- 可选 `talon_tls` 使用 OpenSSL 提供服务端 TLS；客户端 TLS 使用固定 Zig std 兼容源码。
- 项目是库，不是完整 Web 框架或可直接部署的业务服务。路由、认证授权、业务模型、健康检查、进程管理和监控接入由使用它的应用决定。

依据：[公开入口](../src/talon.zig)、[HTTP 入口](../src/http/http.zig)、[架构](talon-architecture.md)、[RESP 示例](../examples/resp.zig)。资源和协议政策由各自所有者解释的方向是合理的；本轮没有证据支持重新拆分核心架构。

## 已实现能力

“已实现”表示存在可调用实现和相关回归，不表示所有部署方式都已得到生产验证。

| 能力                 | 状态与实现边界                                                                                                                                                             | 源码及验证入口                                                                                                                                                                          |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 流式服务引擎         | 已实现；comptime Proto/Listener/adapter 组合、accept 前许可背压、任务追踪、shutdown/drain、连接生命周期统计                                                                | [stream_server](../src/core/stream_server.zig)、stream_server/server_stats/transport_adapter 测试                                                                                       |
| TCP / 内存传输与拨号 | 已实现；TCP/Memory listener、dialer、单次绝对建立期限、候选地址顺序尝试、本地绑定、Linux TCP_USER_TIMEOUT                                                                  | [listener](../src/core/listener.zig)、[dial](../src/core/dial.zig)、dial/tcp_user_timeout 测试                                                                                          |
| 连接与扩展边界       | 已实现；value/pinned I/O、完整 flush、期限能力拒绝、受检 raw 关闭责任移交、连接中间件、PROXY v2 与连接日志                                                                 | [connection](../src/core/connection.zig)、[middleware](../src/core/middleware.zig)、timeout_capabilities/upgrade 测试                                                                   |
| 内存与拆帧           | 已实现；缓冲复用、Debug 借出追踪、共享 requested-bytes allocator 预算、HTTP arena/decoder 保留控制、显式 idle trim、借用/拥有型拆帧                                        | [allocation_budget](../src/core/allocation_budget.zig)、[buffer_pool](../src/core/buffer_pool.zig)、[framing](../src/core/framing.zig)、budget_pressure/owned_framing/idle_reclaim 测试 |
| HTTP 服务端          | 已实现受限 HTTP/1.x 子集；keep-alive/pipeline、定长/chunked、HEAD、100 Continue、CL/TE 走私防御、头/body/写入期限、响应与 101 提交状态                                     | [protocol](../src/http/server/protocol.zig)、[request_parser](../src/http/codec/request_parser.zig)、http_server/body/head_error/response/limits 测试                                   |
| HTTP 客户端          | 已实现；长寿命连接池、全局及 origin 准入、有界等待、idle/lifetime 回收、重定向、有限幂等重试、阶段及逻辑请求期限、流式上传/响应、gzip/deflate/zstd、有界收集与 JSON 所有权 | [client](../src/http/client/client.zig)、[pool](../src/http/client/pool.zig)、[client connection](../src/http/client/connection.zig)、http_client/pool_budget 测试                      |
| HTTPS 客户端         | 已实现；系统 CA/显式测试策略、DNS 身份及 SNI、二进制 IP SAN 身份且不发送 IP SNI、固定地址 TLS 状态、失败回滚                                                               | [tls](../src/http/client/tls.zig)、[tls_identity](../src/http/client/tls_identity.zig)、http_client_tls 测试及 [本地证书验收](../tests/test_tls_client.py)                              |
| TLS 服务端           | 可选实现；TLS 1.2/1.3、memory BIO、ALPN 协商、静态 SNI 身份及恢复隔离、有效读窗口、可选本端 close_notify                                                                   | [tls_server](../src/tls_server.zig)、OpenSSL 单元及 [真实服务端验收](../tests/test_tls_server.py)；默认构建不链接 OpenSSL                                                               |
| WebSocket 两端       | 已实现；握手、mask、片段重组、UTF-8、控制帧、单 reader 与串行帧输出、业务发送额度、可选出站分片、取消后 terminal 状态                                                      | [ws codec](../src/http/codec/ws.zig)、ws/ws_client/ws_concurrency/ws_send_budget/ws_fragment_send/ws_shutdown 测试                                                                      |
| SSE 两端             | 已实现；服务端 chunked 事件流、增量解码、sticky ID、重连/Last-Event-ID/retry、204 停止；应用仍负责心跳、任务取消和事件大小预算                                             | [sse codec](../src/http/codec/sse.zig)、[SseSource](../src/http/client/client.zig)、[SSE 集成](../tests/sse_test.zig)                                                                   |
| 开发与测量工具       | 已实现；默认单元/集成/确定性变异测试、只编译 check、九个示例、真实 TLS probes、独立 TCP/TLS 客户端负载与有界到达模型                                                       | [build](../build.zig)、[tests/all](../tests/all.zig)、[bench 方法](benchmarks/README.md)                                                                                                |

## 已实现但不可扩大解读的承诺

- `max_connections`、pool active/waiting、wire 大小、解码后收集、allocator 预算和 idle 保留是不同约束。AllocationBudget 不覆盖独立运行时栈、allocator 余量、OpenSSL 内部分配或内核 socket；默认请求 arena 峰值预算未开启。它不是全进程内存硬上限。
- 同步 payload 借用、稳定对象地址、先取消/join 子任务再释放 Session/arena，是调用方必须遵守的契约。受检 detach 只移交 raw 的关闭责任，不移交任务、池缓冲或许可。
- HTTP 解析对 framing 和字符注入采取严格策略，但并非全部 RFC 行为。chunk extension/trailer 被明确拒绝；HTTP/1.0 服务端不保持连接。不能把“走私防御已有回归”写成“完整 HTTP 合规或安全审计通过”。
- CookieJar 是可选 host-only 子集，忽略 Domain/Path/SameSite，且没有条目数量/总字节配置上限。它不适合直接承担完整浏览器 Cookie 行为或复杂登录会话。
- Server.shutdown 不自动给所有 WebSocket 发送 close。分片给 controls 调度机会，不提供控制优先级、FIFO、公平性或固定延迟保证。SSE 默认关闭 read/total 期限，静默流需要应用心跳和取消策略。
- TCP 拨号至多使用 32 个 DNS 候选，顺序尝试，没有 Happy Eyeballs/DNS 缓存；不可取消的 resolver 调用不能被协程 timer 抢占。
- OpenSSL 服务端不提供 mTLS、证书热更新或自动 ALPN 协议分派；未知 SNI 回退默认身份。TLS 证书选择不等于 HTTP Host 授权。

以上边界详见 [使用指南](user-guide.md) 和 [设计索引](talon-architecture.md#设计索引)。

## 本轮必要修正

1. **HTTP 重定向语义与所有权**：303 保持 HEAD，其余需要改写的请求使用 GET；丢弃 body 时同时移除 Content-Type/Encoding/Language/Location。后续跨 origin 重定向从当前头集合继续过滤，避免重新引入已删除字段；origin 比较在释放上一跳 host 存储之前执行，消除悬空读取及同 origin 凭据误删。307/308 保留可重放正文，跨 origin 的凭据过滤继续生效。
2. **请求头提交边界**：User-Agent、Accept、Accept-Encoding、Upgrade 在任何输出前完成校验。原实现返回 InvalidRequestField 时可能已经写出请求行和 Host；现回归要求既有 Writer 内容完全不变。
3. **WebSocket 帧长度**：拒绝以 16/64 位扩展长度表示较短 payload，遵循 RFC 6455 §5.2 的最短编码规则；在把数据长度转换成 usize 前检查消息预算。
4. **SSE 元数据与续传**：ID 固定保存在 scratch 尾部，event 紧邻其前，字段替换回收旧空间，避免后续 data 覆盖 sticky ID。断线时保存 metadata-only ID，空 ID 清除自动续传头；保持同一 ID 时不释放仍被 decoder 借用的存储。复制失败也关闭当前响应、归还连接许可。
5. **文档准确性**：修正源码中过时的“total deadline 待实现”“服务端 TLS 未提供”等描述；用户指南降低完整 RFC 合规的过度表述，补充重定向、Cookie 子集和本报告入口。

新增回归覆盖 Writer 无副作用、303 HEAD、POST 的 301/302/303、PUT 的 307/308、多跳 body/凭据头过滤、WS 长度临界值、SSE 小 scratch/字段替换/metadata-only ID/空 ID/跨连接连续事件。

## 距离生产级的工作

以下是基于当前证据的发布和验证工作，优先级表示风险先后，不是对尚未确定的产品需求作承诺。P0 为目标环境上线前应完成，P1 为首次稳定发布重点，P2 根据实际消费者决定。

| 优先级 | 现状、风险与证据                                                                                                                                                                                  | 下一步及完成依据                                                                                                                                                                                                                                        |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P0     | 仓库没有 CI 工作流；本轮只在 Darwin arm64 运行。Linux 的 TCP_USER_TIMEOUT 测试在本机跳过，不能证明生产 Linux 内核行为                                                                             | 建立声明支持的平台原生 CI，固定 Zig/zio；执行 Debug、ReleaseSafe、示例、可选 TLS 和 Python 验收。未构建 probe 的 skip 不能算 TLS 通过；Linux 专项应实际执行                                                                                             |
| P0     | 尚无目标工作负载的生产容量与长稳证据；默认 max_connections=65536，请求 arena 峰值不限。有限客户端 bench 不证明服务端容量                                                                          | 按真实 handler、TLS、上传、WS/SSE 比例配置容量/预算/栈；在目标系统测试稳态、过载、慢客户端、反复连接/取消/停机和恢复。观察 RSS/FD/栈/预算/延迟/错误，确认资源收敛                                                                                       |
| P0     | 私有 std TLS 兼容副本扩大升级维护面；本机 OpenSSL 4.0.3 的成功不代表其他受支持版本                                                                                                                | 将完整 vendor diff、上游出处/哈希、CA/DNS/IP 正反例接入升级流程；为计划支持的 OpenSSL/系统组合运行真实握手和关闭验收。生产客户端使用 system 验证                                                                                                        |
| P1     | 已确认协议覆盖缺口：Host 仅检查存在/重复和通用字段字符，未验证完整 authority 语法；未知 Expect 被忽略（需说明政策）；Location 只实现基础相对合并，未完整处理 network-path/query-only/dot-segments | 明确支持子集与失败政策，补独立客户端/服务器/代理的正反例和分段回归；修正 Host/URI 引用行为并记录 Expect 政策，避免继续宣称完整 RFC 合规。证据：[request_parser](../src/http/codec/request_parser.zig)、[resolveLocation](../src/http/client/client.zig) |
| P1     | CookieJar 无容量上限、忽略 Path 等语义；流式 bodyReader 没有自动解码后输出限额；自动重定向也不提供业务目的地址 allowlist                                                                          | 不可信工作负载使用有界收集或显式流式输出预算；Cookie 子集不满足需求时替换/扩展并加额度；需要 SSRF 防护的应用限制目标和重定向。以对应应用威胁模型与拒绝/恢复用例验收                                                                                     |
| P1     | 现有 stats 覆盖连接任务、池与预算，不提供完整 HTTP 请求错误分类、请求延迟、trace/exporter。若干读/解析失败由协议静默关闭                                                                          | 由消费者接入请求级计量、结构化错误和监控导出；明确过载、超时、TLS 失败、forced drain 的告警与排障路径。Exporter 不必嵌入 core；避免日志泄漏凭据或以正常断连刷屏                                                                                         |
| P1     | 没有仓库级 LICENSE、SECURITY、变更日志或稳定 API 兼容声明；vendor MIT 仅覆盖该第三方源码。当前工作树大量改动尚未形成发布版本                                                                      | 由所有者决定项目许可；建立版本/API 迁移、漏洞报告和依赖更新约定；形成可复现的包与消费方 smoke。不能代替所有者选许可或把工作树当已发布保证                                                                                                               |
| P1     | 缺应用部署和停机实证；库只管理连接任务，不管理进程信号、健康探针、应用后台任务和证书更新                                                                                                          | 为一个真实消费者完成 SIGTERM/readiness/stop-accept/drain/WS close/SSE stop 的顺序，部署预算与 FD 限额，并演练回滚。若 TLS 在代理终止，明确 PROXY 来源信任边界；不要求网络库变成部署平台                                                                 |
| P2     | 池/统计使用自旋锁，部分元数据增长或 Cookie 分配在锁内；无严格公平或 controls 延迟承诺。现有变异测试不是持续覆盖引导 fuzz/互操作认证                                                               | 先按多 executor 实际争用与 p95/p99 定位，再决定锁/缓存优化；补独立协议互操作和持续 fuzz，验证合法与非法输入语义，不能只以“不崩溃”证明合规                                                                                                               |

性能、安全、完整性和支持平台没有统一“完成百分比”；尚未明确目标消费者和容量指标，不能据代码量推算距离生产的比例或工期。

## 不自动列为生产阻塞的扩展

HTTP/2/3、UDP/datagram、UnixListener、路由/参数提取、sendFile、SO_REUSEPORT、多 size class 缓冲、动态 TLS/mTLS、Happy Eyeballs、完整 CookieJar 当前未提供。只要目标场景不依赖它们，就无需为了“生产级”一次性全部实现；若实际消费者需要，则单独形成协议、资源和兼容验收。

## 推荐推进顺序

1. 固化本轮协议回归与现状文档，建立目标平台 CI，补 Linux 与所需 OpenSSL 原生运行证据。
2. 收敛 HTTP/URI/Cookie 支持契约和互操作用例，同时建立 TLS/vendor 与漏洞维护流程、许可和 API 发布约定。
3. 选择一个真实消费者，按其容量和失败场景验证预算、长稳、过载恢复、可观测性和部署停机；形成具体可支持的运行范围。
4. 依据该范围内的瓶颈和需求决定后续性能优化或协议扩展。达标范围之外仍保留明确限制。

## 验证证据

本轮命令、通过数与跳过原因见 [验证记录的项目现状审查部分](benchmarks/validation.md#项目现状审查后的验证)。之前的短网络负载记录仍属于其原有验证范围；本轮没有重新测量吞吐、Linux 运行或长期驻留。
