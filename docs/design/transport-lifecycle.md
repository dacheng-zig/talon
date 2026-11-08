# 传输与连接生命周期

StreamServer 拥有连接准入、任务作用域、窗口和停机；协议只负责已建立连接上的会话。core 不依赖 HTTP。普通传输和固定地址传输共用此生命周期，包装后的 I/O、期限和关闭责任由一个连接对象协调。

## 协议与资源边界

Proto 必须实现 `serve(conn, app)`，可声明 `handshake`、`Limits`、`engineLimits`、`serveWithConfig`、`validateTransport`。HTTP 的 policy 转换为 core.Limits，通用协议直接使用 core.Limits。不增加只转发 engine 调用的 HTTP server 包装层，也不把头部、正文、arena 策略放进 core。

core 默认读写窗口各 4 KiB、最大连接数 65536、建立期限 10 秒、drain 期限 30 秒。窗口和连接容量在 init 固定，服务运行期间不能修改配置或移动 server。HTTP 的容量派生规则见 [HTTP 服务端](http-server.md)。

## 建立与包装

accept 前取得许可，accept 后立即进入被追踪的生命周期；随后在连接任务内包装传输、初始化窗口、执行中间件、可选握手和协议。adapter 不能在 accept loop 中阻塞全部新连接。建立期限从 accept 开始转换为一个绝对期限，包装、中间件、握手和 HTTP 首个请求头共享它，任何阶段不能续期。

`serveAdapted` 使用 adapter 的静态 `Connection(Raw)` 返回类型。优先调用 `wrapWithOptions(raw, WrapOptions)`，将 allocator、原始 deadline、有效读窗口传入；只提供 legacy `wrap(raw, allocator, deadline)` 的 adapter 仍可使用。成功后 Wrapped 拥有 raw；失败时 adapter 释放部分包装状态，引擎关闭尚未移交的 raw。adapter 配置必须活到 serve 返回。

普通 raw 提供 value Reader/Writer；pinned transport 提供稳定的 `ioReader/ioWriter`，连接不再租借替代窗口。完整 flush 包括所有下层 writer，不能只清空最外层明文缓冲。固定地址状态必须在首次暴露 I/O 前完成定位，之后不可复制所有权或移动。

有限读写期限由 timeout capabilities 明确声明，不支持时返回错误；`.none` 显式禁用。引擎、协议、adapter 在 accept 前检查各自需要的能力，不能静默忽略 setter。外部 timer 不能替代传输取消能力。

## 停机、结束与移交

shutdown 幂等地停止 accept，普通 HTTP 在请求边界退出；drain 到期取消剩余连接任务。关闭及窗口归还发生在任务收敛之后。应用创建的 sender/reader 子任务必须在协议返回前 join 或取消并等待，才能释放 Session、arena 和 transport。

可选 adapter.finish 只在链成功、未 detach、未受迫取消时调用。失败或部分输出不可通过再次 finish 重放；close 仍承担最终释放。正常协议返回不是远端应用已收到输出的证明。

`detachRaw()` 只转移 raw 的关闭责任，不转移任务、协议 arena 或池窗口。它检查建立状态、拒绝未消费的预读数据并完成完整 flush；任何失败均保留引擎所有权。成功后 raw 必须由调用方关闭。旧 hijack 入口具有同样的受检错误返回，调用方须添加 try。任务内 HTTP upgrade 不需要 raw detach。

## 出站连接

TcpDialer 拥有 DNS、候选地址尝试、可选本地绑定和 raw 移交。TcpEndpoint 仅含借用 host 和明确 port；HTTP Origin 拥有 scheme、默认端口和 Host。MemoryDialer 的目标为 void，backlog 等待同样受期限约束。选择这个边界，避免把 HTTP Connector 整体移入 core，也避免让各协议复制 socket 清理。

duration 在入口转成一次绝对期限；取消/超时立即终止，迟到成功先关闭再返回错误。DNS 至多 32 个候选按当前顺序尝试，没有 Happy Eyeballs 或 DNS 缓存。协程 timer 无法抢占不可取消的 resolver 系统调用。

`local_address=null` 委托 OS 分配；显式配置先 bind 再 connect，端口 0 由 OS 选取，不静默回退本地地址。family 不匹配、bind/connect 失败都关闭未交付 socket。

Linux 的 `tcp_user_timeout_ms: ?u31` 是独立的内核未确认/零窗口数据政策。null 不设置，0 恢复系统默认；非 Linux 的非 null 配置在 listen/DNS 前拒绝。listener 在 bind/listen 前设置以供 child 继承，dialer 在交付前设置；失败关闭 socket。它不等于应用总期限，也不保证关闭 fd 后立即释放所有内核资源。语义依据见 [tcp(7)](https://man7.org/linux/man-pages/man7/tcp.7.html)。

## 指标与验证

stats 用短锁给出一致快照，满足 `accepted = finished_tasks + active_tasks`；active 包含包装、协议、关闭和归还，不是 HTTP 请求或物理 socket 数。detached 只表示责任移交。错误按 adapter、buffer、连接返回分类，connection_errors 包含 EOF/reset；只观察直接返回的取消/握手超时，不能重分类包装后的 ReadFailed/WriteFailed。许可等待也不等于 backlog 中的客户端数量。

选择短锁而非独立字段 atomics，是为了保持快照守恒；仅在连接级边界更新。只有真实争用证据才支持更换统计策略。

验证入口：`transport_adapter_test`、`stream_server_test`、`timeout_capabilities_test`、`dial_test`、`tcp_user_timeout_test`、`server_stats_test`，通过公开边界覆盖失败、期限、移交、取消和资源归还。
