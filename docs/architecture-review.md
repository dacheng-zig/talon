# 本地架构变更审查

审查结论：保留 Talon 连接生命周期、资源控制、HTTP/WS 和 TLS 的核心实现及边界回归；清除计时专用生产接口、平台实验依赖和重复过程材料。规则由实际资源/协议所有者集中管理，没有为测量增加新的通用架构。

## 核心实现逐项判断

以下按本地变更文件核对责任、错误行为和相关回归。路径相对于 src。

| 文件 | 判断与理由 |
|---|---|
| core/allocation_budget.zig | 保留；allocator 统一预留/归还，增长失败回滚，覆盖实际 requested bytes |
| core/buffer_pool.zig | 保留；payload 分配/释放在缓存锁外，Debug 注册和 trim 所有权完整 |
| core/connection.zig | 保留；pinned I/O、完整 flush、期限能力、受检 detach 属于连接边界 |
| core/core.zig | 保留导出；删除尚无 datagram 消费者的泛化承诺 |
| core/dial.zig | 保留；DNS/候选/本地绑定和期限/关闭集中，不泄漏 HTTP scheme |
| core/framing.zig | 保留；分段不改变限额，拥有型大帧有显式 allocator 和终止失败规则 |
| core/limits.zig | 保留；只负责引擎容量、建立和 drain，不混入 HTTP policy |
| core/listener.zig | 保留；TCP/Memory 生命周期及 Linux TCP_USER_TIMEOUT 配置失败显式 |
| core/pipe.zig | 保留；closeRead 即使有缓冲也终止本端读取 |
| core/stream_server.zig | 保留；tracked adapter、绝对建立预算、finish/关闭、守恒指标共同属于任务生命周期 |
| core/tcp_user_timeout.zig | 保留；小型平台边界统一 OS 编码及不支持/失败，不提供泛用 option 框架 |
| core/timeout.zig | 保留；能力检查使有限期限不再静默失效 |
| http/client/client.zig | 保留请求头/正文预算、取消和复用；删除 sendTimings 转发 |
| http/client/connection.zig | 保留；响应 arena/decoder 所有权和完整 flush 位于稳定连接边界 |
| http/client/connector.zig | 保留；只映射 HTTP Origin 到 core dialer，不重复 socket 生命周期 |
| http/client/pool.zig | 保留；双维度准入、等待竞态、decoder/idle 保留和 trim 有单一所有者 |
| http/client/tls.zig | 保留；明文/TLS owner 按需分配，身份和握手共享建立期限 |
| http/client/tls_identity.zig | 保留；有界 DER IP SAN 验证与 DNS 政策分离 |
| http/client/vendor/tls_client.zig | 保留有限兼容副本；与安装的 Zig 0.16 原件完整比对，差异只在 import/IP 身份/SNI 分支 |
| http/client/vendor/README.md、LICENSE | 保留出处、SHA、许可和上游替换条件；修复设计引用 |
| http/codec/body.zig | 保留；chunk 声明在 payload 等待前执行剩余额度准入 |
| http/codec/head.zig | 保留；写错误显式传播，scanner 上限在读取时执行 |
| http/codec/ws.zig | 保留单 reader、共享写锁、准入和分片；移除阶段 stopwatch、timing 类型和计时锁包装 |
| http/http.zig | 保留；HTTP Limits/DataRate 和协议构造直接使用 engine，避免浅 server wrapper |
| http/server/limits.zig | 保留；头部窗口派生/HTTP policy 在协议内唯一解释 |
| http/server/protocol.zig | 保留；arena/HEAD 错误/期限/terminal output 在 HTTP 循环统一处理 |
| http/server/request.zig | 保留；101 的部分提交状态防止错误 fallback 和重复 accept |
| http/server/ws.zig | 保留共享 Session 接口，移除 SendTiming/SendTimings alias |
| talon.zig | 保留 core/TCP dialer 公开入口，与 HTTP 依赖方向一致 |
| tls_server.zig | 保留；可选 TLS provider 的 Context/State/BIO/期限/身份生命周期集中 |
| tls_openssl.c、tls_openssl.h | 保留必要 OpenSSL 宏/调用边界，不依赖系统内存探针 |
| examples/resp.zig | 保留；使用协议独立 limits/framing，验证 core 不需要 HTTP |

## 回归文件

功能性测试保留，因为它们断言实际协议与所有权；原始日志不是这些测试的替代品。

| 文件（tests/） | 核对的边界 |
|---|---|
| allocation_budget_test、budget_pressure_test | 共享分配/重复拒绝/恢复/已有连接进展；移除导出实验账本的打印和专用计时 |
| body_test、head_error_test | chunk 提前拒绝、未完成头的 HEAD 语义及流水线偏移 |
| dial_test、tcp_user_timeout_test | 出站期限、backlog 取消、本地绑定、Linux 继承与非 Linux 拒绝 |
| http_arena_budget_test | 局部/共享额度、保留、提交和升级作用域 |
| http_client_test | bodyless、协议切换、累计头、解码/收集、arena、复用和失败清理 |
| http_client_tls_test | pinned 合约、部分初始化/握手超时、明文 HTTP 和 HTTPS smoke |
| http_server_test、limits_test | 内存/TCP 请求、头/body/write 期限和保留政策 |
| idle_reclaim_test、pool_budget_test | idle 快照所有权、跨 origin 准入、取消授予竞争和等待期限 |
| owned_framing_test | 小窗口大帧、部分正文取消和 allocator 归零 |
| response_test、upgrade_test | 无正文、terminal output、101 前缀、recoverable validation |
| server_stats_test、stream_server_test | accepted/finished/active 守恒、许可背压、adapter/握手及移交 |
| sse_test | 流式响应继续占准入直到释放 |
| timeout_capabilities_test | 不支持有限政策时在 dial/accept/101 前拒绝 |
| transport_adapter_test | window hook、完整 flush、建立期限、OOM/取消、finish 和关闭恰好一次 |
| ws_client_test、ws_concurrency_test | 两端握手、任务/准入寿命、controls/业务写所有权和终止错误 |
| ws_fragment_send_test、ws_send_budget_test、ws_shutdown_test | 完整消息顺序、分片控制机会、额度、取消和 join 后释放；移除记录打印 |
| ws_send_timing_test | 删除；只验证已删除的测量 API |
| all.zig | 同步移除计时测试入口，继续引用功能性集成回归 |
| tls_probe.zig、fixtures/tls/*.ext | 保留；真实客户端 DNS/IP 身份、认证失败及资源归还 |
| tls_server_probe.zig | 保留真实服务端验收，删除 Darwin malloc size/window-cost 模式 |
| test_tls_server.py、test_tls_reader_window.py | 从 bench 移入 tests；保留协议断言，删除冻结/归档输出路径 |
| test_tls_client.py | 将保留证书模板接入真实 TLS 1.2/1.3 DNS/IPv4/IPv6 验收 |

## 构建、设计与 bench

build 保留默认 unit/integration、九个示例、纯编译 check、HTTP/TLS 负载、client probe 和可选 talon_tls/probe/test。删除 glibc、Darwin VM/stack/PCB、OpenSSL heap、Linux cgroup/slab、WS 进展矩阵等实验构建分支，不让默认检查链接平台观察代码。build.zig.zon 保留 bench 包路径，因为保留的负载仍使用它。

140 份增量 design 合并为 [7 个责任主题](talon-architecture.md#设计索引)。保留协议/资源契约、替代方案和重审条件；删除“仅新增测量”的设计稿及依赖 frozen capture 的验收结论。用户/开发/架构指南同步修复接口现状，尤其是已实现的服务端 ALPN/SNI 与不再适用的路线图。

bench 只保留 http_network、arrival、fixture：闭环复用/cold、有界到达、idle trim 对照直接检查 Talon Client 的现有能力。剥离拨号观察、平台 PCB、恢复时间线和 syscall 插桩，不另建基准框架。docs/benchmarks 的约 1.9 万个文件、约 19.2 GB 原始日志/密文/归档/源码副本删除；历史数字不再作为当前实现的验收证据。session-working-prompt 的无限实验/冻结指令是过程产物，删除。

## 取舍与验证边界

保持现有深模块，局部删去实验接口，比重新拆分 Pool/Session/TLS 更容易维持已有失败语义。统一队列优于串联 semaphore，allocator decorator 优于协议手工记账，显式 idle trim 优于 OOM callback，静态 adapter 优于 HTTP 内部 TLS；具体比较见各 design。

本次 API 删除限于未发布的 WS 测量接口；核心 limits/timeout/detach 的行为变化和迁移要求见设计/使用指南。验证结果见 [当前记录](benchmarks/validation.md)。没有从短时测试推断长期无泄漏、严格公平、控制优先级或远端交付，也没有完成 Linux 运行矩阵。
