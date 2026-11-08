# HTTP 客户端准入、存储与复用

Client 拥有逻辑请求政策，Pool 拥有统一准入和空闲缓存，ClientConnection 拥有响应解析、arena 和解码器。Response/SSE/WebSocket 继续占用连接预留到释放，收到头部不是所有权结束。

## 重定向与 SSE 状态

303 保持 HEAD，其余改用 GET；301/302 只将 POST 改为 GET。改写时丢弃正文及 Content-Type/Encoding/Language/Location，跨 origin 默认移除凭据。origin 比较先于释放上一跳 host 存储；每跳从当前头集合过滤，后续跳转不得恢复已删除字段，分配新集合成功后才释放旧集合。307/308 仅重放可重放 body，流式上传不自动重放。

SSE 的 id 是跨事件 sticky 状态，event 名称只属于当前块。decoder 将 id 放在 scratch 尾部，event 紧邻其前，使字段替换及事件 reset 后仍能按连续空间限额写 data。SseSource 在断线时保存 id-only 块更新，空 id 清除自动 Last-Event-ID；相同 id 不替换仍被 decoder 借用的存储。复制失败也关闭响应并归还许可。

## 统一准入

Pool 在一个锁内同时判断全局及 origin 容量。默认 max_active=256、max_waiters=256、max_per_origin=null、pool_wait=10 秒；null 禁用对应数量限额，active=0 非法，waiters=0 满载时立即拒绝。idle 由另行配置的数量限制，不能把 active 当总 fd 上限。

| 方案 | 取舍 |
|---|---|
| 串联全局与 origin semaphore | 部分获取占住另一维度，取消回滚及等待统计散落 |
| 池内统一预留、有界队列 | 采用；一次授予两维度，失败/取消由单一所有者回滚 |

等待节点在协程栈上，origin 引用覆盖等待、使用和归还。授予先增加计数再出队/set 事件；取消与授予竞争时，重新取锁移除节点或归还名额。队列可跳过饱和 origin，不承诺严格跨 origin FIFO。dial/create/复用失败也归还预留。长 key 即使不池化仍受准入控制。

等待期限与逻辑请求 absolute total 取较早者，跨重试和重定向不续期。过期 total 在 checkout 前拒绝；零等待仍允许立即取得容量。Client/Pool 在开始使用后保持稳定地址，deinit 前结束全部请求、响应、会话及后台 reaper。

## 头部与复用

max_response_head_bytes 默认 64 KiB，累计同一逻辑请求的全部 1xx、最终头、重试及自动重定向；0 在 checkout 前拒绝，null 只关闭累计限额，单头 15 KiB 上限仍在。先收紧 scanner 到剩余额度，完整头在解析前扣减；换连接不能重置额度。超限关闭且不重试已经收到响应字节的请求。

1xx 在 reader 缓冲中验证并丢弃，仅最终头固定到 arena。101 是最终头并保留预读协议数据。Response.header/reason 只借用到 deinit；池在 checkin 才应用 max_retained_response_arena（默认 4 KiB，0 全释放，null 保留高水位），收缩失败 free_all。

HEAD/304 的 Content-Length 是资源元数据，不占正文额度，不读取正文或分配解码器。普通请求 keep_alive=false、101、CONNECT 2xx 或失败正文不能回 HTTP 池；专用 WebSocket 继续持有协议连接。失败 CONNECT/未接受升级仍按合法 HTTP 复用规则处理。

## 正文与解码工作区

readAllAlloc/json 共用有界解码后收集，max_decoded 同时限制输出长度和收集容量。分块读取、几何增长前截断容量，达到上限后只探测一字节以区别精确 EOF 和超限；不计算可能溢出的 max+1。读/分配失败 abort response，deinit 跳过不可信 remainder 的 drain。流式 bodyReader 由消费者设置输出预算。

flate Reader 使用固定 64 KiB 历史窗口，zstd 使用独立固定窗口；零历史模式要求输出保留全部历史，不适合小块 Reader 消费。Pool 的 max_decoder_bytes 默认 64 MiB，max_idle_decoder_bytes 默认 1 MiB，覆盖 decoder 对象与窗口。先预留再分配，失败完整回滚；超限返回 DecoderBudgetExceeded。checkin 空闲额度不足释放 decoder，仍可复用健康传输。

总 decoder 预算与 idle 子预算在 init 固定，改变后 checkout 返回 ResourceLimitsChanged。锁仅保护预留，不包裹分配/free/I/O。stats 分别报告 active、waiting、decoder_bytes、idle_decoder_bytes 和 idle_response_arena_bytes；它们不是总连接内存或 RSS。

相比每响应全部释放 decoder，有限共享总预算加有限 idle 保留同时约束并发分配并保留小解码器复用；没有另一条分配等待队列或自动逐出再重试。数量、wire、decoded 和 allocator 预算独立。

## 验证与迁移

默认全局准入和解码器缓存由无界变为有限。需要更大容量时显式配置，不能仅提高 max_active 就认为解码工作区足够。Response.deinit 后旧头切片可能已释放；有限 timeout setter 要加 try，禁用必须显式配置。

pool_budget/http_client/idle_reclaim/timeout_capabilities/http_client_tls 测试覆盖取消-授予竞态、跨 origin、初始化 OOM、warm 复用、解压放大、停止 drain 和最终预算归还。网络负载方法见 [bench](../benchmarks/README.md)。
