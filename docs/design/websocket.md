# WebSocket 会话与发送所有权

共享 Session 拥有两端的线协议验证、单 reader、帧输出串行化和发送准入。业务调用同步借用 payload，没有后台队列；任务及 payload 的寿命仍归应用。

## 并发与失败

一个 read 可与多个发送者并行；第二个 read 在消费输入前返回 ConcurrentRead。业务帧、自动 pong、主动 ping、close 共用帧锁，锁覆盖 header/payload/flush，不能绕过 Session 写底层 Writer。Session 操作期间保持稳定地址，配置和 payload 不可并发修改。

参数错误在输出前拒绝：文本 UTF-8、控制 payload≤125B、合法 close code、原因 UTF-8 且≤123B；原因不截断。服务端握手验证 method/version/upgrade/key，客户端校验 challenge，不接受无效协议切换。

入站扩展长度必须使用最短编码（16 位表示至少 126B，64 位表示至少 65536B，最高位为 0）；数据长度先检查消息剩余额度，再转换到 usize，不让超大声明触发窄平台整数转换。

等待写锁取消不输出；开始输出后的失败可能留下半帧，永久终止后续写入。成功 close 后 data/ping 返回 Closed，read 仍可等待 peer close 并自动答 pong；close 幂等，不能重试失败输出。

## 同步数据准入

SendLimits 的 max_data_sends/max_data_bytes 可选，null 禁用；0 数量拒绝所有业务发送，0 字节仍允许空消息。预留包含等待锁和正在发送的完整 payload，每个调用独立计费，即使共享同一 slice；不是 allocator、任务栈或物理内存统计。

先验证参数、检查取消、一次预留两维度；SendLimitExceeded 无输出，不挂起或改变流状态。所有返回路径归还额度，成功到完整 flush 才归还。controls 不占 data 额度，但仍竞争帧锁，没有优先保证或控制任务上限。sendStats 给出一致在途/峰值/拒绝快照；峰值也包含随后取消的调用。

比较拥有型队列和专职 writer，后者可支持调用返回后的异步发送，但需要复制/转移 payload、队列取消和 close 作用域；当前选择同步准入，集中错误回收，保持既有背压。只有真实异步消费者才支持增加队列。生产接口不包含锁等待/输出阶段计时；测量应在 bench 边界完成。

## 可选出站分片

SendOptions.max_frame_payload=null 默认单帧，正数启用，0 在副作用前拒绝。启用后消息锁保持不同业务消息不交错；每个完整分片单独持有帧锁并 flush，非最终帧之后 yield，给 controls 调度机会。首帧 text/binary，后续 continuation，只有最后 FIN；空消息仍一帧。客户端每帧生成随机 mask，文本按完整消息验证，允许分片切开 UTF-8。

非最终帧已发出后的取消会留下不完整消息，在释放消息所有权前以不可取消帧锁设置 terminal write state；不能重试。close 可在完整分片边界合法中止消息，剩余调用返回 Closed，不再输出 continuation，仍允许自动 pong。

更小分片增加帧头、masking、flush 和 yield。zio mutex 可 barging，yield 不提供 FIFO、公平或墙钟控制延迟保证。依据见 [RFC 6455 §5.4](https://www.rfc-editor.org/rfc/rfc6455#section-5.4)。默认单帧可以直接回退，不需要替换会话架构。

## 停机与验证

Server.shutdown 不自动注册或通知 WS。应用可用自己的 stop 事件发送 going_away 并等 peer；drain 超时会取消 handler。返回/deinit 前必须取消并 join 所有 sender/reader 子任务，随后才销毁 Session、请求 arena 和 payload。

选择应用作用域而非自动升级会话 registry，避免引入协议关闭路由、借用注册和解绑竞态；现有具体需求由 Group 和 engine drain 覆盖。应用确实无法协调异协议停机时再重审。

ws_concurrency/ws_send_budget/ws_fragment_send/ws_shutdown 覆盖两角色、背压、取消、部分输出、完整消息顺序、controls 穿插和任务/额度归零；可选 TLS 验收检查真实 WS over TLS。有限测试不证明长期无饥饿、控制优先级或 TCP 交付保证。
