# HTTP/1.1 服务端边界

Http1Protocol 拥有 HTTP 限额、请求 arena、解析、响应提交和升级状态。StreamServer 只执行连接生命周期；HTTP Server 是配置好的引擎类型，避免增加浅转发层。

## 限额与期限

默认头部限额 16 KiB，读窗口由 `max(max_header_size, 1024) + 1024` 派生（默认 17 KiB），显式窗口必须大于头部上限，以便 scanner 观察超限。写窗口默认 4 KiB；pinned adapter 同样接收有效窗口。容量验证发生在启动前，协议入口仍验证实际 reader 大小。

首个头受 accept 建立期限及 header_read_timeout 的较早者限制；后续请求先用 keep_alive_timeout 等待，再应用一次绝对头期限，碎片不能续期。正文速率默认 240 bytes/s、5 秒 grace，只累计网络等待；payload 提供时间额度，chunk 元数据不提供额度，handler 工作和输出背压不计入。write_timeout 限制写入无进展等待。

max_body_size 默认 16 MiB，chunk 声明若超过剩余额度，在等 payload 前拒绝。wire 上限与 arena 分配/保留上限独立，见 [资源所有权](resource-ownership.md)。

## 请求循环与输出提交

先固定完整请求头到 arena，Request 的借用活到本次 handler/升级作用域结束。流水线仍有输入时允许缓冲输出；开始阻塞读取前 flush，避免 peer 等不到响应。关闭响应不必 drain 被拒绝的上传；keep-alive 复用前必须消费完整正文。

错误响应也遵守 HEAD 无正文语义。完整解析前只识别严格 `HEAD ` 前缀用于 suppress_body，不把该检查当作请求语法验证。204/205/304 不发正文，1xx 不能作为普通最终响应。

写入或 flush 失败可能已提交前缀，必须保留 terminal error，不能继续 flush、追加 fallback 或再解析下一请求。未提交的 handler 错误可以发错误响应；已提交或已升级的错误结束连接。

## 升级

Upgrade.accept 在输出前检查 AlreadyUpgraded 和期限能力，encoder 先验证所有头。InvalidHeader 或准备失败仍可修正；开始输出后的失败可能留下 101 前缀，必须标记 taken。成功输出也在 flush 前标记，防止失败后恢复 HTTP。只有 accept 成功才能使用升级协议。

taken 表示 HTTP 输出责任已经结束，不表示远端确认握手。升级会话仍位于原连接任务内，共享 arena 和底层 I/O，应用负责结束子任务。无需 raw detach；移交底层关闭责任另见 [传输生命周期](transport-lifecycle.md)。

把提交状态完全交给 handler 最省内部状态，却要求每个协议重建“部分 101”规则；选择 Upgrade/Response 在实际输出边界拥有状态。没有自动升级会话注册或通用关闭框架。

验证：body、head_error、response、limits、http_server、upgrade、http_arena_budget 测试，包含 101 部分输出、终止错误、流水线 HEAD、chunk 提前拒绝、限额和取消。
