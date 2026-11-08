# TLS 身份与传输适配

客户端身份政策属于 TlsConnector，服务端 TLS 属于可选 talon_tls adapter。core 不解释证书、ALPN 或 SNI，HTTP 不持有 OpenSSL 状态。

## 客户端身份

DNS reference identity 来自 Origin.host，匹配证书名称并发送 SNI；IP literal 只按 iPAddress SAN 的 4/16 字节匹配，不发送 SNI，不接受数字 dNSName、CN 或解析后地址替代原目标。匹配后仍执行所选 CA 与握手认证。语义依据为 [RFC 9525](https://www.rfc-editor.org/rfc/rfc9525.html)。

Zig 0.16 std TLS Client 没有 IP 身份 hook。拒绝对 IP 关闭验证或改用另一 DNS 名；选择有出处、MIT 许可和有限补丁的固定 std 兼容副本，避免引入另一客户端 TLS provider。上游 SHA 和修改范围见 [vendor README](../../src/http/client/vendor/README.md)。record/密钥推导/CA/握手认证均保留上游；升级编译器必须完整比较，std 提供等价 hook 后回退而不改变 Connector API。

IP verifier 完整扫描有界 DER SAN，拒绝截断、非规范长度、错误 tag/长度，即使前面已经匹配也不接受损坏尾部。IPv6 URL 去括号再比较字节，HTTP Host 的括号由 HTTP 层补回。当前 literal parser 不支持 scope/zone 和 IPv4 十进制尾段的 IPv6 文本，不将其描述为已覆盖能力。

system 模式使用 RootStore（通常加载 OS 信任库）；self_signed 校验身份/自身签名/有效期但没有 CA 授权，insecure_no_verification 显式禁用两类验证。TCP 与握手共享一个建立期限，失败释放 socket/pinned owner。明文模式只分配 raw State，HTTPS 用一个更大的 owner 包含 TLS 状态/窗口，避免 HTTP 承担 TLS-only 存储。

## 可选服务端

`-Dtls-server=true` 用 pkg-config 链接 OpenSSL 并导出 talon_tls；默认构建纯 Zig。Context 启动时加载 chain/key 和静态配置，活到全部连接结束。支持 TCP TLS 1.2/1.3，没有 mTLS、证书热更新或自动 ALPN 协议分派。

socket BIO 会绕过 zio I/O/期限，重写 TLS 又扩大密码学维护责任；采用 memory BIO，让 OpenSSL 负责 TLS，adapter 负责 raw I/O、所有权和期限。

State 固定地址，拥有 raw、SSL/BIO 和独立明文窗口。wrap 失败释放部分包装而让 engine 关闭 raw；成功后 close 唯一释放所有状态。WrapOptions 的有效 engine 窗口决定明文 read slice（HTTP 默认 17 KiB），密文窗口独立；legacy wrap 明文默认 32 KiB。大头配置不需要另一份 TLS 容量政策。

## OpenSSL 调用与输出

一个 reader 拥有 raw 输入；SSL 操作及密文输出按协程 mutex 串行。SSL_get_error 紧跟原操作且其间不挂起/不插入其他 SSL 调用；错误捕获后再执行 zio I/O。raw receive 时释放 mutex，允许 sender 推进，收到字节后重新加锁喂 BIO。partial raw record 在 idle timeout 后保留，仅 raw Timeout 可恢复，TLS 错误、取消和异常 EOF 不可重试。

每次写至多 16 KiB 明文并完整 flush；writer 不隐式读取输入。直接借用 output memory BIO 的密文，在整个异步 raw write/flush 中保持锁，raw writer 无缓冲；成功后才 reset。借用未转移所有权，失败后永久禁止再发送旧密文。相比栈 drain 数组，借用避免固定栈副本，但 reset 的清零有成本，不声称吞吐改善。使用公开 [BIO_s_mem](https://docs.openssl.org/3.5/man3/BIO_s_mem/) 语义，不依赖内部 read pointer。

## 关闭

shutdown 唤醒 raw I/O；close 无 I/O，不隐式等待 alert。Adapter.close_notify_timeout 默认 null；启用后只在正常成功、未 detach、未受迫取消的 finish 中发送本端 close_notify，使用结束期限和已有写期限较早者，不等 peer。忙锁、fatal 状态或已过期期限拒绝。自定义协议可显式 closeNotify，但须限制写期限并先收敛使用者。

本端 alert/flush 不保证完整双向 TLS close 或应用交付。默认关闭可能为裸 EOF，客户端允许 truncation 必须依赖 HTTP framing 判断正文完整性。OpenSSL 内部分配不计入 Zig AllocationBudget，也不承诺全握手内存/RSS 上限。

## ALPN 与静态身份

Context 复制 ALPN bytes，按服务端顺序选择交集；有 offer 无交集拒绝，未发送 ALPN 保留原行为。标识 1..255B，总编码≤65535B。HTTP/1.1 只配置实际支持的协议，不因 offer h2 而获得 HTTP/2。negotiatedProtocol 返回借用至 transport.close。

NamedIdentity 的 server_name 为严格 ASCII DNS 名，大小写无关精确匹配；拒绝重复、通配符、尾点、空标签、Unicode。未知/缺少 SNI 回退默认，损坏扩展拒绝。Context 拥有名称和稳定 callback 存储，可移动值，不可复制所有权/运行中修改。

ClientHello callback 在会话查找前选择 SSL_CTX 和身份专属 session id context；只在普通 SNI callback 换证书不足以约束恢复身份。配置共享 TLS/ALPN 政策，同身份可恢复，跨身份重新握手；编号不是跨 Context/重载的持久身份，证书选择也不等于 HTTP Host 授权。

验证：TLS identity/vendor 单元、http_client_tls、transport_adapter 测试；`zig build -Dtls-server=true tls-server-test tls-server-probe` 后运行 tests/test_tls_server.py 和 test_tls_reader_window.py，覆盖真实 TLS 1.2/1.3、ALPN、SNI/恢复、配置 OOM、WS、partial record、窗口和关闭。平台实际成本/长期驻留需单独测量。
