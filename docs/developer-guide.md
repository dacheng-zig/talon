# Talon 开发指南

当前依赖为 Zig 0.16.0 与 zio v0.18.0，见 [build.zig.zon](../build.zig.zon)。API 用法见 [用户指南](user-guide.md)，边界与取舍见 [架构](talon-architecture.md)。

## 源码地图

| 路径 | 维护责任 |
|---|---|
| src/core/stream_server.zig | listener/Proto 组合、许可、连接任务、停机、指标 |
| src/core/connection.zig | value/pinned I/O、完整 flush、期限和受检 detach |
| src/core/listener.zig、dial.zig、pipe.zig | TCP/内存传输、出站建立、确定背压 |
| src/core/limits.zig、timeout.zig、tcp_user_timeout.zig | 通用资源限额、能力拒绝、Linux socket 政策 |
| src/core/buffer_pool.zig、allocation_budget.zig、framing.zig | 缓存、路由分配预算、拆帧所有权 |
| src/core/chain.zig、middleware.zig | comptime 中间件链及内置连接中间件 |
| src/http/codec/ | 双向 wire 编解码、body、SSE、WS Session |
| src/http/server/ | HTTP policy、请求/响应、升级与服务端会话 |
| src/http/client/ | 请求政策、连接池、响应/解码、TLS 身份和 Cookie |
| src/tls_server.zig、tls_openssl.c/h | 可选 OpenSSL memory BIO adapter |
| tests/ | 通过公开 API 的集成和真实 TLS 回归 |
| bench/ | 独立 TCP/TLS 客户端负载与 Python 对端 |

## 编译与测试

```sh
zig build check                    # 只编译默认测试，不执行目标二进制
zig build test                     # 单元、集成及保留的 bench 边界检查
zig build test -Doptimize=ReleaseSafe
zig build -Doptimize=ReleaseSafe    # 九个示例
zig build bench -Doptimize=ReleaseSafe
zig build tls-probe -Doptimize=ReleaseSafe
zig build -Dtls-server=true tls-server-test tls-server-probe -Doptimize=ReleaseSafe
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_tls_*.py'
```

可选服务端需要 pkg-config/OpenSSL，Python TLS 回归还需 openssl CLI。未构建 probe 时 Python 会明确 skip；验收时必须先构建再核对 skip 数。HTTPS 外网 smoke 会因网络条件跳过，不把它当本地确定性验证。

`check -Dtarget=…` 只支持源码/链接检查，不能当目标平台运行证据；跨目标 OpenSSL 还需匹配的系统头和库。构建输出在 zig-out，缓存和运行日志不是设计材料。可复用网络方法见 [bench README](benchmarks/README.md)。

## 连接组合与扩展

自定义 Proto 至少实现 `serve(conn, app) !void`。要使用独立协议 policy，声明 Limits/engineLimits/serveWithConfig；有限期限要求可用的 transport capabilities。不要为转发调用增加包装类，也不要把 HTTP 的 max_header_size 放进 core。

Listener 声明 RawConnection 和可取消 accept；Raw 提供 reader/writer、shutdown/close、身份和期限，或以 pinned_transport 提供稳定 ioReader/ioWriter。adapter 声明 Connection(Raw)，在受追踪任务中 wrap；优先使用 WrapOptions 接收有效窗口和原始期限。成功包装后由 Wrapped 关闭 raw，失败释放部分包装但不重复关闭 engine 所有的 raw。

Dialer 拥有 Target/RawConnection/dial(target, timeout)，成功只移交一个 raw owner。协议特有的 scheme/Host/证书政策留在上层 Connector。解析与候选尝试不得重启预算，取消/超时不得按普通失败继续重试。

新增 framing 时明确 consumed 和 payload 限额的区别、切片寿命、碎片边界及错误能否继续。需要跨 refill 保留的大帧显式使用 allocator，不能静默增加每连接窗口。

## 维护不变量

1. core → zio，http → core；TLS 可选模块 → core。协议不能反向控制 engine 内部缓存或停机计数。
2. 固定地址 Reader/Writer、Session、server/client/pool/budget 在使用期间不移动或复制所有权；必须完成初始化后才发布借用。
3. allocator 在自旋锁内不得挂起或重入；payload 分配释放和可挂起 I/O 尽可能在缓存锁外。锁内元数据增长仍须满足 allocator 契约。
4. 所有预留/部分初始化都有对应 rollback；取消授予竞态须测试，不能仅看正常最终计数。
5. 部分输出失败是 terminal。HTTP fallback、101 retry、WS 帧重发和 TLS BIO 重送都可能损坏流。
6. Response 的头借用、请求 arena、应用 payload、子任务必须在各 owner 销毁前结束。Group.cancel 等清理顺序早于栈上 Session 的释放。
7. 配置中的 active/waiting、wire、decoded、allocator 和 idle 上限各有独立作用域；修改任何限额须更新其设计和失败测试。

## 验证选择

纯 parser/codec 使用内存 Reader/Writer，覆盖合法/非法输入和不同分段；MemoryListener 与小 Pipe 测完整连接循环、控制背压和取消；TCP/TLS 回归检查真实 I/O、证书、ALPN/SNI、关闭和窗口。OOM 路径使用 std.testing.checkAllAllocationFailures 或精确共享预算。

回归应在规则所有者边界断言 wire、状态和资源守恒，不依赖固定 sleep 假定竞争已发生。定时等待须有上限。性能工具计时及操作系统诊断留在 bench，生产 API 只保留具有稳定语义的运行状态和资源指标。

客户端 TLS 保留的 std 兼容副本必须随编译器升级完整比对，出处/哈希/许可证见 [vendor README](../src/http/client/vendor/README.md)。不随手刷新 vendor 文件或改变密码学认证；上游身份 hook 可替代后再移除副本。
