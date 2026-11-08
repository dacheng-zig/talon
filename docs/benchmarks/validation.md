# 清理后验证记录

2026-10-09，Darwin arm64，Zig 0.16.0、zio v0.18.0、OpenSSL 4.0.3、Python 3.14.8。检查针对当前未提交工作树；不依赖已删除的历史实验档案，也不声称性能改善。

| 检查                   | 命令/结果                                                                                                                                                                           |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Debug                  | `zig build test --summary all`：8/8 steps，370 通过，2 跳过                                                                                                                         |
| ReleaseSafe + 可选 TLS | `zig build test check bench tls-probe tls-server-test tls-server-probe -Dtls-server=true -Doptimize=ReleaseSafe --summary all`：22/22 steps，378 通过，3 跳过，含 9 项 OpenSSL 单元 |
| 九个示例               | `zig build -Doptimize=ReleaseSafe -j2 --summary all`：全部编译/安装通过                                                                                                             |
| 真实 TLS               | 先构建 probes，`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_tls_*.py'`：29 项全部通过，无跳过                                                          |
| 格式/差异              | 改动的 Zig 文件通过 `zig fmt --check`；`git diff --check` 通过                                                                                                                      |

本机两项跳过是 Linux TCP_USER_TIMEOUT 运行测试；ReleaseSafe 另跳过 Debug-only 借出追踪检查。非 Linux 拒绝配置已验证，不把跨目标编译或本机拒绝当成 Linux 内核行为验收。

TLS 检查包括本地 CA 签发证书的 DNS/IPv4/IPv6 类型匹配及无 IP SNI（客户端两个测试内共 18 个子场景），TLS 1.2/1.3，服务端 HTTP/WS，ALPN 优先/拒绝，SNI 静态身份/跨身份恢复，损坏扩展，配置分配回滚，默认/大头窗口与 pipeline，partial record timeout、close_notify 和 forced drain。

## 精简网络负载

全部使用 ReleaseSafe 客户端、独立 fixture、17B 完整响应。每个场景均退出 0，active/waiting=0，Client.deinit 后 requested bytes=0。以下参数在 URL 后传入；对端使用 `--port 0`，客户端采用实际输出地址。

| 场景          | 参数/断言                                                                                                                                                                                                  |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| TCP keepalive | `--requests 64 --concurrency 4 --executors 1 --warmup 2`                                                                                                                                                   |
| TCP cold      | 同上，`--executors 2 --mode cold`；测量期 created=64、reused=0                                                                                                                                             |
| 有界 arrival  | `--requests 40 --concurrency 4 --rate 100 --warmup 0`                                                                                                                                                      |
| 过载          | `--requests 40 --concurrency 8 --rate 1000 --warmup 0 --pool-active 1 --pool-waiters 0 --allow-overload yes`；对端 delay=20ms，观测 2 成功、33 PoolQueueFull、5 generator dropped、0 其他失败，分类合计 40 |
| idle retain   | `--requests 32 --concurrency 4 --warmup 2 --idle-ms 10`                                                                                                                                                    |
| idle trim     | 同上加 `--idle-action trim`；受测动作关闭 4 个空闲连接，恢复成功                                                                                                                                           |
| TLS 1.2 / 1.3 | 各 `--requests 32 --concurrency 4 --executors 2 --warmup 2 --trust self-signed`，对端分别固定版本，证书含目标 IP SAN                                                                                       |

这些有限场景支持保留工具的行为、错误分类及释放契约，不支持吞吐排名、严格公平、长期驻留或全进程/内核资源上限。临时证书、执行日志、缓存和二进制不作为仓库设计资产。复现方法见 [README](README.md)。

## 项目现状审查后的验证

同日、同一 Darwin arm64 / Zig 0.16.0 / zio v0.18.0 / OpenSSL 4.0.3 环境，针对 [项目现状审查](../project-status.md) 的 HTTP、WebSocket、SSE 修正重新验证。既有本地改动和 index 保留，未创建提交；这不是 Linux 或生产长稳验收。

修复前的新增回归复现了 303 HEAD 被改为 GET、请求校验错误留下输出前缀、WS 接受非最短扩展长度、SSE ID/事件元数据被覆盖；修复后补充多跳重定向和跨连接 SSE 状态检查。默认 suite 相比本轮基线新增 6 项测试，不能将测试数增长解读为覆盖率百分比。

客户端 TLS vendor 的原件 SHA-256 与 vendor README 一致；完整 diff 只有声明的 import、IP identity 和 SNI 分支，未修改密码学实现。这个源码比对不能代替安全审计。

| 检查                            | 命令与结果                                                                                                                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 最终 Debug 全套                 | `zig build test -j2 --summary all`：8/8 steps，376 通过、2 跳过；含新增多跳同 origin 凭据保留用例                                                                                       |
| ReleaseSafe + 可选 TLS          | `zig build test check bench tls-probe tls-server-test tls-server-probe -Dtls-server=true -Doptimize=ReleaseSafe -j2 --summary all`：22/22 steps，384 通过、3 跳过，含 9 项 OpenSSL 单元 |
| 最终重定向 ReleaseSafe 定向回归 | 用构建相同的 zio/talon 模块参数执行 `zig test ... --test-filter 'redirect method and body headers' -OReleaseSafe`：1 项通过，包含 7 个重定向场景                                        |
| 九个示例                        | `zig build -Doptimize=ReleaseSafe -j2 --summary all`：20/20 steps，全数编译/安装通过                                                                                                    |
| 真实 TLS                        | `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_tls_*.py' -v`：29 项通过、0 跳过                                                                              |
| 格式与文档                      | 本轮涉及 Zig 文件的 `zig fmt --check`、worktree/index 的 `git diff --check` 通过；新增/修改表格通过 Unicode 宽度与幂等检查；83 个本地文档链接目标存在                                   |

完整 ReleaseSafe/示例构建启动后还修正了 origin 比较与上一跳 host 释放的顺序；这项最终调整由最终 Debug 全套和 ReleaseSafe 定向回归核验。定向命令使用本轮生成的 zio_options 和固定依赖模块参数，完整复现入口仍为上表的 zig build 命令。

两项共同跳过仍是 Linux TCP_USER_TIMEOUT 运行测试；ReleaseSafe 额外跳过 Debug-only 借出追踪。真实 TLS 覆盖与前述本地证书、SNI/ALPN、关闭及窗口验收一致。本轮只构建网络 bench，没有重跑网络性能测量或补充长期容量结论。
