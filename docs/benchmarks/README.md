# Talon 网络 bench

保留一个独立 TCP/TLS 客户端负载（bench/http_network.zig）、有界到达发生器（arrival.zig）和 Python HTTP/1.1 对端（fixture.py）。功能回归属于 tests；系统分配器、内核记账、冻结源码/二进制和逐次抓包不作为项目架构或发布输入。

## 复现

```sh
zig build bench -Doptimize=ReleaseSafe
# 终端一；--port 0 自动选端口，以输出地址为准
python3 bench/fixture.py --port 8081 --body-bytes 17
# 终端二
zig-out/bin/http_network_bench http://127.0.0.1:8081/ --requests 1000 --concurrency 8 --executors 1 --mode keepalive
zig-out/bin/http_network_bench http://127.0.0.1:8081/ --requests 1000 --concurrency 8 --executors 2 --mode cold
zig-out/bin/http_network_bench http://127.0.0.1:8081/ --requests 200 --concurrency 8 --rate 100 --warmup 0
zig-out/bin/http_network_bench http://127.0.0.1:8081/ --requests 100 --concurrency 8 --idle-ms 100 --idle-action trim
```

TLS 对端添加 `--cert CERT --key KEY`，可用 `--tls-version 1.2|1.3`。客户端用 https URL；系统信任为默认，`--trust self-signed` 只适用于显式本地证书，仍验证目标 DNS/IP。本地证书可临时生成：

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost,IP:127.0.0.1 -keyout /tmp/talon-key.pem -out /tmp/talon-cert.pem
python3 bench/fixture.py --port 8443 --cert /tmp/talon-cert.pem --key /tmp/talon-key.pem
zig-out/bin/http_network_bench https://127.0.0.1:8443/ --requests 100 --concurrency 4 --trust self-signed
```

## 测量契约

每次请求必须状态 200 且完整读取指定 body_bytes，否则负载失败。预热与测量分开；日志包含请求数、并发、executor、构建模式及信任政策。样本/发生器分配不计入 Client 的 AllocationBudget。

closed-loop 每个 worker 等上一响应后才发送，报告完成吞吐和 nearest-rank p50/p95/p99/max；慢响应会自动降低 offered rate。arrival-rate 按索引计算绝对计划时刻，不累积整数取整误差，以独立槽位上限限制在途；记录实际派发/开始滞后、成功、池拒绝、pool timeout、发生器丢弃和其他错误。完成吞吐包括 drain，不能冒充配置到达率。成功百分位只代表成功子集。

`--pool-active`、`--pool-waiters`、`--pool-wait-ms` 独立配置准入；`--allow-overload yes` 只容许声明的过载分类，不隐藏其他请求失败。发生器取消先结束全部 worker 再释放 records。

idle retain/trim 比较当前缓存和恢复成本，只能用于 keepalive。初始测量、受测 idle 动作、恢复和最终统一清理分别记录，不能把最终清理算成测量中的 trim。长期 idle 或 peer 关闭也可能导致重建连接。

日志报告连接创建/复用/逐出、分配调用和路由 requested bytes，并检查 active/waiting 归零、Client.deinit 后预算归零。它不包含独立 runtime 栈、allocator 余量、OpenSSL 或内核 socket，不代表 RSS。短本地负载是行为检查，不能用于跨机器性能排名或长期容量结论。

清理后的验证记录见 [validation.md](validation.md)。新测量写到临时目录，只有支持实际架构决策的简短结果进入文档；不要复制依赖/源码树或提交原始抓包。
