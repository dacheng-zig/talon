# 资源所有权与预算

资源由实际所有者释放，容量政策在其边界执行。统一的 AllocationBudget 计量路由到 allocator 的分配；协议大小、活跃数量、等待数量和空闲保留量各自限制不同资源，不能互相代替。

## 共享分配预算

调用方持有稳定地址的 `AllocationBudget.init(child, max_bytes)`，将 `allocator()` 交给 server/client/listener/应用。0 拒绝非零分配。预算对象和 child 活到所有句柄及分配释放，不能复制对象或运行中修改上限。

alloc/增长先 CAS 预留，child 失败回滚；成功缩小归还差额，free 在 child 完成后归还。标准 allocate-copy-free 扩容可能暂时同时计入旧新块。arena reset 或归还缓存并不等于 free；保留容量继续收费。额度不足沿用 OutOfMemory/null/false，不增加隐式排队。

stats 的 reserved 包含尚在执行的增长预留，peak 包含随后失败的预留，denied 只计预算拒绝的 allocator 调用；一次逻辑分配可能多次调用。字段独立原子读取，并非一致多字段快照。

| 方案 | 取舍 |
|---|---|
| 各协议手动 reserve/release | 重复扩容、节点和失败回滚规则，容易把 payload 当真实容量 |
| server 内置预算 | 无法自然跨 client/server 共享，移交资源依赖 server 寿命 |
| 调用方 allocator decorator | 采用；复用既有分配/释放契约，覆盖范围显式 |

requested bytes 不包含 allocator 元数据/size class 余量、child 内部复制、独立 mmap 栈、内核 socket 或 OpenSSL 内部分配。它是分配请求预算，不是 RSS 硬上限。

## BufferPool

池锁只协调缓存 pop/append、容量判断和 Debug 借出记录。payload 分配、超额归还释放、trim 快照释放在锁外执行；元数据增长仍在锁内，不承诺临界区完全无 allocator 调用。

rent 的未注册窗口归当前调用独占，注册失败先解锁再 free；give 的缓存保留判断只有一个所有者。trim 在锁内摘取 free list，仅在没有借出记录时摘取 Debug map，随后锁外释放。已有租借和并行新租借保持有效。allocator 必须线程安全、不挂起协程、不重入池；deinit 前所有调用，包括锁外 allocator 调用，都必须结束。

预分配全部缓存槽或引入分片可减少元数据争用，却改变初始化预算并增加竞争安装规则；当前仅缩短 payload 临界区。只有实际元数据瓶颈才支持进一步复杂化。

## arena 与借用

HTTP 协议拥有请求 arena，core Connection 只提供 gpa。`max_request_arena_bytes` 可选限制该 arena 的 live requested bytes（含节点和保留容量）；`max_retained_arena` 在请求边界限制保留。局部预算叠在共享 child 上，先销毁 arena 再销毁预算。Request/升级会话不能把 allocator 或借用逃逸到协议作用域之外。

客户端最终响应头保存在连接 arena；reason/header 借用只到 Response.deinit。池在 checkin 才重置并限制保留，不能在活跃响应时收缩。收缩失败 free_all，不因缓存策略放弃健康传输。解码器和正文收集预算见 [客户端](http-client.md)。

## 显式空闲回收

`BufferPool.trimIdle` / `StreamServer.trimIdleBuffers` 释放当前空闲窗口；`Client.trimIdleConnections` 摘取当前 idle buckets，再锁外关闭连接、释放 arena/decoder/TLS 和缓存元数据。活跃 Response/SSE/WS、正在借用的窗口和准入状态不受影响。返回值分别为 payload 字节或连接数量，不是 RSS 降幅。

两读写池、并发 checkin 与各统计不是全局原子快照，后续归还可形成新缓存。client close 可能挂起，trim 不得与 deinit 并发。活跃 HTTP arena 与 MemoryListener 所有的管道也不属于这些 idle 快照。

拒绝 allocator OOM callback 自动回收：它会跨对象寿命、重入锁，并可能在标准 allocator 中挂起。调用方在尚未消费输入/提交输出的安全边界协调回收，然后尝试新的操作；失败的半帧、响应或不可重放请求不能自动重试。

验证：allocation_budget、budget_pressure、http_arena_budget、idle_reclaim，以及 BufferPool 内部并发/失败注入测试。关注预留守恒、部分初始化回滚、已有连接进展和最终归零，不把短测试解释为长期内存稳定性。
