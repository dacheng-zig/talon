# 流式分帧

framing 只解释字节流边界，协议决定消息语义。选择借用或拥有取决于消费者是否需要跨 refill/后续读取保留消息，而不是统一使用一种内存模型。

## 借用窗口

LengthPrefixed 的 max_frame 计 payload；正文须能进入 reader 的窗口，返回切片只在下一次修改 reader 前有效。Delimited 的 max_frame 同样计 payload，搜索时允许 delimiter 的未完成前缀，不能因输入分段而拒绝精确限额帧。自重叠 delimiter 必须保留可匹配前缀。

Accumulator 按 Decoder 返回的 consumed 限制整个编码帧，包括长度/分隔开销，排除后续 pipeline 字节。完整帧与 need_more 使用同一限额，不允许一次到齐绕过。需要纯 payload 限额时由 Decoder 解释自己的编码并校验。

## 拥有型长度帧

OwnedLengthPrefixed 在读出并校验长度后，通过显式 allocator 分配正文，可处理大于 reader 窗口的消息。OwnedFrame 是唯一所有者，不能复制后重复 deinit，allocator 活到帧释放。

每帧上限只保护单个消息；保留多个帧或多个连接须共用 [分配预算](resource-ownership.md)。任何读取/分配错误释放尚未返回的正文并使 frame reader 终止；后续 next 返回 FrameReaderFailed。已经消费半帧时无法可靠重同步，不提供隐式重试。

全部拥有化会增加分配及消费者释放责任；全部借用又迫使大消息扩大每连接窗口。保留两条明确入口，避免带隐式复制的“自动”策略。

## 内存传输边界

Pipe.closeRead 终止本端读取，即使内部仍有缓冲；关闭读端不是“最后再读一次”的信号。MemoryListener 持有注册 Pair 到 listener.deinit，raw.close 结束连接并不释放 listener 的全部内存。

验证包括碎片输入、完整超限、精确上限、多帧同窗、自重叠分隔符、整数/EOF 边界、OOM/取消以及小窗口的大帧 round-trip。单元测试位于 framing/pipe，集成入口是 owned_framing_test。
