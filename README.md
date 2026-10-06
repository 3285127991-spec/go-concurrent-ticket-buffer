# ticketbuffer

并发安全的 Go 有序票据缓冲库：乱序提交（Commit），严格按票号顺序消费（Next）。
仅依赖标准库，无第三方服务。

## 功能

- `Reserve` 从 1 开始分配严格递增、永不重复的 `uint64` 票据。
- 每张票据只能被 `Commit` 或 `Cancel` 一次；重复完成、未知票据、票据 0 均返回明确错误。
- `Commit` 可任意乱序；`Next` 永远按票号顺序消费：头部票据 pending 则等待，已 Cancel 则跳过，已 Commit 则返回并永久消费。
- 多个并发 `Next` 不会重复消费同一票据；`ctx` 取消只影响当次调用。
- `Close` 后拒绝新的 Reserve/Commit/Cancel，所有 pending 票据原子视为取消；已 Commit 的数据仍按顺序排空，排空后 `Next` 返回 `ErrClosed`。
- `Snapshot` 返回 `NextReserve / NextConsume / Pending / Ready / Closed` 的独立副本；`Len` 只统计已 Commit 未消费的票据数。

## 使用

```go
buf := ticketbuffer.New()
defer buf.Close()

t1, _ := buf.Reserve() // 1
t2, _ := buf.Reserve() // 2

buf.Commit(t2, "b") // 乱序提交
buf.Cancel(t1)      // t1 被跳过

ticket, value, err := buf.Next(ctx) // -> (2, "b", nil)
```

## 演示

```sh
go run ./cmd/ticketbuffer-demo
```

演示 4 张票据：3 次乱序 Commit、1 次 Cancel、顺序 Next、Close 后排空。

## 测试

```sh
go test -race ./...
```
