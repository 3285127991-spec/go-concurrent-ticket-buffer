# ticketbuffer

并发安全、严格有序的 Go ticket 缓冲区。仅标准库，无第三方依赖。

## 语义

- `Reserve()` 从 1 开始分配严格递增、永不重复的 `uint64` ticket；新 ticket 处于 pending，只能被 `Commit` 或 `Cancel` 完成一次。
- `Commit(ticket, value)` / `Cancel(ticket)` 可任意乱序；重复完成、未知 ticket、`ticket=0` 返回明确错误。
- `Next(ctx)` 严格按 ticket 顺序消费：头部 pending 则等待；已 Cancel 自动跳过；已 Commit 返回 `(ticket, value)` 并永久消费。并发 `Next` 不会重复消费；`ctx` 取消只影响该次调用。
- `Close()` 拒绝新的 Reserve/Commit/Cancel，并将所有 pending ticket 原子视为取消；已 Commit 的数据仍可按序排空，排空后 `Next` 返回 `ErrClosed`。`Close` 幂等。
- `Snapshot()` 返回 `NextReserve / NextConsume / Pending / Ready / Closed` 的独立值副本；`Len()` 只统计已 Commit 但未消费的 ticket 数。

## 使用

```go
b := ticketbuffer.New()
t1, _ := b.Reserve()
t2, _ := b.Reserve()
b.Commit(t2, "second") // 乱序提交
b.Cancel(t1)           // t1 被跳过
ticket, value, err := b.Next(context.Background()) // => (t2, "second")
b.Close()
```

## 运行

```sh
go run ./cmd/ticketbuffer   # 演示：乱序 Commit、Cancel、顺序 Next、Close 排空
go test -race ./...         # 竞态检测下运行全部测试
```

## 明确不做

持久化、重试、TTL、优先级、网络、GUI、数据库、内部后台 goroutine。
