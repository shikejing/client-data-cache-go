# client-data-cache-go

客户端数据获取与缓存层：对同一份数据的重复读取与并发请求收敛到一致结果，
并在刷新、失效与网络中断时保持行为可预期。整条链路不依赖外部服务，
数据源由进程内模拟服务（`Source`）提供，可在本地复现与验证。

## 快速开始

```go
src := cache.NewSource()          // 进程内模拟数据服务
src.Set("user:1", "alice")

c := cache.New(src.Fetch, cache.Options{TTL: time.Minute})
v, err := c.Get(ctx, "user:1", cache.WaitFresh, "user:1")
```

## 缓存语义

### 请求去重与复用（singleflight）

- 以稳定标识（key）为单位去重：同一 key 的并发读取只触发一次真实获取，
  其余读取共享同一结果。
- 请求完成后的读取在 TTL 内直接命中缓存，不再触发真实获取。
- 缓存条目是不可变快照，整体替换；任何时刻读到的都是某个自洽快照，
  不会出现字段级混合或读一半的中间结果。

### 时效与刷新策略

`Get(ctx, key, mode, tags...)` 的 `mode` 决定数据过期（超过 `TTL`）后的行为：

| 模式 | 行为 |
|---|---|
| `StaleWhileRevalidate` | 立即返回旧值，并在后台触发刷新；刷新成功后后续读取得到新值 |
| `WaitFresh` | 等待最新值；刷新失败时回退到旧值（若有），同时把错误透传给本次调用 |

刷新失败**不会**清掉已有可用数据：失败的刷新只把错误透传给等待者，旧条目保留。

### 读取取消

`Get` 接受 `context.Context`：取消的读取返回 `ctx.Err()`，不污染缓存，
不影响其他共享同一获取的读取者。

## 失效规则

- `Invalidate(keys...)`：按标识失效指定条目。
- `InvalidateTag(tags...)`：按标签级联失效。读取时通过 `Get` 的 `tags`
  参数登记标签；同一数据即使通过不同读取路径（不同 key）写入，
  只要带同一标签，一次 `InvalidateTag` 即可全部淘汰。
- 失效是即时的：失效之后的读取不会再返回被淘汰的内容，会重新获取。
- 失效不影响无关标签的条目。

## 乐观写入与回滚

```go
err := c.OptimisticWrite(ctx, cache.Mutation{
    Key:   "user:1",
    Value: "alice2",
    Tags:  []string{"user:1"},
    Confirm: func(ctx context.Context) error {
        return submitToServer() // 返回错误则触发回滚
    },
})
```

- 写入在确认之前即可被读到（乐观应用）。
- 确认失败或被拒绝时回滚到乐观写入之前的状态：先前有值则恢复旧值，
  先前无值则彻底移除，不残留中间态。

## 断网排队与恢复重放

```go
q := cache.NewWriteQueue("wal.log", applier)
_ = q.Enqueue(ctx, cache.WriteOp{ID: "w1", Key: "a", Value: "1"}) // 离线时安全调用
// 网络恢复后：
_ = q.Replay(ctx)
```

- 写入先入 WAL（持久化日志）再尝试生效；离线或失败时保留在队列中，
  **不会静默丢弃**。
- 每条写入必须带幂等标识 `ID`；重放按 `ID` 去重，不重复生效。
  `IdempotentApplier` 可包装目的地，覆盖“已生效但完成标记未落盘”的崩溃场景。
- 重启后从 WAL 恢复：已完成（有 done 标记）与未完成写入可区分，
  只有未完成的会被重放。
- `Pending()` 返回未完成写入数。

## 配置项

| 项 | 说明 |
|---|---|
| `Options.TTL` | 数据被视为新鲜的时长；过期后按 `RefreshMode` 处理 |
| `Options.Now` | 可注入时钟，便于测试；为空时使用 `time.Now` |
| `NewWriteQueue(path, applier)` | `path` 为 WAL 文件路径，为空时仅内存排队；`applier` 必须按 `ID` 幂等 |

## 测试

```sh
go test -race ./...
```

覆盖：并发读取去重、过期刷新（含失败保留旧值）、失效级联、
乐观写入可见性与回滚、断网排队与恢复重放、重启后完成/未完成区分、
并发变更下的快照自洽性。
