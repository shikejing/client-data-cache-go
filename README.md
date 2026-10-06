# client-data-cache-go

客户端数据获取与缓存层：对同一份数据的重复读取与并发请求收敛到一致结果，
并在刷新、失效与网络中断时保持行为可预期。整条链路不依赖外部服务，
数据源由进程内模拟服务（`Source`）提供，可在本地复现与验证。

## 快速开始

```go
src := cache.NewSource()
c := cache.New(cache.Options{
    TTL:                  time.Minute,
    StaleWhileRevalidate: true,
})

fetch := func(ctx context.Context, key string) (cache.FetchOutcome, error) {
    v, err := src.Get(ctx, key)
    if err != nil {
        return cache.FetchOutcome{}, err
    }
    return cache.FetchOutcome{Value: v, Tags: []string{"user:1"}}, nil
}

v, err := c.Get(ctx, "user:1:profile", fetch)
```

## 缓存语义

- **按标识去重**：同一 key 的并发读取只触发一次真实获取（singleflight），
  其余读取共享同一结果；获取完成后的读取命中缓存，不再访问数据源。
- **快照一致性**：缓存条目是不可变整体，读取、刷新、失效、取消任意交错时，
  任一时刻读到的都是某个自洽快照，不存在字段级混合或读一半的中间态。
- **取消**：`Get`/`GetFresh` 的调用方取消 context 时仅该调用方收到取消错误，
  底层获取继续为其他等待者完成并填充缓存。
- **时效与刷新**：
  - 数据在 `TTL` 内为新鲜，直接返回。
  - 过期后：`StaleWhileRevalidate=true` 时直接返回旧值并在后台刷新；
    `false` 时等待最新值。
  - `GetFresh` 始终等待最新值（绕过新鲜缓存，共享进行中的获取）。
  - 刷新失败不会清掉已有可用数据，旧值继续按过期策略服务。
- **乐观写入**：`OptimisticSet` 写入立即对读取可见；`Commit` 确认后成为正式缓存；
  `Rollback` 恢复到写入前的状态（包括"此前无缓存"），不残留中间态。

## 失效规则

- `Invalidate(keys...)`：使指定 key 失效，并**级联**失效所有 `DependsOn`
  中包含已失效 key 的条目（递归）。
- `InvalidateTag(tags...)`：使所有带指定标签的条目失效（含级联）。
  同一实体通过不同读取路径（不同 key）写入的缓存，只要打了相同标签，
  一次失效即可全部覆盖。
- 失效后的读取必然触发真实获取，不会读到被淘汰的内容。
- 条目在获取时通过 `FetchOutcome.Tags` / `FetchOutcome.DependsOn` 声明标签与依赖。

## 断网写入排队与重放

`WriteQueue` 将网络中断期间的写入持久化到本地日志（JSON Lines），恢复后重放：

- `Enqueue(op)`：写入入队并落盘；`op.ID` 全局唯一，重复 ID 只入队一次。
- `Replay(apply)`：逐条应用未完成写入，成功后追加 `done` 记录；
  某条失败即停止并保留剩余写入，不静默丢弃。
- 重启后重新 `NewWriteQueue(path)` 即可区分未完成（`Pending()`）与已完成写入。
- 重放不重复生效：`apply` 必须按 `op.ID` 幂等（`Source.Apply` 已内置该语义）。
- 注意：经队列持久化的值按 JSON 语义还原（如数字变为 `float64`）。

## 配置项（`Options`）

| 字段 | 说明 |
| --- | --- |
| `TTL` | 数据新鲜时长，过期后进入陈旧状态 |
| `StaleWhileRevalidate` | 过期读取直接返回旧值并后台刷新（true）或等待最新值（false） |
| `Now` | 可注入时钟，便于测试；默认 `time.Now` |

## 测试

```sh
go test ./...        # 全部测试
go test -race ./...  # 含竞态检测
```

覆盖场景：并发读取去重、缓存命中、过期两种刷新策略、刷新失败保留旧值、
取消与并发变更下的快照一致性、标签失效、级联失效、乐观写入与回滚、
断网排队、重放幂等、重启后未完成/已完成区分。
