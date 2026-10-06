package cache

import "context"

// Mutation 描述一次乐观写入：本地先应用，确认后生效，失败回滚。
type Mutation struct {
	Key   string
	Value any
	// Tags 作为乐观条目的标签，便于失效级联。
	Tags []string
	// Confirm 同步确认写入（例如提交到服务端）；返回错误则触发回滚。
	Confirm func(ctx context.Context) error
}

// OptimisticWrite 先让写入可见，再确认；确认失败回滚到写入前状态，
// 不残留中间态。确认与回滚都在本调用返回前完成，行为可预期。
func (c *Cache) OptimisticWrite(ctx context.Context, m Mutation) error {
	// 1. 快照写入前状态（可能不存在）。
	c.mu.Lock()
	prev, hadPrev := c.entries[m.Key]
	c.mu.Unlock()

	// 2. 乐观应用：确认前即可被读到。
	c.store(m.Key, &entry{
		value:     m.Value,
		fetchedAt: c.opts.Now(),
		tags:      m.Tags,
	})

	// 3. 确认。
	if m.Confirm != nil {
		if err := m.Confirm(ctx); err != nil {
			// 4. 回滚：恢复写入前状态，或彻底移除。
			c.mu.Lock()
			if hadPrev {
				c.storeLocked(m.Key, prev)
			} else {
				c.removeLocked(m.Key)
			}
			c.mu.Unlock()
			return err
		}
	}
	return nil
}
