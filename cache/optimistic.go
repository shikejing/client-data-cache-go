package cache

// Tx 代表一次乐观写入：提交前即可被读到，确认失败时回滚。
// Tx 不是并发安全的，应由单个 goroutine 完成 Commit 或 Rollback。
type Tx struct {
	cache *Cache
	key   string
	prev  *entry // 乐观写入前的快照，nil 表示此前无缓存
	done  bool
}

// OptimisticSet 对 key 做一次乐观写入，立即对读取可见；
// 返回的 Tx 用于后续确认或回滚。
func (c *Cache) OptimisticSet(key string, value any, tags ...string) *Tx {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.entries[key] // entry 不可变，直接保留指针即快照
	c.entries[key] = &entry{
		value:     value,
		tags:      tags,
		expiresAt: c.now().Add(c.opts.TTL),
		updatedAt: c.now(),
	}
	return &Tx{cache: c, key: key, prev: prev}
}

// Commit 确认乐观写入，使其成为正式缓存内容。
func (tx *Tx) Commit() {
	c := tx.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	tx.done = true
	// 乐观值已在缓存中，无需额外动作。
}

// Rollback 回滚乐观写入，恢复到写入前的状态，不残留中间态。
func (tx *Tx) Rollback() {
	c := tx.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if tx.done {
		return
	}
	tx.done = true
	if tx.prev == nil {
		delete(c.entries, tx.key)
	} else {
		c.entries[tx.key] = tx.prev
	}
}
