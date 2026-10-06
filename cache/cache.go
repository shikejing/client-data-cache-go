package cache

import (
	"context"
	"sync"
	"time"
)

// Options 配置缓存的时效与刷新策略。
type Options struct {
	// TTL 是数据被视为新鲜的时间；过期后按 RefreshMode 处理。
	TTL time.Duration
	// Now 可注入时钟，便于测试；为空时使用 time.Now。
	Now func() time.Time
}

// RefreshMode 决定过期数据的读取行为。
type RefreshMode int

const (
	// StaleWhileRevalidate 直接返回旧值并在后台刷新。
	StaleWhileRevalidate RefreshMode = iota
	// WaitFresh 等待最新值；刷新失败时回退到旧值（若有）。
	WaitFresh
)

// entry 是一条缓存记录，整体替换以保证快照自洽。
type entry struct {
	value     any
	err       error
	fetchedAt time.Time
	tags      []string
}

// call 表示一次正在进行的真实获取（singleflight）。
type call struct {
	done chan struct{}
	ent  *entry
}

// Cache 是并发安全的客户端数据缓存。
type Cache struct {
	mu      sync.Mutex
	fetcher Fetcher
	opts    Options
	entries map[string]*entry
	tags    map[string]map[string]struct{} // tag -> keys
	// inflight 实现按标识的请求去重（singleflight）。
	inflight map[string]*call
}

// New 创建缓存。
func New(fetcher Fetcher, opts Options) *Cache {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{
		fetcher:  fetcher,
		opts:     opts,
		entries:  map[string]*entry{},
		tags:     map[string]map[string]struct{}{},
		inflight: map[string]*call{},
	}
}

// fresh 判断条目是否仍在 TTL 内。
func (c *Cache) fresh(e *entry) bool {
	return e != nil && e.err == nil && c.opts.Now().Sub(e.fetchedAt) < c.opts.TTL
}

// store 整体替换条目并登记标签索引。
func (c *Cache) store(key string, e *entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeLocked(key, e)
}

func (c *Cache) storeLocked(key string, e *entry) {
	if old, ok := c.entries[key]; ok {
		for _, t := range old.tags {
			if set, ok := c.tags[t]; ok {
				delete(set, key)
				if len(set) == 0 {
					delete(c.tags, t)
				}
			}
		}
	}
	c.entries[key] = e
	for _, t := range e.tags {
		set, ok := c.tags[t]
		if !ok {
			set = map[string]struct{}{}
			c.tags[t] = set
		}
		set[key] = struct{}{}
	}
}

// load 执行一次真实获取并共享结果；并发同 key 只会有一个 load 在执行。
// 返回的 entry 是不可变快照，调用方读取无需持锁。
func (c *Cache) load(ctx context.Context, key string, tags []string, wait bool) *entry {
	c.mu.Lock()
	if cl, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		if !wait {
			return nil // 已有后台刷新在进行，调用方不等
		}
		select {
		case <-cl.done:
			return cl.ent
		case <-ctx.Done():
			return &entry{err: ctx.Err()}
		}
	}
	cl := &call{done: make(chan struct{})}
	c.inflight[key] = cl
	c.mu.Unlock()

	go func() {
		v, err := c.fetcher(ctx, key)
		e := &entry{value: v, err: err, fetchedAt: c.opts.Now(), tags: tags}
		c.mu.Lock()
		if err == nil {
			// 刷新失败不得清掉已有可用数据：仅成功时落盘。
			c.storeLocked(key, e)
		} else if old, ok := c.entries[key]; ok {
			// 保留旧值，仅把错误透传给本次等待者。
			e = &entry{value: old.value, err: err, fetchedAt: old.fetchedAt, tags: old.tags}
		}
		cl.ent = e
		delete(c.inflight, key)
		close(cl.done)
		c.mu.Unlock()
	}()

	if !wait {
		return nil
	}
	select {
	case <-cl.done:
		return cl.ent
	case <-ctx.Done():
		return &entry{err: ctx.Err()}
	}
}

// Get 读取 key；并发同 key 只触发一次真实获取，完成后命中缓存。
// mode 决定过期时是后台刷新（返回旧值）还是等待最新值。
func (c *Cache) Get(ctx context.Context, key string, mode RefreshMode, tags ...string) (any, error) {
	c.mu.Lock()
	e, ok := c.entries[key]
	c.mu.Unlock()

	switch {
	case ok && c.fresh(e):
		return e.value, nil
	case ok && mode == StaleWhileRevalidate:
		// 后台刷新，立即返回旧值。
		c.load(context.Background(), key, tags, false)
		return e.value, nil
	default:
		// 无缓存或等待最新值。
		ent := c.load(ctx, key, tags, true)
		if ent == nil {
			// 极少见：wait=true 必返回非空；防御性返回。
			return nil, nil
		}
		return ent.value, ent.err
	}
}

// Invalidate 使指定 key 失效；之后的读取不会再返回被淘汰的内容。
func (c *Cache) Invalidate(keys ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		c.removeLocked(k)
	}
}

// InvalidateTag 使标签下所有 key 级联失效。
func (c *Cache) InvalidateTag(tags ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tags {
		for k := range c.tags[t] {
			c.removeLocked(k)
		}
	}
}

func (c *Cache) removeLocked(key string) {
	e, ok := c.entries[key]
	if !ok {
		return
	}
	for _, t := range e.tags {
		if set, ok := c.tags[t]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.tags, t)
			}
		}
	}
	delete(c.entries, key)
}
