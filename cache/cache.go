package cache

import (
	"context"
	"sync"
	"time"
)

// FetchOutcome 是一次真实获取的结果，除值外还可声明标签与依赖，
// 供失效与级联失效使用。
type FetchOutcome struct {
	Value     any
	Tags      []string
	DependsOn []string
}

// FetchFunc 从数据源获取 key 对应的数据。
type FetchFunc func(ctx context.Context, key string) (FetchOutcome, error)

// Options 控制缓存的时效与刷新行为。
type Options struct {
	// TTL 是数据的新鲜时长，过期后进入陈旧状态。
	TTL time.Duration
	// StaleWhileRevalidate 为 true 时，过期读取直接返回旧值并后台刷新；
	// 为 false 时，过期读取等待最新值。
	StaleWhileRevalidate bool
	// Now 可注入时钟，便于测试；为空时使用 time.Now。
	Now func() time.Time
}

// Cache 提供按标识去重的读取、时效刷新与失效能力。
type Cache struct {
	mu       sync.Mutex
	opts     Options
	entries  map[string]*entry
	inflight map[string]*call
}

// call 代表一次进行中的真实获取，并发读取共享同一结果。
type call struct {
	done chan struct{}
	out  FetchOutcome
	err  error
}

// New 创建一个缓存实例。
func New(opts Options) *Cache {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{
		opts:     opts,
		entries:  make(map[string]*entry),
		inflight: make(map[string]*call),
	}
}

func (c *Cache) now() time.Time { return c.opts.Now() }

// Get 按标识读取：命中新鲜缓存直接返回；过期时按策略返回旧值并后台刷新，
// 或等待最新值；同一标识的并发读取只触发一次真实获取。
func (c *Cache) Get(ctx context.Context, key string, fetch FetchFunc) (any, error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		if !e.expired(c.now()) {
			v := e.value
			c.mu.Unlock()
			return v, nil
		}
		if c.opts.StaleWhileRevalidate {
			v := e.value
			c.startRefreshLocked(key, fetch)
			c.mu.Unlock()
			return v, nil
		}
	}
	cl, leader := c.startFetchLocked(key, fetch)
	c.mu.Unlock()

	if leader {
		c.runFetch(ctx, key, cl, fetch)
	}
	return c.wait(ctx, cl)
}

// GetFresh 总是等待最新值：若已有进行中的获取则共享其结果，
// 否则发起一次新的获取，绕过新鲜缓存。
func (c *Cache) GetFresh(ctx context.Context, key string, fetch FetchFunc) (any, error) {
	c.mu.Lock()
	cl, leader := c.startFetchLocked(key, fetch)
	c.mu.Unlock()

	if leader {
		c.runFetch(ctx, key, cl, fetch)
	}
	return c.wait(ctx, cl)
}

// startFetchLocked 返回该 key 的进行中获取；若不存在则创建并标记调用方为 leader。
// 调用方必须持有 c.mu。
func (c *Cache) startFetchLocked(key string, _ FetchFunc) (*call, bool) {
	if cl, ok := c.inflight[key]; ok {
		return cl, false
	}
	cl := &call{done: make(chan struct{})}
	c.inflight[key] = cl
	return cl, true
}

// startRefreshLocked 在没有进行中获取时发起一次后台刷新。
// 调用方必须持有 c.mu。
func (c *Cache) startRefreshLocked(key string, fetch FetchFunc) {
	if cl, leader := c.startFetchLocked(key, fetch); leader {
		go c.runFetch(context.Background(), key, cl, fetch)
	}
}

// runFetch 执行真实获取并落定结果：成功写入缓存，失败保留旧值。
func (c *Cache) runFetch(ctx context.Context, key string, cl *call, fetch FetchFunc) {
	out, err := fetch(ctx, key)
	c.mu.Lock()
	defer c.mu.Unlock()
	cl.out, cl.err = out, err
	if err == nil {
		c.entries[key] = &entry{
			value:     out.Value,
			tags:      out.Tags,
			dependsOn: out.DependsOn,
			expiresAt: c.now().Add(c.opts.TTL),
			updatedAt: c.now(),
		}
	}
	delete(c.inflight, key)
	close(cl.done)
}

// wait 等待共享的获取结果；ctx 取消时返回取消错误，获取本身继续为其他等待者完成。
func (c *Cache) wait(ctx context.Context, cl *call) (any, error) {
	select {
	case <-cl.done:
		if cl.err != nil {
			return nil, cl.err
		}
		return cl.out.Value, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Invalidate 使指定标识的缓存失效，并级联失效依赖于它们的条目。
func (c *Cache) Invalidate(keys ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidateLocked(keys)
}

// InvalidateTag 使带有指定标签的所有缓存失效（含级联）。
func (c *Cache) InvalidateTag(tags ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	want := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		want[t] = struct{}{}
	}
	var keys []string
	for k, e := range c.entries {
		for _, t := range e.tags {
			if _, hit := want[t]; hit {
				keys = append(keys, k)
				break
			}
		}
	}
	c.invalidateLocked(keys)
}

// invalidateLocked 删除指定 key，并递归删除 dependsOn 中包含已失效 key 的条目。
// 调用方必须持有 c.mu。
func (c *Cache) invalidateLocked(keys []string) {
	gone := make(map[string]struct{}, len(keys))
	queue := append([]string(nil), keys...)
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		if _, ok := gone[k]; ok {
			continue
		}
		gone[k] = struct{}{}
		delete(c.entries, k)
		// 级联：依赖于 k 的条目一并失效。
		for ck, e := range c.entries {
			for _, dep := range e.dependsOn {
				if dep == k {
					queue = append(queue, ck)
					break
				}
			}
		}
	}
}
