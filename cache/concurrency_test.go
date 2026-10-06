package cache

import (
	"context"
	"sync"
	"testing"
	"time"
)

// snapshot 用于验证读到的一定是某个自洽快照，而非字段级混合。
type snapshot struct {
	A int
	B int
}

func TestConcurrentMutationSnapshotConsistency(t *testing.T) {
	src := NewSource()
	var mu sync.Mutex
	version := 0
	src.Set("k", snapshot{0, 0})
	fetcher := func(ctx context.Context, key string) (any, error) {
		mu.Lock()
		defer mu.Unlock()
		return snapshot{version, version}, nil
	}
	c := New(fetcher, Options{TTL: time.Millisecond})

	stop := make(chan struct{})
	var readers, writers sync.WaitGroup
	// 读者：任何时刻读到的 A 必须等于 B。
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, err := c.Get(context.Background(), "k", StaleWhileRevalidate)
				if err != nil {
					continue
				}
				s := v.(snapshot)
				if s.A != s.B {
					t.Errorf("torn read: %+v", s)
					return
				}
			}
		}()
	}
	// 写者：并发刷新、失效、取消。
	for i := 0; i < 4; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			for j := 0; j < 200; j++ {
				mu.Lock()
				version++
				mu.Unlock()
				switch j % 3 {
				case 0:
					c.Invalidate("k")
				case 1:
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_, _ = c.Get(ctx, "k", WaitFresh)
				default:
					_, _ = c.Get(context.Background(), "k", WaitFresh)
				}
			}
		}(i)
	}
	writers.Wait()
	close(stop)
	readers.Wait()
}
