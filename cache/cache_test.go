package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func fetchFrom(s *Source) FetchFunc {
	return func(ctx context.Context, key string) (FetchOutcome, error) {
		v, err := s.Get(ctx, key)
		if err != nil {
			return FetchOutcome{}, err
		}
		return FetchOutcome{Value: v}, nil
	}
}

// 并发读取同一标识只触发一次真实获取，且所有读取得到一致结果。
func TestConcurrentReadsDedup(t *testing.T) {
	src := NewSource()
	src.SetLatency(50 * time.Millisecond)
	if err := src.Apply(context.Background(), "seed", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	c := New(Options{TTL: time.Minute})

	const n = 32
	var wg sync.WaitGroup
	results := make([]any, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.Get(context.Background(), "k", fetchFrom(src))
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = v
		}(i)
	}
	wg.Wait()

	if got := src.FetchCount("k"); got != 1 {
		t.Fatalf("expected 1 real fetch, got %d", got)
	}
	for i, v := range results {
		if v != "v1" {
			t.Fatalf("reader %d got %v, want v1", i, v)
		}
	}
}

// 获取完成后的读取必须命中缓存，不再触发真实获取。
func TestReadAfterCompletionHitsCache(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "seed", "k", "v1")
	c := New(Options{TTL: time.Minute})

	if _, err := c.Get(context.Background(), "k", fetchFrom(src)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		v, err := c.Get(context.Background(), "k", fetchFrom(src))
		if err != nil || v != "v1" {
			t.Fatalf("got %v, %v", v, err)
		}
	}
	if got := src.FetchCount("k"); got != 1 {
		t.Fatalf("expected 1 real fetch, got %d", got)
	}
}

// 过期后：SWR 模式直接返回旧值并后台刷新；刷新失败保留旧值。
func TestStaleWhileRevalidate(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "seed", "k", "v1")
	c := New(Options{TTL: 30 * time.Millisecond, StaleWhileRevalidate: true})

	if _, err := c.Get(context.Background(), "k", fetchFrom(src)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // 过期

	_ = src.Apply(context.Background(), "update", "k", "v2")
	v, err := c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil {
		t.Fatal(err)
	}
	if v != "v1" {
		t.Fatalf("stale read should return old value, got %v", v)
	}
	// 等待后台刷新完成
	deadline := time.Now().Add(2 * time.Second)
	for src.FetchCount("k") < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	v, _ = c.Get(context.Background(), "k", fetchFrom(src))
	if v != "v2" {
		t.Fatalf("after background refresh got %v, want v2", v)
	}

	// 刷新失败不得清掉已有数据
	time.Sleep(50 * time.Millisecond)
	src.SetDown(true)
	v, err = c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "v2" {
		t.Fatalf("stale read during outage got %v, %v", v, err)
	}
	time.Sleep(100 * time.Millisecond) // 给后台刷新失败留出时间
	v, err = c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "v2" {
		t.Fatalf("failed refresh must keep old data, got %v, %v", v, err)
	}
}

// 过期后：非 SWR 模式等待最新值。
func TestExpiredWaitsForFresh(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "seed", "k", "v1")
	c := New(Options{TTL: 30 * time.Millisecond})

	if _, err := c.Get(context.Background(), "k", fetchFrom(src)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	_ = src.Apply(context.Background(), "update", "k", "v2")

	v, err := c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "v2" {
		t.Fatalf("expired read should wait for fresh value, got %v, %v", v, err)
	}
}

// 取消的读取返回取消错误，但获取继续为其他等待者完成，缓存仍被填充。
func TestCancellationKeepsSnapshot(t *testing.T) {
	src := NewSource()
	src.SetLatency(80 * time.Millisecond)
	_ = src.Apply(context.Background(), "seed", "k", "v1")
	c := New(Options{TTL: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		_, err := c.Get(ctx, "k", fetchFrom(src))
		errCh <- err
	}()
	go func() {
		defer wg.Done()
		v, err := c.Get(context.Background(), "k", fetchFrom(src))
		if err != nil || v != "v1" {
			t.Errorf("other waiter got %v, %v", v, err)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()

	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read should return context.Canceled, got %v", err)
	}
	// 缓存已被共享的获取填充
	v, err := c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "v1" {
		t.Fatalf("cache should be populated, got %v, %v", v, err)
	}
	if got := src.FetchCount("k"); got != 1 {
		t.Fatalf("expected 1 real fetch, got %d", got)
	}
}

// 取消、失效与刷新并发发生时，读到的必须是某个自洽快照。
func TestConcurrentMutationConsistency(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "seed", "k", 0) // 播种初始值，保证读到的都是 int 快照
	c := New(Options{TTL: 5 * time.Millisecond, StaleWhileRevalidate: true})
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 写入端：不断更新数据源
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				i++
				_ = src.Apply(context.Background(), fmt.Sprintf("op-%d", i), "k", i)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 失效端
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.Invalidate("k")
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// 读取端：每次读到的必须是完整的 int 快照
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, err := c.Get(context.Background(), "k", fetchFrom(src))
					if err != nil {
						continue
					}
					if _, ok := v.(int); !ok {
						t.Errorf("read non-atomic snapshot: %#v", v)
						return
					}
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}
