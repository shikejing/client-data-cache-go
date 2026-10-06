package cache

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrentReadsDedup(t *testing.T) {
	src := NewSource()
	src.Set("user:1", "alice")
	c := New(src.Fetch, Options{TTL: time.Minute})

	const n = 32
	var wg sync.WaitGroup
	results := make([]any, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.Get(context.Background(), "user:1", WaitFresh)
			results[i], errs[i] = v, err
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("reader %d got error: %v", i, errs[i])
		}
		if results[i] != "alice" {
			t.Fatalf("reader %d got %v, want alice", i, results[i])
		}
	}
	if got := src.FetchCount(); got != 1 {
		t.Fatalf("fetch count = %d, want 1 (singleflight)", got)
	}

	// 完成后的读取必须命中缓存，不再触发真实获取。
	v, err := c.Get(context.Background(), "user:1", WaitFresh)
	if err != nil || v != "alice" {
		t.Fatalf("cached read = %v, %v", v, err)
	}
	if got := src.FetchCount(); got != 1 {
		t.Fatalf("fetch count after cached read = %d, want 1", got)
	}
}

func TestStaleWhileRevalidateAndWaitFresh(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	src := NewSource()
	src.Set("k", "v1")
	c := New(src.Fetch, Options{TTL: 10 * time.Second, Now: clock})

	if v, _ := c.Get(context.Background(), "k", WaitFresh); v != "v1" {
		t.Fatalf("initial = %v", v)
	}

	// 过期后：StaleWhileRevalidate 立即返回旧值，后台刷新。
	src.Set("k", "v2")
	now = now.Add(time.Minute)
	if v, _ := c.Get(context.Background(), "k", StaleWhileRevalidate); v != "v1" {
		t.Fatalf("stale read = %v, want v1", v)
	}
	// 等后台刷新完成后应读到新值。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if v, _ := c.Get(context.Background(), "k", WaitFresh); v == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background refresh did not land")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 过期 + 刷新失败：WaitFresh 回退到旧值，且旧值不被清掉。
	src.SetOnline(false)
	now = now.Add(time.Minute)
	v, err := c.Get(context.Background(), "k", WaitFresh)
	if v != "v2" {
		t.Fatalf("failed refresh must keep old value, got %v (err=%v)", v, err)
	}
	if err == nil {
		t.Fatal("expected refresh error to be surfaced")
	}
	// 旧值仍在缓存中。
	if v, _ := c.Get(context.Background(), "k", StaleWhileRevalidate); v != "v2" {
		t.Fatalf("old value was cleared after failed refresh: %v", v)
	}
}

func TestCancelGetsConsistentSnapshot(t *testing.T) {
	src := NewSource()
	src.Set("k", "v1")
	c := New(src.Fetch, Options{TTL: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Get(ctx, "missing", WaitFresh)
	if err == nil {
		t.Fatal("canceled read should return error")
	}
	// 取消不得污染缓存：后续正常读取仍能获取。
	src.Set("missing", "v")
	if v, err := c.Get(context.Background(), "missing", WaitFresh); err != nil || v != "v" {
		t.Fatalf("read after cancel = %v, %v", v, err)
	}
}
