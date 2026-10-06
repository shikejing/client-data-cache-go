package cache

import (
	"context"
	"testing"
	"time"
)

// 乐观写入在确认前即可被读到。
func TestOptimisticVisibleBeforeCommit(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "s1", "k", "v1")
	c := New(Options{TTL: time.Minute})

	if _, err := c.Get(context.Background(), "k", fetchFrom(src)); err != nil {
		t.Fatal(err)
	}
	tx := c.OptimisticSet("k", "v2-optimistic")
	v, err := c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "v2-optimistic" {
		t.Fatalf("optimistic value should be visible, got %v, %v", v, err)
	}
	tx.Commit()
	v, _ = c.Get(context.Background(), "k", fetchFrom(src))
	if v != "v2-optimistic" {
		t.Fatalf("after commit got %v", v)
	}
}

// 确认失败时回滚到乐观写入之前的状态，不残留中间态。
func TestOptimisticRollback(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "s1", "k", "v1")
	c := New(Options{TTL: time.Minute})

	if _, err := c.Get(context.Background(), "k", fetchFrom(src)); err != nil {
		t.Fatal(err)
	}
	tx := c.OptimisticSet("k", "v2-optimistic")
	tx.Rollback()

	v, err := c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "v1" {
		t.Fatalf("rollback should restore previous value, got %v, %v", v, err)
	}
	// 回滚后命中的是原缓存，不应触发新的真实获取
	if got := src.FetchCount("k"); got != 1 {
		t.Fatalf("rollback left intermediate state, fetch count %d", got)
	}
}

// 此前无缓存时回滚，不得残留任何中间态。
func TestOptimisticRollbackWithoutPrevious(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "s1", "k", "real")
	c := New(Options{TTL: time.Minute})

	tx := c.OptimisticSet("k", "ghost")
	tx.Rollback()

	// 缓存应为空，读取会触发真实获取并返回数据源的值
	v, err := c.Get(context.Background(), "k", fetchFrom(src))
	if err != nil || v != "real" {
		t.Fatalf("got %v, %v", v, err)
	}
	if got := src.FetchCount("k"); got != 1 {
		t.Fatalf("expected refetch after rollback, fetch count %d", got)
	}
}
