package cache

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// sourceApplier 把写入应用到模拟数据源；离线时失败。
func sourceApplier(src *Source) Applier {
	return func(ctx context.Context, op WriteOp) error {
		if _, err := src.Fetch(ctx, "__probe__"); err != nil {
			return err // 离线
		}
		src.Set(op.Key, op.Value)
		return nil
	}
}

func TestOfflineEnqueueAndReplay(t *testing.T) {
	src := NewSource()
	src.SetOnline(false)
	dir := t.TempDir()
	q := NewWriteQueue(filepath.Join(dir, "wal.log"), sourceApplier(src))

	// 断网时写入排队，不丢失。
	if err := q.Enqueue(context.Background(), WriteOp{ID: "w1", Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(context.Background(), WriteOp{ID: "w2", Key: "b", Value: "2"}); err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 2 {
		t.Fatalf("pending = %d, want 2", q.Pending())
	}

	// 恢复网络后重放。
	src.SetOnline(true)
	if err := q.Replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 0 {
		t.Fatalf("pending after replay = %d", q.Pending())
	}
	if v, _ := src.Fetch(context.Background(), "a"); v != "1" {
		t.Fatalf("a = %v", v)
	}
	if v, _ := src.Fetch(context.Background(), "b"); v != "2" {
		t.Fatalf("b = %v", v)
	}
}

func TestReplayIdempotentNoDuplicateEffect(t *testing.T) {
	var applies atomic.Int64
	dir := t.TempDir()
	applier := IdempotentApplier(filepath.Join(dir, "seen.json"), func(ctx context.Context, op WriteOp) error {
		applies.Add(1)
		return nil
	})
	wal := filepath.Join(dir, "wal.log")
	q := NewWriteQueue(wal, applier)

	if err := q.Enqueue(context.Background(), WriteOp{ID: "w1", Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	// 重放已完成的写入不得重复生效。
	if err := q.Replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := applies.Load(); got != 1 {
		t.Fatalf("applies = %d, want 1", got)
	}
	// 同 ID 重复入队也不得重复生效。
	if err := q.Enqueue(context.Background(), WriteOp{ID: "w1", Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if got := applies.Load(); got != 1 {
		t.Fatalf("applies after dup enqueue = %d, want 1", got)
	}
}

func TestRestartDistinguishesDoneAndPending(t *testing.T) {
	src := NewSource()
	dir := t.TempDir()
	wal := filepath.Join(dir, "wal.log")
	q := NewWriteQueue(wal, sourceApplier(src))

	// 在线完成一笔。
	if err := q.Enqueue(context.Background(), WriteOp{ID: "done1", Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	// 断网后排队一笔。
	src.SetOnline(false)
	if err := q.Enqueue(context.Background(), WriteOp{ID: "pend1", Key: "b", Value: "2"}); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：新队列实例从 WAL 恢复，只应看到未完成的那笔。
	q2 := NewWriteQueue(wal, sourceApplier(src))
	if q2.Pending() != 1 {
		t.Fatalf("pending after restart = %d, want 1", q2.Pending())
	}

	// 恢复网络重放：只有 pend1 生效，done1 不重复。
	src.SetOnline(true)
	if err := q2.Replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q2.Pending() != 0 {
		t.Fatalf("pending after replay = %d", q2.Pending())
	}
	if v, _ := src.Fetch(context.Background(), "b"); v != "2" {
		t.Fatalf("b = %v, want 2", v)
	}
}

func TestReplayKeepsPendingOnFailure(t *testing.T) {
	src := NewSource()
	src.SetOnline(false)
	q := NewWriteQueue("", sourceApplier(src))
	if err := q.Enqueue(context.Background(), WriteOp{ID: "w1", Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	// 仍离线时重放失败，写入不得被静默丢弃。
	if err := q.Replay(context.Background()); err == nil {
		t.Fatal("expected replay error while offline")
	}
	if q.Pending() != 1 {
		t.Fatalf("pending = %d, want 1 (not silently dropped)", q.Pending())
	}
}
