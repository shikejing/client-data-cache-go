package cache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// 断网时写入排队，恢复后重放生效。
func TestOfflineEnqueueAndReplay(t *testing.T) {
	dir := t.TempDir()
	src := NewSource()
	src.SetDown(true) // 模拟断网

	q, err := NewWriteQueue(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	// 断网期间：写入无法直达，进入队列
	if err := src.Apply(context.Background(), "op-1", "a", 1); err == nil {
		t.Fatal("expected source down error")
	}
	if err := q.Enqueue(WriteOp{ID: "op-1", Key: "a", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(WriteOp{ID: "op-2", Key: "b", Value: 2}); err != nil {
		t.Fatal(err)
	}
	if got := len(q.Pending()); got != 2 {
		t.Fatalf("expected 2 pending ops, got %d", got)
	}

	// 网络恢复：重放
	src.SetDown(false)
	err = q.Replay(func(op WriteOp) error {
		return src.Apply(context.Background(), op.ID, op.Key, op.Value)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(q.Pending()); got != 0 {
		t.Fatalf("expected 0 pending ops after replay, got %d", got)
	}
	if v, _ := src.Value("a"); v != 1 {
		t.Fatalf("a = %v, want 1", v)
	}
	if v, _ := src.Value("b"); v != 2 {
		t.Fatalf("b = %v, want 2", v)
	}
}

// 重放不重复生效：同一 op 重复应用只生效一次。
func TestReplayIdempotent(t *testing.T) {
	dir := t.TempDir()
	src := NewSource()
	q, err := NewWriteQueue(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(WriteOp{ID: "op-1", Key: "counter", Value: "applied"}); err != nil {
		t.Fatal(err)
	}
	apply := func(op WriteOp) error {
		return src.Apply(context.Background(), op.ID, op.Key, op.Value)
	}
	if err := q.Replay(apply); err != nil {
		t.Fatal(err)
	}
	// 再次手动应用同一 op（模拟重放路径上的重复投递）
	if err := apply(WriteOp{ID: "op-1", Key: "counter", Value: "applied-twice"}); err != nil {
		t.Fatal(err)
	}
	if v, _ := src.Value("counter"); v != "applied" {
		t.Fatalf("duplicate apply took effect: %v", v)
	}
}

// 重启后未完成与已完成的写入可区分，未完成的不会静默丢弃。
func TestRestartDistinguishesPendingAndDone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	src := NewSource()

	q, err := NewWriteQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Enqueue(WriteOp{ID: "op-1", Key: "a", Value: 1})
	_ = q.Enqueue(WriteOp{ID: "op-2", Key: "b", Value: 2})
	_ = q.Enqueue(WriteOp{ID: "op-3", Key: "c", Value: 3})

	// 只重放成功第一条，然后"崩溃"（op-2 应用失败即停止）
	src.SetDown(false)
	attempts := 0
	err = q.Replay(func(op WriteOp) error {
		attempts++
		if op.ID == "op-2" {
			return ErrSourceDown // 模拟重放中途再次断网
		}
		return src.Apply(context.Background(), op.ID, op.Key, op.Value)
	})
	if err == nil {
		t.Fatal("expected replay to stop on failure")
	}

	// 重启：重新打开队列
	q2, err := NewWriteQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	pending := q2.Pending()
	if len(pending) != 2 || pending[0].ID != "op-2" || pending[1].ID != "op-3" {
		t.Fatalf("pending after restart = %+v, want op-2 and op-3", pending)
	}
	// 已完成的 op-1 不在待重放列表中，且其效果已存在
	if v, _ := src.Value("a"); v != 1 {
		t.Fatalf("completed op-1 effect missing: %v", v)
	}

	// 恢复后重放剩余写入，全部生效，无静默丢弃
	err = q2.Replay(func(op WriteOp) error {
		return src.Apply(context.Background(), op.ID, op.Key, op.Value)
	})
	if err != nil {
		t.Fatal(err)
	}
	// 注意：经队列 JSON 持久化的值会按 JSON 语义还原（数字变为 float64），
	// 这里按值的文本形式比较。
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		v, _ := src.Value(k)
		if fmt.Sprint(v) != want {
			t.Fatalf("%s = %v, want %s", k, v, want)
		}
	}
	if got := len(q2.Pending()); got != 0 {
		t.Fatalf("expected empty queue, got %d pending", got)
	}
}

// 断网期间重放失败，写入保留在队列中，不静默丢弃。
func TestReplayFailureKeepsOps(t *testing.T) {
	dir := t.TempDir()
	src := NewSource()
	src.SetDown(true)

	q, err := NewWriteQueue(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Enqueue(WriteOp{ID: "op-1", Key: "a", Value: 1})

	err = q.Replay(func(op WriteOp) error {
		return src.Apply(context.Background(), op.ID, op.Key, op.Value)
	})
	if err == nil {
		t.Fatal("expected replay error while down")
	}
	if got := len(q.Pending()); got != 1 {
		t.Fatalf("failed replay must keep ops, pending = %d", got)
	}
}
