package cache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOptimisticWriteVisibleBeforeConfirm(t *testing.T) {
	src := NewSource()
	src.Set("k", "old")
	c := New(src.Fetch, Options{TTL: time.Minute})
	if _, err := c.Get(context.Background(), "k", WaitFresh); err != nil {
		t.Fatal(err)
	}

	confirmed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.OptimisticWrite(context.Background(), Mutation{
			Key:   "k",
			Value: "new",
			Confirm: func(ctx context.Context) error {
				<-confirmed // 确认前阻塞
				return nil
			},
		})
	}()

	// 确认之前就能读到乐观值。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if v, _ := c.Get(context.Background(), "k", WaitFresh); v == "new" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("optimistic value not visible before confirm")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(confirmed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get(context.Background(), "k", WaitFresh); v != "new" {
		t.Fatalf("after confirm = %v, want new", v)
	}
}

func TestOptimisticWriteRollback(t *testing.T) {
	src := NewSource()
	src.Set("k", "old")
	c := New(src.Fetch, Options{TTL: time.Minute})
	if _, err := c.Get(context.Background(), "k", WaitFresh); err != nil {
		t.Fatal(err)
	}

	reject := errors.New("rejected by server")
	err := c.OptimisticWrite(context.Background(), Mutation{
		Key:     "k",
		Value:   "new",
		Confirm: func(ctx context.Context) error { return reject },
	})
	if !errors.Is(err, reject) {
		t.Fatalf("err = %v, want reject", err)
	}
	// 回滚到写入前状态，不残留中间态。
	if v, _ := c.Get(context.Background(), "k", WaitFresh); v != "old" {
		t.Fatalf("after rollback = %v, want old", v)
	}
}

func TestOptimisticWriteRollbackNoPrior(t *testing.T) {
	src := NewSource()
	c := New(src.Fetch, Options{TTL: time.Minute})

	err := c.OptimisticWrite(context.Background(), Mutation{
		Key:     "fresh-key",
		Value:   "temp",
		Confirm: func(ctx context.Context) error { return errors.New("nope") },
	})
	if err == nil {
		t.Fatal("expected error")
	}
	// 写入前不存在，回滚后也不得残留。
	src.Set("fresh-key", "real")
	if v, _ := c.Get(context.Background(), "fresh-key", WaitFresh); v != "real" {
		t.Fatalf("rollback left residue, got %v", v)
	}
}
