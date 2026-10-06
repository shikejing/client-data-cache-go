package cache

import (
	"context"
	"testing"
	"time"
)

func TestInvalidateByKey(t *testing.T) {
	src := NewSource()
	src.Set("a", "1")
	c := New(src.Fetch, Options{TTL: time.Minute})

	if _, err := c.Get(context.Background(), "a", WaitFresh); err != nil {
		t.Fatal(err)
	}
	c.Invalidate("a")

	src.Set("a", "2")
	v, _ := c.Get(context.Background(), "a", WaitFresh)
	if v != "2" {
		t.Fatalf("after invalidate read = %v, want refetched 2", v)
	}
	if got := src.FetchCount(); got != 2 {
		t.Fatalf("fetch count = %d, want 2", got)
	}
}

func TestInvalidateTagCascade(t *testing.T) {
	src := NewSource()
	src.Set("user:1", "alice")
	src.Set("user:1/posts", "p1")
	src.Set("other", "x")
	c := New(src.Fetch, Options{TTL: time.Minute})

	// 同一数据通过不同读取路径写入，但都带 user:1 标签。
	if _, err := c.Get(context.Background(), "user:1", WaitFresh, "user:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "user:1/posts", WaitFresh, "user:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "other", WaitFresh, "misc"); err != nil {
		t.Fatal(err)
	}

	// 一次标签失效级联到所有相关缓存。
	c.InvalidateTag("user:1")

	src.Set("user:1", "alice2")
	src.Set("user:1/posts", "p2")
	v1, _ := c.Get(context.Background(), "user:1", WaitFresh)
	v2, _ := c.Get(context.Background(), "user:1/posts", WaitFresh)
	if v1 != "alice2" || v2 != "p2" {
		t.Fatalf("cascaded entries not refetched: %v %v", v1, v2)
	}
	// 未受影响的标签不得被误伤。
	before := src.FetchCount()
	if v, _ := c.Get(context.Background(), "other", WaitFresh); v != "x" {
		t.Fatalf("unrelated entry = %v", v)
	}
	if src.FetchCount() != before {
		t.Fatal("unrelated entry was refetched")
	}
}
