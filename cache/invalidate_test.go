package cache

import (
	"context"
	"testing"
	"time"
)

// 按标签失效：一次失效同时影响通过不同读取路径写入的相关缓存。
func TestInvalidateTag(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "s1", "user:1:profile", "profile-v1")
	_ = src.Apply(context.Background(), "s2", "user:1:settings", "settings-v1")
	_ = src.Apply(context.Background(), "s3", "user:2:profile", "other")
	c := New(Options{TTL: time.Minute})

	fetchTagged := func(ctx context.Context, key string) (FetchOutcome, error) {
		v, err := src.Get(ctx, key)
		if err != nil {
			return FetchOutcome{}, err
		}
		// 两条读取路径都归属同一实体的标签
		tag := "user:1"
		if key == "user:2:profile" {
			tag = "user:2"
		}
		return FetchOutcome{Value: v, Tags: []string{tag}}, nil
	}

	for _, k := range []string{"user:1:profile", "user:1:settings", "user:2:profile"} {
		if _, err := c.Get(context.Background(), k, fetchTagged); err != nil {
			t.Fatal(err)
		}
	}

	c.InvalidateTag("user:1")

	// user:1 的两条路径都失效，重新读取触发真实获取
	for _, k := range []string{"user:1:profile", "user:1:settings"} {
		if _, err := c.Get(context.Background(), k, fetchTagged); err != nil {
			t.Fatal(err)
		}
		if got := src.FetchCount(k); got != 2 {
			t.Fatalf("%s should be refetched after invalidation, fetch count %d", k, got)
		}
	}
	// user:2 不受影响
	if _, err := c.Get(context.Background(), "user:2:profile", fetchTagged); err != nil {
		t.Fatal(err)
	}
	if got := src.FetchCount("user:2:profile"); got != 1 {
		t.Fatalf("unrelated entry should stay cached, fetch count %d", got)
	}
}

// 级联失效：失效一个标识会连带失效依赖它的条目，且失效后读不到被淘汰的内容。
func TestInvalidateCascade(t *testing.T) {
	src := NewSource()
	_ = src.Apply(context.Background(), "s1", "user:1", "user-v1")
	_ = src.Apply(context.Background(), "s2", "user:1:orders", "orders-v1")
	_ = src.Apply(context.Background(), "s3", "user:1:orders:summary", "summary-v1")
	c := New(Options{TTL: time.Minute})

	fetchDep := func(deps ...string) FetchFunc {
		return func(ctx context.Context, key string) (FetchOutcome, error) {
			v, err := src.Get(ctx, key)
			if err != nil {
				return FetchOutcome{}, err
			}
			return FetchOutcome{Value: v, DependsOn: deps}, nil
		}
	}

	if _, err := c.Get(context.Background(), "user:1", fetchDep()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "user:1:orders", fetchDep("user:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "user:1:orders:summary", fetchDep("user:1:orders")); err != nil {
		t.Fatal(err)
	}

	// 源数据变更后失效根节点
	_ = src.Apply(context.Background(), "u1", "user:1", "user-v2")
	_ = src.Apply(context.Background(), "u2", "user:1:orders", "orders-v2")
	_ = src.Apply(context.Background(), "u3", "user:1:orders:summary", "summary-v2")
	c.Invalidate("user:1")

	// 三级缓存全部级联失效，读到的是新值而非被淘汰的内容
	want := map[string]string{
		"user:1":                "user-v2",
		"user:1:orders":         "orders-v2",
		"user:1:orders:summary": "summary-v2",
	}
	fetch := func(ctx context.Context, key string) (FetchOutcome, error) {
		v, err := src.Get(ctx, key)
		if err != nil {
			return FetchOutcome{}, err
		}
		return FetchOutcome{Value: v}, nil
	}
	for k, w := range want {
		v, err := c.Get(context.Background(), k, fetch)
		if err != nil {
			t.Fatal(err)
		}
		if v != w {
			t.Fatalf("%s: got %v, want %v (stale content survived invalidation)", k, v, w)
		}
		if got := src.FetchCount(k); got != 2 {
			t.Fatalf("%s should be refetched, fetch count %d", k, got)
		}
	}
}
