package cache

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrSourceDown 在模拟网络中断时返回。
var ErrSourceDown = errors.New("cache: source is unreachable")

// Source 是进程内模拟的数据源/服务，支持延迟与故障注入，
// 用于在本地复现网络中断等场景，不依赖任何外部服务。
type Source struct {
	mu       sync.Mutex
	data     map[string]any
	applied  map[string]struct{}
	latency  time.Duration
	down     bool
	fetchCnt map[string]int
}

// NewSource 创建一个空数据源。
func NewSource() *Source {
	return &Source{
		data:     make(map[string]any),
		applied:  make(map[string]struct{}),
		fetchCnt: make(map[string]int),
	}
}

// SetLatency 设置每次访问的模拟延迟。
func (s *Source) SetLatency(d time.Duration) {
	s.mu.Lock()
	s.latency = d
	s.mu.Unlock()
}

// SetDown 模拟网络中断/恢复。
func (s *Source) SetDown(down bool) {
	s.mu.Lock()
	s.down = down
	s.mu.Unlock()
}

// Get 读取一个键。
func (s *Source) Get(ctx context.Context, key string) (any, error) {
	s.mu.Lock()
	latency, down := s.latency, s.down
	s.mu.Unlock()

	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if down {
		return nil, ErrSourceDown
	}
	s.fetchCnt[key]++
	v, ok := s.data[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

// Apply 以幂等方式应用一次写入：同一 opID 重复提交不会重复生效。
func (s *Source) Apply(ctx context.Context, opID, key string, value any) error {
	s.mu.Lock()
	latency, down := s.latency, s.down
	s.mu.Unlock()

	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if down {
		return ErrSourceDown
	}
	if _, ok := s.applied[opID]; ok {
		return nil // 幂等：已应用过的写入直接确认
	}
	s.applied[opID] = struct{}{}
	s.data[key] = value
	return nil
}

// FetchCount 返回某个键被真实读取的次数，用于验证去重。
func (s *Source) FetchCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fetchCnt[key]
}

// Value 直接查看当前值（测试辅助）。
func (s *Source) Value(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}
