// Package cache 提供客户端数据获取与缓存层。
package cache

import (
	"context"
	"errors"
	"sync"
)

// ErrOffline 表示模拟网络处于中断状态。
var ErrOffline = errors.New("cache: network offline")

// Fetcher 按稳定标识获取一份数据。返回的值必须是不可变快照。
type Fetcher func(ctx context.Context, key string) (any, error)

// Source 是进程内模拟的数据服务，可控制网络通断与失败注入。
type Source struct {
	mu      sync.Mutex
	data    map[string]any
	online  bool
	fetches int
}

// NewSource 创建一个在线的模拟数据源。
func NewSource() *Source {
	return &Source{data: map[string]any{}, online: true}
}

// Set 直接写入数据源（模拟服务端状态）。
func (s *Source) Set(key string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
}

// SetOnline 切换模拟网络的通断。
func (s *Source) SetOnline(online bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.online = online
}

// FetchCount 返回真实获取发生的次数，用于验证去重。
func (s *Source) FetchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fetches
}

// Fetch 实现一次真实获取；离线时返回 ErrOffline。
func (s *Source) Fetch(ctx context.Context, key string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.online {
		return nil, ErrOffline
	}
	s.fetches++
	v, ok := s.data[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}
