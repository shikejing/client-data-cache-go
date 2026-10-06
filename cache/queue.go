package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// WriteOp 是一次待应用的写入，ID 稳定且全局唯一，用于重放去重。
// Value 必须是可 JSON 序列化的值。
type WriteOp struct {
	ID    string
	Key   string
	Value any
}

// record 是持久化日志中的一条记录。
type record struct {
	State string  `json:"state"` // "pending" 或 "done"
	Op    WriteOp `json:"op"`
}

// WriteQueue 在网络中断时缓存写入，恢复后重放；
// 通过持久化日志区分未完成与已完成的写入，重启后不静默丢弃。
type WriteQueue struct {
	mu      sync.Mutex
	path    string
	pending []WriteOp
	done    map[string]struct{}
}

// NewWriteQueue 打开（必要时创建）位于 path 的写入队列，
// 并加载已有日志，恢复未完成/已完成状态。
func NewWriteQueue(path string) (*WriteQueue, error) {
	q := &WriteQueue{path: path, done: make(map[string]struct{})}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return q, nil
		}
		return nil, fmt.Errorf("cache: read write queue: %w", err)
	}
	dec := json.NewDecoder(bytesReader(data))
	for {
		var r record
		if err := dec.Decode(&r); err != nil {
			break // EOF 或末尾损坏：已加载的部分仍然有效
		}
		switch r.State {
		case "pending":
			q.pending = append(q.pending, r.Op)
		case "done":
			q.done[r.Op.ID] = struct{}{}
		}
	}
	// 过滤掉已完成的（日志中 pending 与 done 成对出现）。
	kept := q.pending[:0]
	for _, op := range q.pending {
		if _, ok := q.done[op.ID]; !ok {
			kept = append(kept, op)
		}
	}
	q.pending = kept
	return q, nil
}

// Enqueue 将一次写入记入队列并持久化。重复 ID 的写入只入队一次。
func (q *WriteQueue) Enqueue(op WriteOp) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.done[op.ID]; ok {
		return nil
	}
	for _, p := range q.pending {
		if p.ID == op.ID {
			return nil
		}
	}
	if err := q.appendLocked(record{State: "pending", Op: op}); err != nil {
		return err
	}
	q.pending = append(q.pending, op)
	return nil
}

// Pending 返回尚未确认完成的写入。
func (q *WriteQueue) Pending() []WriteOp {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]WriteOp(nil), q.pending...)
}

// Replay 对每条未完成写入调用 apply；apply 成功后标记完成并持久化。
// apply 必须幂等（按 op.ID 去重），以保证重放不重复生效。
// 某条写入失败时停止并返回错误，剩余写入保留在队列中等待下次重放。
func (q *WriteQueue) Replay(apply func(WriteOp) error) error {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.mu.Unlock()
			return nil
		}
		op := q.pending[0]
		q.mu.Unlock()

		if err := apply(op); err != nil {
			return fmt.Errorf("cache: replay op %s: %w", op.ID, err)
		}

		q.mu.Lock()
		if err := q.appendLocked(record{State: "done", Op: op}); err != nil {
			q.mu.Unlock()
			return err
		}
		q.done[op.ID] = struct{}{}
		q.pending = q.pending[1:]
		q.mu.Unlock()
	}
}

// appendLocked 追加一条记录到日志并落盘。调用方必须持有 q.mu。
func (q *WriteQueue) appendLocked(r record) error {
	f, err := os.OpenFile(q.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("cache: open write queue: %w", err)
	}
	defer f.Close()
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("cache: encode write op: %w", err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("cache: append write queue: %w", err)
	}
	return f.Sync()
}
