package cache

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

// WriteOp 是一条待确认/待重放的写入操作。
type WriteOp struct {
	ID    string // 幂等标识，重放时用于去重
	Key   string
	Value any
}

// Applier 把一条写入操作应用到目的地；必须按 ID 幂等，
// 因为崩溃可能发生在“已生效但尚未标记完成”之间。
type Applier func(ctx context.Context, op WriteOp) error

// walRecord 是持久化日志的一行。
type walRecord struct {
	Kind string   `json:"kind"` // "add" 或 "done"
	Op   *WriteOp `json:"op,omitempty"`
	ID   string   `json:"id,omitempty"`
}

// WriteQueue 在网络中断时排队写入，恢复后重放。
// 持久化到 WAL 文件后，重启可区分已完成与未完成写入，且不会静默丢弃。
type WriteQueue struct {
	mu      sync.Mutex
	path    string
	applier Applier
	pending map[string]WriteOp // 未完成写入
}

// NewWriteQueue 创建队列并加载 WAL；path 为空时仅内存排队。
func NewWriteQueue(path string, applier Applier) *WriteQueue {
	q := &WriteQueue{path: path, applier: applier, pending: map[string]WriteOp{}}
	if path != "" {
		q.load()
	}
	return q
}

// load 回放 WAL，重建未完成集合；已完成的（有 done 标记）不会进入 pending。
func (q *WriteQueue) load() {
	f, err := os.Open(q.path)
	if err != nil {
		return // 无日志即空队列
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec walRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue // 跳过损坏行（如崩溃时的半行），不静默丢弃其它记录
		}
		switch rec.Kind {
		case "add":
			if rec.Op != nil {
				q.pending[rec.Op.ID] = *rec.Op
			}
		case "done":
			delete(q.pending, rec.ID)
		}
	}
}

// append 追加一条 WAL 记录并落盘。
func (q *WriteQueue) append(rec walRecord) error {
	if q.path == "" {
		return nil
	}
	f, err := os.OpenFile(q.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// Enqueue 追加一条写入。先入 WAL（不丢），再尝试立即生效；
// 离线或失败时保留在队列中，等待 Replay。
func (q *WriteQueue) Enqueue(ctx context.Context, op WriteOp) error {
	if op.ID == "" {
		return errors.New("cache: WriteOp.ID 不能为空")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, dup := q.pending[op.ID]; dup {
		return nil // 同 ID 重复入队不重复生效
	}
	if err := q.append(walRecord{Kind: "add", Op: &op}); err != nil {
		return err
	}
	q.pending[op.ID] = op
	// 立即生效失败（如离线）不算入队失败：写入已在 WAL 中，留待 Replay。
	_ = q.applyLocked(ctx, op)
	return nil
}

// Replay 重放所有未完成的写入；已完成的跳过。
func (q *WriteQueue) Replay(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	var firstErr error
	for _, op := range q.pending {
		if err := q.applyLocked(ctx, op); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// applyLocked 应用单条写入；成功后写 done 标记并从 pending 移除。
func (q *WriteQueue) applyLocked(ctx context.Context, op WriteOp) error {
	if q.applier == nil {
		return nil
	}
	if err := q.applier(ctx, op); err != nil {
		return err // 保留在 pending，等待下次重放
	}
	if err := q.append(walRecord{Kind: "done", ID: op.ID}); err != nil {
		return err
	}
	delete(q.pending, op.ID)
	return nil
}

// Pending 返回未完成写入数。
func (q *WriteQueue) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// IdempotentApplier 包装一个 Applier，用持久化的已生效 ID 集合去重，
// 保证“已生效但 done 标记未落盘”的崩溃场景下重放不重复生效。
func IdempotentApplier(seenPath string, applier Applier) Applier {
	var mu sync.Mutex
	seen := map[string]struct{}{}
	if seenPath != "" {
		if b, err := os.ReadFile(seenPath); err == nil {
			_ = json.Unmarshal(b, &seen)
		}
	}
	return func(ctx context.Context, op WriteOp) error {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := seen[op.ID]; ok {
			return nil
		}
		if err := applier(ctx, op); err != nil {
			return err
		}
		seen[op.ID] = struct{}{}
		if seenPath != "" {
			b, _ := json.Marshal(seen)
			_ = os.WriteFile(seenPath, b, 0o644)
		}
		return nil
	}
}
