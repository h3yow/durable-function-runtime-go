package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store 是执行状态的持久化存储接口。
// 所有方法必须并发安全。
type Store interface {
	// CreateIfAbsent 按幂等键创建执行；若键已存在返回已存在的执行与 created=false。
	CreateIfAbsent(ctx context.Context, exec *Execution) (stored *Execution, created bool, err error)
	// Get 按执行 ID 读取，不存在返回 ErrExecutionNotFound。
	Get(ctx context.Context, id string) (*Execution, error)
	// GetByIdempotencyKey 按幂等键读取，不存在返回 nil, nil。
	GetByIdempotencyKey(ctx context.Context, key string) (*Execution, error)
	// CompareAndSwap 基于 exec.Version 做乐观并发写回；版本不匹配返回 ErrConcurrencyConflict。
	CompareAndSwap(ctx context.Context, exec *Execution) error
	// ListIncomplete 返回所有非终态执行。
	ListIncomplete(ctx context.Context) ([]*Execution, error)
}

// FileStore 用单个 JSON 文件承载全部执行状态，写盘采用 tmp+rename 保证原子性。
// 进程重启后重新打开同一文件即可恢复全部执行状态。
type FileStore struct {
	mu    sync.Mutex
	path  string
	byID  map[string]*Execution
	byKey map[string]string // idempotencyKey -> id
}

type fileStoreData struct {
	Executions []*Execution `json:"executions"`
}

// NewFileStore 打开（必要时创建）位于 path 的存储文件并加载已有状态。
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, byID: map[string]*Execution{}, byKey: map[string]string{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("durable: read store: %w", err)
	}
	if len(data) == 0 {
		return s, nil
	}
	var d fileStoreData
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("durable: decode store: %w", err)
	}
	for _, e := range d.Executions {
		s.byID[e.ID] = e
		s.byKey[e.IdempotencyKey] = e.ID
	}
	return s, nil
}

// marshalJSON 序列化时不做 HTML 转义，保证存储内容与提交内容字节级一致。
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// persist 必须在持锁状态下调用。
func (s *FileStore) persistLocked() error {
	d := fileStoreData{Executions: make([]*Execution, 0, len(s.byID))}
	for _, e := range s.byID {
		d.Executions = append(d.Executions, e)
	}
	data, err := marshalJSON(d)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// clone 深拷贝执行，避免调用方与存储共享可变状态。
func clone(exec *Execution) *Execution {
	data, err := marshalJSON(exec)
	if err != nil {
		panic(err)
	}
	var out Execution
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return &out
}

func (s *FileStore) CreateIfAbsent(ctx context.Context, exec *Execution) (*Execution, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.byKey[exec.IdempotencyKey]; ok {
		return clone(s.byID[id]), false, nil
	}
	stored := clone(exec)
	stored.Version = 1
	stored.CreatedAt = time.Now()
	stored.UpdatedAt = stored.CreatedAt
	s.byID[stored.ID] = stored
	s.byKey[stored.IdempotencyKey] = stored.ID
	if err := s.persistLocked(); err != nil {
		delete(s.byID, stored.ID)
		delete(s.byKey, stored.IdempotencyKey)
		return nil, false, err
	}
	return clone(stored), true, nil
}

func (s *FileStore) Get(ctx context.Context, id string) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byID[id]
	if !ok {
		return nil, ErrExecutionNotFound
	}
	return clone(e), nil
}

func (s *FileStore) GetByIdempotencyKey(ctx context.Context, key string) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byKey[key]
	if !ok {
		return nil, nil
	}
	return clone(s.byID[id]), nil
}

func (s *FileStore) CompareAndSwap(ctx context.Context, exec *Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[exec.ID]
	if !ok {
		return ErrExecutionNotFound
	}
	if cur.Version != exec.Version {
		return ErrConcurrencyConflict
	}
	stored := clone(exec)
	stored.Version = cur.Version + 1
	stored.UpdatedAt = time.Now()
	s.byID[stored.ID] = stored
	if err := s.persistLocked(); err != nil {
		s.byID[stored.ID] = cur
		return err
	}
	exec.Version = stored.Version
	return nil
}

func (s *FileStore) ListIncomplete(ctx context.Context) ([]*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Execution
	for _, e := range s.byID {
		if !e.Status.Terminal() {
			out = append(out, clone(e))
		}
	}
	return out, nil
}

// 确保编译期检查接口实现。
var _ Store = (*FileStore)(nil)

// Dir 返回存储文件所在目录，便于测试断言文件位置。
func (s *FileStore) Dir() string { return filepath.Dir(s.path) }
