package durable

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FileStore 是以本地目录承载执行状态的 Store 实现：
//   - 状态文件原子写（临时文件 + rename），写时按执行互斥；
//   - 进程级文件锁（lock 文件）保证同一时刻只有一个 Runtime 打开目录；
//   - 事件按执行追加到独立日志文件；
//   - 幂等占用以 O_CREATE|O_EXCL 互斥建档。
type FileStore struct {
	dir string

	// lockFile 持有进程级 flock。
	lockFile *os.File

	writeMu sync.Map // execID -> *sync.Mutex，串行化同执行的读-改-写
	evMu    sync.Map // execID -> *sync.Mutex
	clmMu   sync.Map // claimKey -> *sync.Mutex，避免读到写一半的占用文件
}

// NewFileStore 打开（必要时创建）状态目录并获取进程级锁。
func NewFileStore(ctx context.Context, dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("durable: empty store directory")
	}
	for _, sub := range []string{"exec", "events", "claims"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("durable: init dir: %w", err)
		}
	}
	lf, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("durable: open lock: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("durable: state dir is locked by another process: %w", err)
	}
	s := &FileStore{dir: dir, lockFile: lf}
	go func() { _ = ctx.Done() }() // ctx 仅用于满足调用期生命周期约定
	return s, nil
}

func (s *FileStore) execMu(id string) *sync.Mutex {
	m, _ := s.writeMu.LoadOrStore(id, &sync.Mutex{})
	return m.(*sync.Mutex)
}

func (s *FileStore) eventMu(id string) *sync.Mutex {
	m, _ := s.evMu.LoadOrStore(id, &sync.Mutex{})
	return m.(*sync.Mutex)
}

func (s *FileStore) execPath(id string) string {
	return filepath.Join(s.dir, "exec", id+".json")
}

func (s *FileStore) eventsPath(id string) string {
	return filepath.Join(s.dir, "events", id+".log")
}

func (s *FileStore) claimPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, "claims", hex.EncodeToString(sum[:])+".json")
}

// SaveExecution 使用期望版本号做 CAS：
//   - expectedRev == 0：执行文件必须尚不存在（新建）；
//   - expectedRev > 0：磁盘版本必须等于 expectedRev。
//
// 成功后返回新版本号（在旧版本上 +1；新建返回 1）。
func (s *FileStore) SaveExecution(ctx context.Context, e *Execution, expectedRev int64) (int64, error) {
	if e == nil || e.ID == "" {
		return 0, errors.New("durable: save execution requires id")
	}
	mu := s.execMu(e.ID)
	mu.Lock()
	defer mu.Unlock()

	path := s.execPath(e.ID)
	cur, err := readRevision(path)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return 0, err
	}
	if expectedRev == 0 {
		if cur != 0 {
			return 0, ErrConflict
		}
	} else if cur != expectedRev {
		return 0, ErrConflict
	}

	newRev := expectedRev + 1
	e.Rev = newRev
	if err := writeJSONAtomic(path, e); err != nil {
		return 0, err
	}
	return newRev, nil
}

func readRevision(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	var head struct {
		Rev int64 `json:"rev"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return 0, fmt.Errorf("durable: decode revision %s: %w", path, err)
	}
	return head.Rev, nil
}

func (s *FileStore) GetExecution(ctx context.Context, id string) (*Execution, int64, error) {
	path := s.execPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	var e Execution
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, 0, fmt.Errorf("durable: decode execution %s: %w", id, err)
	}
	return &e, e.Rev, nil
}

func (s *FileStore) ListExecutionIDs(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "exec"))
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, en := range entries {
		if en.IsDir() || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(en.Name(), ".json"))
	}
	return ids, nil
}

// CreateClaim 互斥建档：键已存在时返回 ErrClaimExists。
// 先写临时文件并 fsync，再以 link(2) 原子落到目标路径——
// link 在 POSIX 上要么原子新建、要么返回 EEXIST，读者永远看不到半成品。
func (s *FileStore) CreateClaim(ctx context.Context, c IdempotencyClaim) error {
	if c.Key == "" {
		return errors.New("durable: claim requires key")
	}
	mu := s.claimMu(c.Key)
	mu.Lock()
	defer mu.Unlock()

	target := s.claimPath(c.Key)
	if _, err := os.Stat(target); err == nil {
		return ErrClaimExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	claimsDir := filepath.Join(s.dir, "claims")
	tmp, err := os.CreateTemp(claimsDir, ".claim-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// link 要求目标不存在；已存在时返回 EEXIST（映射为 ErrClaimExists）。
	if err := os.Link(tmpName, target); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrClaimExists
		}
		return err
	}
	return syncDir(claimsDir)
}

func (s *FileStore) claimMu(key string) *sync.Mutex {
	m, _ := s.clmMu.LoadOrStore(key, &sync.Mutex{})
	return m.(*sync.Mutex)
}

func (s *FileStore) GetClaim(ctx context.Context, key string) (IdempotencyClaim, error) {
	var c IdempotencyClaim
	data, err := os.ReadFile(s.claimPath(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return c, ErrNotFound
		}
		return c, err
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("durable: decode claim: %w", err)
	}
	return c, nil
}

func (s *FileStore) ListClaims(ctx context.Context) ([]IdempotencyClaim, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "claims"))
	if err != nil {
		return nil, err
	}
	out := make([]IdempotencyClaim, 0, len(entries))
	for _, en := range entries {
		if en.IsDir() || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		var c IdempotencyClaim
		data, err := os.ReadFile(filepath.Join(s.dir, "claims", en.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("durable: decode claim %s: %w", en.Name(), err)
		}
		out = append(out, c)
	}
	return out, nil
}

func (s *FileStore) AppendEvent(ctx context.Context, execID string, ev Event) error {
	mu := s.eventMu(execID)
	mu.Lock()
	defer mu.Unlock()

	// 在同一互斥区内完成“读取末序号 -> 分配序号 -> 追加”，
	// 保证同一执行的事件序号在任意并发路径下严格单调递增且绝不覆盖。
	path := s.eventsPath(execID)
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	if ev.Seq == 0 {
		last, err := lastEventSeq(path)
		if err != nil {
			return err
		}
		ev.Seq = last + 1
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func countEventLines(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	return bytes.Count(data, []byte{'\n'}), nil
}

// lastEventSeq 返回日志中最后一条事件的序号；无文件/空文件返回 0。
func lastEventSeq(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	trimmed := bytes.TrimRight(data, "\n")
	if len(trimmed) == 0 {
		return 0, nil
	}
	idx := bytes.LastIndexByte(trimmed, '\n')
	last := trimmed[idx+1:]
	var head struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(last, &head); err != nil {
		return 0, fmt.Errorf("durable: decode last event seq: %w", err)
	}
	return head.Seq, nil
}

func (s *FileStore) ReadEvents(ctx context.Context, execID string) ([]Event, error) {
	f, err := os.Open(s.eventsPath(execID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("durable: decode event: %w", err)
		}
		out = append(out, ev)
	}
	return out, sc.Err()
}

func (s *FileStore) Close() error {
	if s.lockFile != nil {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		return s.lockFile.Close()
	}
	return nil
}

// writeJSONAtomic 将 v 以“临时文件 + fsync + rename”原子落盘。
func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	return d.Sync()
}
