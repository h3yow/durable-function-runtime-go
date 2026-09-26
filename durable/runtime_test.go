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
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 把运行时日志同时输出到 t.Log，使幂等键、重试依据、版本判定可见。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("[runtime] "+format, args...)
}

func newTestRuntime(t *testing.T, reg *Registry, path string, workers int) (*Runtime, context.CancelFunc) {
	t.Helper()
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	rt := NewRuntime(store, reg, Options{
		Workers:      workers,
		PollInterval: 5 * time.Millisecond,
		Logger:       testLogger{t},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = rt.Run(ctx) }()
	return rt, cancel
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within timeout")
	}
}

func waitTerminal(t *testing.T, rt *Runtime, id string) *Execution {
	t.Helper()
	var got *Execution
	waitFor(t, 3*time.Second, func() bool {
		e, err := rt.Get(context.Background(), id)
		if err != nil {
			return false
		}
		got = e
		return e.Status.Terminal()
	})
	return got
}

func noopStep(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
	return in, nil
}

// TestConcurrentIdempotentSubmit 验证同一幂等键的并发重复提交收敛到同一执行；
// 同键不同内容被明确拒绝并给出可区分原因。
func TestConcurrentIdempotentSubmit(t *testing.T) {
	dir := t.TempDir()
	reg := NewRegistry()
	reg.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "s1", Func: noopStep},
	}})
	rt, cancel := newTestRuntime(t, reg, filepath.Join(dir, "store.json"), 4)
	defer cancel()

	const n = 32
	var created int64
	ids := make(chan string, n)
	var wg sync.WaitGroup
	idem := "idem-concurrent-001"
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, createdNow, err := rt.Submit(context.Background(), "flow", "v1", idem, []byte(`{"a":1}`))
			if err != nil {
				t.Errorf("unexpected submit error: %v", err)
				return
			}
			if createdNow {
				atomic.AddInt64(&created, 1)
			}
			ids <- e.ID
		}()
	}
	wg.Wait()
	close(ids)
	if created != 1 {
		t.Fatalf("expected exactly 1 created, got %d", created)
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("idempotency did not converge: %s vs %s", first, id)
		}
	}

	// 同键不同输入/定义：必须以可区分原因拒绝。
	if _, _, err := rt.Submit(context.Background(), "flow", "v1", idem, []byte(`{"a":2}`)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
	if _, _, err := rt.Submit(context.Background(), "flow", "v9", "another-key", nil); !errors.Is(err, ErrVersionIncompatible) {
		t.Fatalf("expected ErrVersionIncompatible, got %v", err)
	}
	if _, _, err := rt.Submit(context.Background(), "missing", "v1", "k3", nil); !errors.Is(err, ErrDefinitionUnknown) {
		t.Fatalf("expected ErrDefinitionUnknown, got %v", err)
	}
}

// TestRestartRecovery 验证进程重启后从未完成步骤继续，已完成步骤不重复产生副作用。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")

	var s1Calls int64
	var s2Calls int64
	reg := NewRegistry()
	reg.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "s1", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			atomic.AddInt64(&s1Calls, 1)
			return []byte(`"after-s1"`), nil
		}},
		{Name: "s2", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			// s2 在第一轮运行中不断瞬时失败，制造重启窗口。
			atomic.AddInt64(&s2Calls, 1)
			if atomic.LoadInt64(&s2Calls) == 1 {
				return nil, errors.New("transient boom")
			}
			return []byte(`"after-s1 -> s2"`), nil
		}, Retry: RetryPolicy{MaxAttempts: 5, BaseDelay: 200 * time.Millisecond}},
	}})
	rt1, cancel1 := newTestRuntime(t, reg, path, 2)
	e, created, err := rt1.Submit(context.Background(), "flow", "v1", "idem-restart-001", nil)
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	// 等到 s1 已完成、s2 已开始（Running/Pending）。
	waitFor(t, 2*time.Second, func() bool {
		cur, _ := rt1.Get(context.Background(), e.ID)
		return cur != nil && len(cur.Steps) == 2 && cur.Steps[0].Status == StatusSucceeded
	})
	// 模拟进程退出（此时 s2 处于退避中）。
	cancel1()
	time.Sleep(30 * time.Millisecond) // 让 Run goroutine 退出
	if got := atomic.LoadInt64(&s1Calls); got != 1 {
		t.Fatalf("s1 side effect count before restart = %d, want 1", got)
	}

	// 用全新运行时（全新计数器）打开同一存储文件，模拟重启恢复。
	var s1Calls2 int64
	reg2 := NewRegistry()
	reg2.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "s1", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			atomic.AddInt64(&s1Calls2, 1)
			return []byte(`"after-s1"`), nil
		}},
		{Name: "s2", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			if string(in) != `"after-s1"` {
				return nil, fmt.Errorf("s2 input must come from persisted s1 output, got %s", in)
			}
			return []byte(`"after-s1 -> s2"`), nil
		}},
	}})
	rt2, cancel2 := newTestRuntime(t, reg2, path, 2)
	defer cancel2()
	final := waitTerminal(t, rt2, e.ID)
	if final.Status != StatusSucceeded {
		t.Fatalf("final status = %s kind=%s reason=%s", final.Status, final.FailureKind, final.FailureReason)
	}
	if got := atomic.LoadInt64(&s1Calls2); got != 0 {
		t.Fatalf("s1 re-executed after restart: %d times (must not repeat side effects)", got)
	}
	if string(final.Output) != `"after-s1 -> s2"` {
		t.Fatalf("unexpected output: %s", final.Output)
	}
	// 存储文件必须真实落盘。
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("store file not persisted: %v", err)
	}
}

// TestRetryExhausted 验证瞬时失败按上限退避重试，达到上限进入可区分终态。
func TestRetryExhausted(t *testing.T) {
	dir := t.TempDir()
	var calls int64
	reg := NewRegistry()
	reg.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "flaky", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			atomic.AddInt64(&calls, 1)
			return nil, errors.New("network jitter")
		}, Retry: RetryPolicy{MaxAttempts: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond}},
	}})
	rt, cancel := newTestRuntime(t, reg, filepath.Join(dir, "s.json"), 2)
	defer cancel()
	e, _, _ := rt.Submit(context.Background(), "flow", "v1", "idem-retry-001", nil)
	final := waitTerminal(t, rt, e.ID)
	if final.Status != StatusFailed || final.FailureKind != FailureRetryExhausted {
		t.Fatalf("status=%s kind=%s", final.Status, final.FailureKind)
	}
	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if final.Steps[0].Attempts != 3 || final.Steps[0].FailureKind != FailureRetryExhausted {
		t.Fatalf("step record: %+v", final.Steps[0])
	}
}

// TestPermanentFailure 验证永久失败立即进入可区分终态，不消耗重试。
func TestPermanentFailure(t *testing.T) {
	dir := t.TempDir()
	var calls int64
	reg := NewRegistry()
	reg.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "bad", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			atomic.AddInt64(&calls, 1)
			return nil, Permanent(errors.New("invalid argument"))
		}, Retry: RetryPolicy{MaxAttempts: 5, BaseDelay: 1 * time.Millisecond}},
	}})
	rt, cancel := newTestRuntime(t, reg, filepath.Join(dir, "s.json"), 2)
	defer cancel()
	e, _, _ := rt.Submit(context.Background(), "flow", "v1", "idem-perm-001", nil)
	final := waitTerminal(t, rt, e.ID)
	if final.Status != StatusFailed || final.FailureKind != FailurePermanent {
		t.Fatalf("status=%s kind=%s reason=%s", final.Status, final.FailureKind, final.FailureReason)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("permanent failure retried %d times, want 1", got)
	}
}

// TestStepTimeout 验证超时立即进入 TimedOut 终态。
func TestStepTimeout(t *testing.T) {
	dir := t.TempDir()
	reg := NewRegistry()
	reg.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "slow", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			time.Sleep(200 * time.Millisecond)
			return nil, nil
		}, Timeout: 20 * time.Millisecond, Retry: RetryPolicy{MaxAttempts: 5, BaseDelay: 1 * time.Millisecond}},
	}})
	rt, cancel := newTestRuntime(t, reg, filepath.Join(dir, "s.json"), 2)
	defer cancel()
	e, _, _ := rt.Submit(context.Background(), "flow", "v1", "idem-timeout-001", nil)
	final := waitTerminal(t, rt, e.ID)
	if final.Status != StatusTimedOut || final.FailureKind != FailureTimeout {
		t.Fatalf("status=%s kind=%s", final.Status, final.FailureKind)
	}
}

// TestVersionIncompatibleOnResume 验证进行中的执行在定义版本缺失时被明确拒绝，
// 不静默按新语义继续。
func TestVersionIncompatibleOnResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	started := make(chan struct{})
	reg1 := NewRegistry()
	reg1.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "s1", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			time.Sleep(500 * time.Millisecond) // 让执行停在 Running
			return nil, nil
		}},
	}})
	rt1, cancel1 := newTestRuntime(t, reg1, path, 2)
	e, _, _ := rt1.Submit(context.Background(), "flow", "v1", "idem-version-001", nil)
	<-started
	cancel1()
	time.Sleep(30 * time.Millisecond)

	// 重启时只注册了新版本 v2，v1 已不存在。
	reg2 := NewRegistry()
	reg2.Register(&Definition{Name: "flow", Version: "v2", Steps: []StepDef{
		{Name: "totally-different", Func: noopStep},
	}})
	rt2, cancel2 := newTestRuntime(t, reg2, path, 2)
	defer cancel2()
	final := waitTerminal(t, rt2, e.ID)
	if final.Status != StatusFailed || final.FailureKind != FailureVersionIncompat {
		t.Fatalf("status=%s kind=%s reason=%s", final.Status, final.FailureKind, final.FailureReason)
	}
	if final.DefinitionVersion != "v1" {
		t.Fatalf("pinned version = %s, want v1", final.DefinitionVersion)
	}
}

// TestCancelReleasesSlot 验证取消进入可区分终态且不残留占用的并发额度。
func TestCancelReleasesSlot(t *testing.T) {
	dir := t.TempDir()
	block := make(chan struct{})
	reg := NewRegistry()
	reg.Register(&Definition{Name: "flow", Version: "v1", Steps: []StepDef{
		{Name: "blocking", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) {
			<-block
			return nil, nil
		}},
		{Name: "after", Func: noopStep},
	}})
	rt, cancel := newTestRuntime(t, reg, filepath.Join(dir, "s.json"), 1) // 单并发额度
	defer cancel()
	defer close(block)
	e, _, _ := rt.Submit(context.Background(), "flow", "v1", "idem-cancel-001", nil)
	waitFor(t, time.Second, func() bool {
		cur, _ := rt.Get(context.Background(), e.ID)
		return cur != nil && cur.Status == StatusRunning
	})
	if err := rt.Cancel(context.Background(), e.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	final := waitTerminal(t, rt, e.ID)
	if final.Status != StatusCanceled || final.FailureKind != FailureCanceled {
		t.Fatalf("status=%s kind=%s", final.Status, final.FailureKind)
	}
	// 取消后并发额度必须释放：另一个执行可以跑完。
	reg.Register(&Definition{Name: "fast", Version: "v1", Steps: []StepDef{
		{Name: "done", Func: func(ctx StepContext, in json.RawMessage) (json.RawMessage, error) { return []byte(`"ok"`), nil }},
	}})
	e2, _, _ := rt.Submit(context.Background(), "fast", "v1", "idem-cancel-002", nil)
	final2 := waitTerminal(t, rt, e2.ID)
	if final2.Status != StatusSucceeded {
		t.Fatalf("slot not released after cancel, second execution = %s kind=%s", final2.Status, final2.FailureKind)
	}
}

// TestFailureReasonsDistinguishable 校验各终态/拒绝原因互不相同。
func TestFailureReasonsDistinguishable(t *testing.T) {
	kinds := []FailureKind{FailurePermanent, FailureTimeout, FailureRetryExhausted, FailureCanceled, FailureVersionIncompat, FailureDefinitionUnknown}
	seen := map[FailureKind]bool{}
	for _, k := range kinds {
		if seen[k] {
			t.Fatalf("duplicate failure kind: %s", k)
		}
		seen[k] = true
	}
	errs := []error{ErrIdempotencyConflict, ErrDefinitionUnknown, ErrVersionIncompatible, ErrExecutionNotFound, ErrExecutionTerminal}
	var buf bytes.Buffer
	for _, e := range errs {
		buf.WriteString(e.Error())
		buf.WriteByte('|')
	}
	t.Logf("distinguishable rejection errors: %s", buf.String())
}
