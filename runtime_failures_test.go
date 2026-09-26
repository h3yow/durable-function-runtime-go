package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// 场景 5：瞬时失败必须按有上限退避重试；达到上限进入可区分终态，
// 不残留重试调度，且并发额度被释放。
func TestRetryLimitExceeded(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var calls int32
	if err := reg.Register(Definition{
		Name: "flaky", Version: "1.0.0",
		Steps: []Step{{
			Name: "always-transient",
			Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				n := atomic.AddInt32(&calls, 1)
				return nil, fmt.Errorf("transient boom #%d", n)
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-retry-limit"
	res, err := rt.Start(context.Background(), "flaky", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 5*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonRetryLimitExceeded {
		t.Fatalf("failure=%+v, want code %s", e.Failure, ReasonRetryLimitExceeded)
	}
	if got := atomic.LoadInt32(&calls); int(got) != fastPolicy().MaxAttempts {
		t.Fatalf("attempts=%d, want exactly %d (no retries beyond cap)", got, fastPolicy().MaxAttempts)
	}
	if e.Steps[0].Attempts != fastPolicy().MaxAttempts {
		t.Fatalf("persisted attempts=%d", e.Steps[0].Attempts)
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvStepFailed, 3*time.Second)
	var retryEv *Event
	for i := range evs {
		if evs[i].Type == EvStepRetrying {
			retryEv = &evs[i]
		}
	}
	if retryEv == nil {
		t.Fatalf("expected step_retrying event with backoff basis; evs=%+v", evs)
	}
	if retryEv.MaxAttempts != fastPolicy().MaxAttempts || retryEv.BackoffMS <= 0 {
		t.Fatalf("retry basis not logged: attempt=%d max=%d backoffMS=%d",
			retryEv.Attempt, retryEv.MaxAttempts, retryEv.BackoffMS)
	}
	t.Logf("[test] idempKey=%q retried attempt=%d/%d backoffMS=%d retryReason=%s finalReason=%s",
		key, retryEv.Attempt, retryEv.MaxAttempts, retryEv.BackoffMS, ReasonStepTransient, ReasonRetryLimitExceeded)

	// 终态后不得有任何悬挂调度或被占用额度。
	waitInflightZero(t, rt, time.Second)
	time.Sleep(100 * time.Millisecond) // 观察窗口：确认不会再有额外尝试
	if got := atomic.LoadInt32(&calls); int(got) != fastPolicy().MaxAttempts {
		t.Fatalf("terminal execution scheduled again: attempts=%d", got)
	}
}

// 场景 6：永久失败立即终态，绝不重试，原因与瞬时失败可区分。
func TestPermanentFailureNoRetry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var calls int32
	if err := reg.Register(Definition{
		Name: "hard", Version: "2.0.0",
		Steps: []Step{{
			Name: "perm",
			Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&calls, 1)
				return nil, &PermanentError{Err: errors.New("bad request shape")}
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-permanent"
	res, err := rt.Start(context.Background(), "hard", "2.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 3*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonStepPermanent {
		t.Fatalf("failure=%+v, want %s", e.Failure, ReasonStepPermanent)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("permanent failure was retried: calls=%d", got)
	}
	if e.Steps[0].Status != StepFailed {
		t.Fatalf("step status=%s", e.Steps[0].Status)
	}
	t.Logf("[test] idempKey=%q permanent failure after attempt=1 reason=%s (no retry)", key, ReasonStepPermanent)
	waitInflightZero(t, rt, time.Second)
}

// 场景 7：步骤超时立即进入独立终态 timed_out，不重试。
func TestStepTimeout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var calls int32
	if err := reg.Register(Definition{
		Name: "slow", Version: "1.1.0",
		Steps: []Step{{
			Name:    "hangs",
			Timeout: 60 * time.Millisecond,
			Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&calls, 1)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-timeout"
	res, err := rt.Start(context.Background(), "slow", "1.1.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecTimedOut, 3*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonStepTimeout {
		t.Fatalf("failure=%+v, want %s", e.Failure, ReasonStepTimeout)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("timed out step was retried: calls=%d", got)
	}
	if e.Steps[0].Status != StepTimedOut {
		t.Fatalf("step status=%s want timed_out", e.Steps[0].Status)
	}
	t.Logf("[test] idempKey=%q timed_out reason=%s attempt=1 (timeout is non-retryable)", key, ReasonStepTimeout)
	waitInflightZero(t, rt, time.Second)
}

// 场景 8：执行在退避等待期间被取消，必须立即进入 cancelled，
// 不得继续等待、不产生后续尝试、不占用额度。
func TestCancelDuringBackoff(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var calls int32
	if err := reg.Register(Definition{
		Name: "flaky2", Version: "1.0.0",
		Steps: []Step{{
			Name: "fails-then-cancelled",
			Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&calls, 1)
				return nil, errors.New("temporary network error")
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-cancel-backoff"
	res, err := rt.Start(context.Background(), "flaky2", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	// 等待第一次失败并进入退避（retrying 已持久化 NotBefore）。
	waitForEvent(t, rt, res.ExecutionID, EvStepRetrying, 3*time.Second)
	if err := rt.Cancel(context.Background(), res.ExecutionID); err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecCancelled, 3*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonExecutionCancelled {
		t.Fatalf("failure=%+v, want %s", e.Failure, ReasonExecutionCancelled)
	}
	attempts := atomic.LoadInt32(&calls)
	time.Sleep(150 * time.Millisecond) // 远超退避窗口
	if atomic.LoadInt32(&calls) != attempts {
		t.Fatalf("cancelled execution still retried: before=%d after=%d", attempts, calls)
	}
	t.Logf("[test] idempKey=%q cancelled during backoff after attempt=%d reason=%s", key, attempts, ReasonExecutionCancelled)
	waitInflightZero(t, rt, time.Second)

	// 对终态执行再次取消必须被明确拒绝。
	if err := rt.Cancel(context.Background(), res.ExecutionID); err == nil {
		t.Fatal("cancelling terminal execution must be rejected")
	} else {
		if rj, ok := AsRejectError(err); !ok || rj.Code != "TerminalNoCancel" {
			t.Fatalf("want TerminalNoCancel reject, got %v", err)
		}
	}
}

// 场景 15：步骤正在执行时收到取消 -> 立即 cancelled，不把 ctx 错误误判为瞬时失败，
// 并发额度必须释放，后续不再调度。
func TestCancelWhileStepRunning(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	started := make(chan struct{})
	var calls int32
	if err := reg.Register(Definition{
		Name: "blocker", Version: "1.0.0",
		Steps: []Step{{
			Name: "long",
			Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&calls, 1)
				close(started)
				<-ctx.Done()
				return nil, ctx.Err() // 步骤本身会返回“错误”，但取消意图优先
			},
		}, echoStep("never-runs")},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 1)
	key := "key-cancel-running"
	res, err := rt.Start(context.Background(), "blocker", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := rt.Cancel(context.Background(), res.ExecutionID); err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecCancelled, 3*time.Second)
	if e.Steps[0].Status != StepCancelled || e.Steps[1].Status != StepCancelled {
		t.Fatalf("steps=%+v", e.Steps)
	}
	t.Logf("[test] idempKey=%q cancelled while running; later steps never executed", key)
	waitInflightZero(t, rt, time.Second)
	time.Sleep(100 * time.Millisecond)
	if calls != 1 {
		t.Fatalf("cancelled step retried: calls=%d", calls)
	}
}
