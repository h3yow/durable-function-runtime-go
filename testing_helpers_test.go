package durable

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func fastPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     40 * time.Millisecond,
		Multiplier:     2,
	}
}

func mustRuntime(t *testing.T, dir string, reg *Registry, max int) *Runtime {
	t.Helper()
	cfg := Config{Dir: dir, MaxConcurrent: max, Retry: fastPolicy()}
	rt, err := New(context.Background(), cfg, reg)
	if err != nil {
		t.Fatalf("New runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

// waitForStatus 轮询直到执行进入指定终态（或超时失败），并返回最终快照。
func waitForStatus(t *testing.T, rt *Runtime, id string, want ExecStatus, within time.Duration) Execution {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		e, err := rt.GetExecution(context.Background(), id)
		if err != nil {
			t.Fatalf("GetExecution %s: %v", id, err)
		}
		if e.Status == want {
			return e
		}
		if e.Status.Terminal() && e.Status != want {
			t.Fatalf("execution %s reached %s, want %s; failure=%+v", id, e.Status, want, e.Failure)
		}
		time.Sleep(5 * time.Millisecond)
	}
	e, _ := rt.GetExecution(context.Background(), id)
	t.Fatalf("execution %s did not reach %s within %s; last status=%s failure=%+v",
		id, want, within, e.Status, e.Failure)
	return e
}

func waitInflightZero(t *testing.T, rt *Runtime, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if rt.Inflight() == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("inflight slots leaked: %d", rt.Inflight())
}

// waitForEvent 轮询直到事件日志中出现指定类型（事件追加滞后于状态落盘）。
func waitForEvent(t *testing.T, rt *Runtime, id string, want EventType, within time.Duration) []Event {
	t.Helper()
	deadline := time.Now().Add(within)
	var evs []Event
	for time.Now().Before(deadline) {
		var err error
		evs, err = rt.Events(context.Background(), id)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		for _, ev := range evs {
			if ev.Type == want {
				return evs
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event %s not found within %s; events=%+v", want, within, evs)
	return evs
}

func jsonIn(s string) json.RawMessage { return json.RawMessage(s) }

// jsonEqual 比较两段 JSON 的语义（容忍空白/键序差异）。
func jsonEqual(t *testing.T, got, want json.RawMessage) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(got, &a); err != nil {
		t.Fatalf("invalid json %q: %v", string(got), err)
	}
	if err := json.Unmarshal(want, &b); err != nil {
		t.Fatalf("invalid json %q: %v", string(want), err)
	}
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

func echoStep(name string) Step {
	return Step{
		Name: name,
		Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			return in, nil
		},
	}
}
