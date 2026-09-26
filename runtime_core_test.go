package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 场景 1：正常链路，每步输入/输出/状态均持久化；幂等键与状态迁移可解释。
func TestHappyPathStepsPersisted(t *testing.T) {
	dir := t.TempDir()
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name:    "pipeline",
		Version: "1.0.0",
		Steps: []Step{
			echoStep("s1"),
			echoStep("s2"),
			echoStep("s3"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, filepath.Join(dir, "d"), reg, 4)

	key := "key-happy"
	res, err := rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{"n":1}`))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Logf("[test] idempKey=%q execution=%s new=%v versionDecision=%s",
		key, res.ExecutionID, res.NewExecution, VerAcceptCompatible)

	e := waitForStatus(t, rt, res.ExecutionID, ExecSucceeded, 3*time.Second)
	for i, s := range e.Steps {
		if s.Status != StepSucceeded {
			t.Fatalf("step %d status=%s, want succeeded", i, s.Status)
		}
		if len(s.Output) == 0 {
			t.Fatalf("step %d output not persisted", i)
		}
	}
	if !jsonEqual(t, e.Steps[0].Input, jsonIn(`{"n":1}`)) {
		t.Fatalf("step input not persisted: %s", e.Steps[0].Input)
	}

	evs := waitForEvent(t, rt, res.ExecutionID, EvSucceeded, 3*time.Second)
	var sawSuccess bool
	for _, ev := range evs {
		if ev.IdempKey != key {
			t.Fatalf("event missing idempKey: %+v", ev)
		}
		if ev.Type == EvSucceeded {
			sawSuccess = true
		}
	}
	if !sawSuccess {
		t.Fatalf("event log missing succeeded: %+v", evs)
	}
}

// 场景 2：同一幂等键的并发重复投递必须收敛到同一执行与同一结果。
func TestConcurrentDuplicateSubmission(t *testing.T) {
	dir := t.TempDir()
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name: "pipeline", Version: "1.0.0",
		Steps: []Step{echoStep("s1"), echoStep("s2")},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, filepath.Join(dir, "d"), reg, 4)

	const n = 32
	key := "key-dup"
	var wg sync.WaitGroup
	results := make([]StartResult, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{"x":1}`))
		}(i)
	}
	close(start)
	wg.Wait()

	var execID string
	newCount := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("submit %d got error: %v (duplicates must converge, not error)", i, errs[i])
		}
		if execID == "" {
			execID = results[i].ExecutionID
		} else if results[i].ExecutionID != execID {
			t.Fatalf("submit %d diverged: %s != %s", i, results[i].ExecutionID, execID)
		}
		if results[i].NewExecution {
			newCount++
		}
	}
	if newCount != 1 {
		t.Fatalf("want exactly 1 new execution, got %d", newCount)
	}
	t.Logf("[test] idempKey=%q %d concurrent submissions converged to execution=%s",
		key, n, execID)

	e := waitForStatus(t, rt, execID, ExecSucceeded, 3*time.Second)
	if !jsonEqual(t, e.Steps[1].Output, jsonIn(`{"x":1}`)) {
		t.Fatalf("converged output mismatch: %s", e.Steps[1].Output)
	}
}

// 场景 3：同幂等键不同请求内容必须被明确拒绝，原因可区分。
func TestIdempotencyConflict(t *testing.T) {
	dir := t.TempDir()
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name: "pipeline", Version: "1.0.0", Steps: []Step{echoStep("s1")},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, filepath.Join(dir, "d"), reg, 2)

	key := "key-conflict"
	r1, err := rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{"a":2}`))
	ie, ok := AsIdempotencyError(err)
	if !ok {
		t.Fatalf("want *IdempotencyError, got %v", err)
	}
	if ie.Duplicate {
		t.Fatalf("different request must not be treated as duplicate")
	}
	if ie.Code != ReasonIdempotencyConflict {
		t.Fatalf("reason=%s, want %s", ie.Code, ReasonIdempotencyConflict)
	}
	if ie.ExecutionID != r1.ExecutionID {
		t.Fatalf("conflict error must name existing execution %s, got %s", r1.ExecutionID, ie.ExecutionID)
	}
	t.Logf("[test] idempKey=%q conflicting request rejected reason=%s existingExec=%s",
		key, ie.Code, ie.ExecutionID)

	// 相同请求仍可幂等收敛。
	r3, err := rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{"a":1}`))
	if err != nil || r3.ExecutionID != r1.ExecutionID {
		t.Fatalf("identical resubmit must converge: r3=%+v err=%v", r3, err)
	}
}

// 场景 4：进程重启后从首个未完成步骤继续；已完成步骤不得重复产生副作用。
func TestRestartResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()

	var step1Calls int32
	proceed := make(chan struct{})
	runningDurably := make(chan struct{})
	block := func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		// 通知测试：本步骤已经被持久化为 running，此时“崩溃”才能命中恢复重置路径。
		select {
		case <-runningDurably:
		default:
			close(runningDurably)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-proceed:
			return in, nil
		}
	}
	def := func() Definition {
		return Definition{
			Name: "pipeline", Version: "1.0.0",
			Steps: []Step{
				{
					Name: "s1",
					Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
						atomic.AddInt32(&step1Calls, 1)
						return in, nil
					},
				},
				{Name: "s2", Fn: block},
			},
		}
	}
	if err := reg.Register(def()); err != nil {
		t.Fatal(err)
	}

	rt := mustRuntime(t, dir, reg, 2)
	key := "key-restart"
	res, err := rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{"k":"v"}`))
	if err != nil {
		t.Fatal(err)
	}
	// 等待 s2 已被持久化为 running（Fn 在 running 状态落盘后才被调用），
	// 此时关闭运行时模拟进程崩溃。
	select {
	case <-runningDurably:
	case <-time.After(3 * time.Second):
		t.Fatal("s2 never reached durably-running state before crash")
	}
	if atomic.LoadInt32(&step1Calls) != 1 {
		t.Fatalf("step1 calls=%d before crash, want 1", step1Calls)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("[test] idempKey=%q simulated crash after step1 succeeded; reopening store", key)

	// 新进程：同一目录、同一钉住版本 1.0.0。
	reg2 := NewRegistry()
	if err := reg2.Register(def()); err != nil {
		t.Fatal(err)
	}
	rt2 := mustRuntime(t, dir, reg2, 2)
	// 恢复后 s1 绝不能再执行一次。
	if atomic.LoadInt32(&step1Calls) != 1 {
		t.Fatalf("completed step1 re-executed after restart: calls=%d", step1Calls)
	}
	close(proceed) // 放行 s2 的重试运行。

	e := waitForStatus(t, rt2, res.ExecutionID, ExecSucceeded, 3*time.Second)
	if e.Steps[0].Status != StepSucceeded || e.Steps[1].Status != StepSucceeded {
		t.Fatalf("steps after restart: %+v", e.Steps)
	}
	if !jsonEqual(t, e.Steps[0].Output, jsonIn(`{"k":"v"}`)) {
		t.Fatalf("step1 output lost across restart: %s", e.Steps[0].Output)
	}
	evs := waitForEvent(t, rt2, res.ExecutionID, EvRunningReset, 3*time.Second)
	var reset bool
	for _, ev := range evs {
		if ev.Type == EvRunningReset && ev.Step == "s2" {
			reset = true
		}
	}
	if !reset {
		t.Fatalf("expected running_step_reset event for s2, got %+v", evs)
	}
	t.Logf("[test] idempKey=%q resumed after restart; step1 side-effect count stays %d", key, step1Calls)
}

var _ = fmt.Sprint
var _ = errors.New
