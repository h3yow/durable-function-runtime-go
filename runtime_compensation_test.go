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

func fastCompPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 40 * time.Millisecond, Multiplier: 2}
}

func mustRuntimeComp(t *testing.T, dir string, reg *Registry, max int) *Runtime {
	t.Helper()
	cfg := Config{Dir: dir, MaxConcurrent: max, Retry: fastPolicy(), CompRetry: fastCompPolicy()}
	rt, err := New(context.Background(), cfg, reg)
	if err != nil {
		t.Fatalf("New runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

func compRec(order *sync.Mutex, log *[]string) func(name string) StepFunc {
	return func(name string) StepFunc {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			order.Lock()
			*log = append(*log, name)
			order.Unlock()
			return json.RawMessage(`{"compensated":"` + name + `"}`), nil
		}
	}
}

// 场景 C1：正向永久失败后，已成功且声明补偿的步骤按相反顺序补偿；
// 触发原因（哪一步失败）完整保留，补偿每步结果落盘。
func TestCompensationReverseOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var mu sync.Mutex
	var compLog []string
	mk := compRec(&mu, &compLog)
	if err := reg.Register(Definition{
		Name: "order", Version: "1.0.0",
		Steps: []Step{
			{Name: "charge", Fn: echoStep("charge").Fn, Compensate: mk("refund")},
			{Name: "reserve", Fn: echoStep("reserve").Fn, Compensate: mk("release")},
			{Name: "ship", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return nil, &PermanentError{Err: errors.New("carrier rejected")}
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 2)
	key := "key-comp-reverse"
	res, err := rt.Start(context.Background(), "order", "1.0.0", key, jsonIn(`{"order":1}`))
	if err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 5*time.Second)

	// 逆序：reserve 的补偿先于 charge。
	mu.Lock()
	got := append([]string(nil), compLog...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "release" || got[1] != "refund" {
		t.Fatalf("compensation order=%v, want [release refund]", got)
	}
	// 原始触发原因保留，不被补偿过程覆盖。
	if e.Failure == nil || e.Failure.Code != ReasonStepPermanent {
		t.Fatalf("failure=%+v, want original trigger %s", e.Failure, ReasonStepPermanent)
	}
	if e.Comp == nil || e.Comp.Trigger.Code != ReasonStepPermanent || e.Comp.TriggerStep != "ship" {
		t.Fatalf("comp trigger not preserved: %+v", e.Comp)
	}
	if e.Comp.OriginStatus != ExecFailed {
		t.Fatalf("origin=%s, want failed", e.Comp.OriginStatus)
	}
	for _, cs := range e.Comp.Steps {
		if cs.Status != CompSucceeded || cs.Attempts != 1 {
			t.Fatalf("comp step %+v, want succeeded with exactly 1 attempt", cs)
		}
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensated, 3*time.Second)
	var started, ok int
	for _, ev := range evs {
		switch ev.Type {
		case EvCompStarted:
			started++
			if ev.Reason != ReasonStepPermanent || ev.Step != "ship" {
				t.Fatalf("compensation_started basis wrong: %+v", ev)
			}
		case EvCompStepOK:
			ok++
		}
	}
	if started != 1 || ok != 2 {
		t.Fatalf("comp events: started=%d ok=%d, want 1/2", started, ok)
	}
	t.Logf("[test] idempKey=%q compensated=%v trigger=%s triggerStep=ship", key, got, ReasonStepPermanent)
	waitInflightZero(t, rt, time.Second)
}

// 场景 C2：部分步骤未声明补偿 -> 直接跳过（记录 skipped），不影响其它步骤的补偿。
func TestCompensationSkipsUndeclared(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var mu sync.Mutex
	var compLog []string
	mk := compRec(&mu, &compLog)
	if err := reg.Register(Definition{
		Name: "mixed", Version: "1.0.0",
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn, Compensate: mk("comp-a")},
			{Name: "b", Fn: echoStep("b").Fn}, // 未声明补偿
			{Name: "c", Fn: echoStep("c").Fn, Compensate: mk("comp-c")},
			{Name: "d", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return nil, &PermanentError{Err: errors.New("boom")}
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 2)
	key := "key-comp-skip"
	res, err := rt.Start(context.Background(), "mixed", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 5*time.Second)

	mu.Lock()
	got := append([]string(nil), compLog...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "comp-c" || got[1] != "comp-a" {
		t.Fatalf("compensation order=%v, want [comp-c comp-a] (b skipped)", got)
	}
	byStep := map[string]CompState{}
	for _, cs := range e.Comp.Steps {
		byStep[cs.Step] = cs
	}
	if byStep["b"].Status != CompSkipped {
		t.Fatalf("step b comp status=%s, want skipped", byStep["b"].Status)
	}
	if byStep["a"].Status != CompSucceeded || byStep["c"].Status != CompSucceeded {
		t.Fatalf("declared comps must succeed: %+v", byStep)
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensated, 3*time.Second)
	var skipEv *Event
	for i := range evs {
		if evs[i].Type == EvCompStepSkipped && evs[i].Step == "b" {
			skipEv = &evs[i]
		}
	}
	if skipEv == nil {
		t.Fatalf("expected compensation_step_skipped event for step b; evs=%+v", evs)
	}
	t.Logf("[test] idempKey=%q compensated=%v skipped=b reason=%s", key, got, skipEv.Reason)
	waitInflightZero(t, rt, time.Second)
}

// 场景 C3：补偿动作自身瞬时失败，按独立的有上限退避重试；耗尽后进入
// 与正向失败可区分的 compensation_failed 终态，原始触发原因保留。
func TestCompensationRetryLimit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var compCalls int32
	if err := reg.Register(Definition{
		Name: "unlucky", Version: "1.0.0",
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				n := atomic.AddInt32(&compCalls, 1)
				return nil, fmt.Errorf("comp transient #%d", n)
			}},
			{Name: "b", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return nil, &PermanentError{Err: errors.New("forward boom")}
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 2)
	key := "key-comp-retry-limit"
	res, err := rt.Start(context.Background(), "unlucky", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecCompensationFailed, 5*time.Second)

	if e.Failure == nil || e.Failure.Code != ReasonCompRetryExhausted {
		t.Fatalf("failure=%+v, want %s", e.Failure, ReasonCompRetryExhausted)
	}
	// 与正向流程失败可区分：终态不是 failed，原因码不是 StepPermanent/RetryLimitExceeded。
	if e.Status == ExecFailed || e.Failure.Code == ReasonStepPermanent || e.Failure.Code == ReasonRetryLimitExceeded {
		t.Fatalf("compensation failure must be distinguishable: status=%s code=%s", e.Status, e.Failure.Code)
	}
	// 原始触发原因保留在 Comp.Trigger。
	if e.Comp == nil || e.Comp.Trigger.Code != ReasonStepPermanent || e.Comp.TriggerStep != "b" {
		t.Fatalf("original trigger overwritten: %+v", e.Comp)
	}
	want := fastCompPolicy().MaxAttempts
	if got := atomic.LoadInt32(&compCalls); int(got) != want {
		t.Fatalf("comp attempts=%d, want exactly %d", got, want)
	}
	if e.Comp.Steps[0].Attempts != want || e.Comp.Steps[0].Status != CompFailed {
		t.Fatalf("comp state=%+v", e.Comp.Steps[0])
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompFailed, 3*time.Second)
	var retryEv *Event
	for i := range evs {
		if evs[i].Type == EvCompStepRetry {
			retryEv = &evs[i]
		}
	}
	if retryEv == nil || retryEv.MaxAttempts != want || retryEv.BackoffMS <= 0 {
		t.Fatalf("comp retry basis not logged: %+v", retryEv)
	}
	t.Logf("[test] idempKey=%q compRetry attempt=%d/%d backoffMS=%d finalReason=%s trigger=%s",
		key, retryEv.Attempt, retryEv.MaxAttempts, retryEv.BackoffMS, ReasonCompRetryExhausted, e.Comp.Trigger.Code)

	// 终态后无悬挂调度、无被占用额度。
	waitInflightZero(t, rt, time.Second)
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&compCalls); int(got) != want {
		t.Fatalf("compensation scheduled again after terminal: attempts=%d", got)
	}
}

// 场景 C4：补偿做到一半进程重启，恢复后从未补偿完的地方继续；
// 已补偿成功的步骤绝不重复补偿，崩溃留下的 running 中间态不被当作已完成。
func TestCompensationRestartResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()

	var compA, compB int32
	block := make(chan struct{})
	runningDurably := make(chan struct{})
	def := func() Definition {
		return Definition{
			Name: "pipeline", Version: "1.0.0",
			Steps: []Step{
				{Name: "a", Fn: echoStep("a").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&compA, 1)
					// 补偿 a 时阻塞，制造“补偿进行中崩溃”的窗口。
					select {
					case <-runningDurably:
					default:
						close(runningDurably)
					}
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-block:
						return json.RawMessage(`{"ok":"a"}`), nil
					}
				}},
				{Name: "b", Fn: echoStep("b").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&compB, 1)
					return json.RawMessage(`{"ok":"b"}`), nil
				}},
				{Name: "c", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					return nil, &PermanentError{Err: errors.New("boom")}
				}},
			},
		}
	}
	if err := reg.Register(def()); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 2)
	key := "key-comp-restart"
	res, err := rt.Start(context.Background(), "pipeline", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	// 等补偿队列推进到 a（b 已补偿成功并落盘），此时“崩溃”。
	select {
	case <-runningDurably:
	case <-time.After(3 * time.Second):
		t.Fatal("compensation of step a never reached running before crash")
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&compB); got != 1 {
		t.Fatalf("comp b calls=%d before crash, want 1", got)
	}
	t.Logf("[test] idempKey=%q crash during compensation of step a; compB=%d", key, atomic.LoadInt32(&compB))

	// 新进程：同一目录、同一钉住版本。
	reg2 := NewRegistry()
	if err := reg2.Register(def()); err != nil {
		t.Fatal(err)
	}
	close(block) // 恢复后允许补偿 a 完成
	rt2 := mustRuntimeComp(t, dir, reg2, 2)
	e := waitForStatus(t, rt2, res.ExecutionID, ExecFailed, 5*time.Second)

	// 已补偿成功的 b 不得重复补偿；a 的 running 中间态未被当作已完成（恢复后重新执行）。
	if got := atomic.LoadInt32(&compB); got != 1 {
		t.Fatalf("comp b re-executed after restart: calls=%d", got)
	}
	if got := atomic.LoadInt32(&compA); got < 1 {
		t.Fatalf("comp a never completed after restart: calls=%d", got)
	}
	byStep := map[string]CompState{}
	for _, cs := range e.Comp.Steps {
		byStep[cs.Step] = cs
	}
	if byStep["a"].Status != CompSucceeded || byStep["b"].Status != CompSucceeded {
		t.Fatalf("comp states after resume: %+v", byStep)
	}
	if byStep["b"].Attempts != 1 {
		t.Fatalf("comp b attempts=%d, want exactly 1 (no duplicate compensation)", byStep["b"].Attempts)
	}
	if e.Comp.Trigger.Code != ReasonStepPermanent {
		t.Fatalf("trigger lost across restart: %+v", e.Comp.Trigger)
	}
	t.Logf("[test] idempKey=%q resumed compensation: a=%s(attempts=%d) b=%s(attempts=%d) final=%s",
		key, byStep["a"].Status, byStep["a"].Attempts, byStep["b"].Status, byStep["b"].Attempts, e.Status)
	waitInflightZero(t, rt2, time.Second)
}

// 场景 C5：步骤超时与外部取消同时发生，补偿只触发一次、结论收敛成同一份；
// 补偿期间的取消不会中断补偿，也不会产生第二份补偿。
func TestCompensationCancelTimeoutRace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var compCalls int32
	compBlock := make(chan struct{})
	compRunning := make(chan struct{})
	if err := reg.Register(Definition{
		Name: "racey", Version: "1.0.0",
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&compCalls, 1)
				close(compRunning)
				select {
				case <-compBlock:
					return json.RawMessage(`{"ok":"a"}`), nil
				case <-time.After(3 * time.Second):
					return nil, errors.New("test did not release compensation")
				}
			}},
			{Name: "b", Timeout: 40 * time.Millisecond, Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 2)
	key := "key-comp-race"
	res, err := rt.Start(context.Background(), "racey", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	// 步骤 b 超时触发补偿；补偿 a 进行中时并发取消（可能重复多次）。
	select {
	case <-compRunning:
	case <-time.After(3 * time.Second):
		t.Fatal("compensation did not start after step timeout")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rt.Cancel(context.Background(), res.ExecutionID)
		}()
	}
	wg.Wait()
	close(compBlock)

	// 终态收敛：origin 是 timed_out（超时先落盘）；取消与超时的竞争只产生一份补偿。
	e := waitForStatus(t, rt, res.ExecutionID, ExecTimedOut, 5*time.Second)
	if got := atomic.LoadInt32(&compCalls); got != 1 {
		t.Fatalf("compensation executed %d times, want exactly 1", got)
	}
	if e.Comp == nil || e.Comp.Trigger.Code != ReasonStepTimeout {
		t.Fatalf("trigger=%+v, want %s", e.Comp.Trigger, ReasonStepTimeout)
	}
	if e.Comp.OriginStatus != e.Status {
		t.Fatalf("final status %s != comp origin %s (divergent conclusions)", e.Status, e.Comp.OriginStatus)
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensated, 3*time.Second)
	var started int
	for _, ev := range evs {
		if ev.Type == EvCompStarted {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("compensation_started events=%d, want exactly 1 (converged)", started)
	}
	t.Logf("[test] idempKey=%q cancel+timeout raced; compensations=%d trigger=%s final=%s",
		key, started, e.Comp.Trigger.Code, e.Status)
	waitInflightZero(t, rt, time.Second)
}

// 场景 C6：补偿动作不理会取消信号时，步骤级超时到期后仍尽快给出结论
// 并释放并发额度，不把额度一直拖着。
func TestCompensationTimeoutReleasesSlot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	stubborn := make(chan struct{})
	defer close(stubborn)
	entered := make(chan struct{})
	if err := reg.Register(Definition{
		Name: "stubborn-comp", Version: "1.0.0",
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn,
				CompTimeout: 50 * time.Millisecond,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					close(entered)
					<-stubborn // 完全不理会 ctx 取消
					return json.RawMessage(`{"ok":"a"}`), nil
				}},
			{Name: "b", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return nil, &PermanentError{Err: errors.New("boom")}
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 1) // 额度只有 1：若补偿不释放额度会立刻暴露
	key := "key-comp-timeout"
	res, err := rt.Start(context.Background(), "stubborn-comp", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	e := waitForStatus(t, rt, res.ExecutionID, ExecCompensationFailed, 5*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonCompTimeout {
		t.Fatalf("failure=%+v, want %s", e.Failure, ReasonCompTimeout)
	}
	if e.Comp.Steps[0].Status != CompTimedOut {
		t.Fatalf("comp step status=%s, want timed_out", e.Comp.Steps[0].Status)
	}
	// 补偿函数仍在阻塞（不理会取消），但额度必须已经释放。
	waitInflightZero(t, rt, 2*time.Second)
	if e.Comp.Trigger.Code != ReasonStepPermanent {
		t.Fatalf("trigger overwritten: %+v", e.Comp.Trigger)
	}
	t.Logf("[test] idempKey=%q comp timeout after 50ms -> %s; slot released while fn still blocked",
		key, ReasonCompTimeout)
}

// 场景 C7：补偿期间钉住的定义版本不可用 -> 明确拒绝，原因码与补偿执行失败
// 可区分，绝不静默切换到新版本去补偿。
func TestCompensationDefinitionVersionUnavailable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var v1CompCalls, v11CompCalls int32
	// 补偿第一次瞬时失败 -> 进入退避窗口；退避期间摘除 1.0.0 并注册 1.1.0。
	if err := reg.Register(Definition{
		Name: "svc", Version: "1.0.0",
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&v1CompCalls, 1)
				return nil, errors.New("comp transient")
			}},
			{Name: "b", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return nil, &PermanentError{Err: errors.New("boom")}
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 2)
	key := "key-comp-def-gone"
	res, err := rt.Start(context.Background(), "svc", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	// 等补偿第一次失败并持久化退避窗口。
	waitForEvent(t, rt, res.ExecutionID, EvCompStepRetry, 3*time.Second)
	reg.Unregister("svc", "1.0.0")
	if err := reg.Register(Definition{
		Name: "svc", Version: "1.1.0", // 同主版本但精确版本缺失
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				atomic.AddInt32(&v11CompCalls, 1)
				return json.RawMessage(`{"ok":"v11"}`), nil
			}},
			{Name: "b", Fn: echoStep("b").Fn},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt.kick()

	e := waitForStatus(t, rt, res.ExecutionID, ExecCompensationFailed, 5*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonCompDefUnavailable {
		t.Fatalf("failure=%+v, want %s", e.Failure, ReasonCompDefUnavailable)
	}
	// 拒绝原因必须与补偿执行失败本身可区分。
	if e.Failure.Code == ReasonCompRetryExhausted || e.Failure.Code == ReasonCompPermanent || e.Failure.Code == ReasonCompTimeout {
		t.Fatalf("version-unavailable must be distinguishable from compensation failure: %s", e.Failure.Code)
	}
	// 绝不能悄悄换成 1.1.0 的补偿动作。
	if got := atomic.LoadInt32(&v11CompCalls); got != 0 {
		t.Fatalf("compensated with NEW definition version: v1.1 comp calls=%d", got)
	}
	if got := atomic.LoadInt32(&v1CompCalls); got != 1 {
		t.Fatalf("v1.0 comp calls=%d, want exactly 1 (no extra charge after rejection)", got)
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompRejected, 3*time.Second)
	var rej *Event
	for i := range evs {
		if evs[i].Type == EvCompRejected {
			rej = &evs[i]
		}
	}
	if rej == nil || rej.VersionDecision != string(VerVersionMissing) {
		t.Fatalf("expected compensation_rejected with versionDecision=version_missing: %+v", rej)
	}
	t.Logf("[test] idempKey=%q versionDecision=%s reason=%s (refused to fall back to v1.1.0)",
		key, rej.VersionDecision, rej.Reason)
	waitInflightZero(t, rt, time.Second)
}

// 场景 C8：运行期注册新版本与正在补偿的执行并发推进：
// 已启动的执行继续按钉住的 1.0.0 语义补偿，新提交的执行才用 2.0.0。
func TestRegisterNewVersionWhileCompensating(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	var v1Comp, v2Comp int32
	release := make(chan struct{})
	entered := make(chan struct{})
	mkDef := func(version string, counter *int32, block bool) Definition {
		return Definition{
			Name: "evo", Version: version,
			Steps: []Step{
				{Name: "a", Fn: echoStep("a").Fn, Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(counter, 1)
					if block {
						close(entered)
						<-release
					}
					return json.RawMessage(`{"ok":"` + version + `"}`), nil
				}},
				{Name: "b", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					return nil, &PermanentError{Err: errors.New("boom")}
				}},
			},
		}
	}
	if err := reg.Register(mkDef("1.0.0", &v1Comp, true)); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntimeComp(t, dir, reg, 4)
	keyOld := "key-comp-pinned-v1"
	res, err := rt.Start(context.Background(), "evo", "1.0.0", keyOld, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("v1 compensation did not start")
	}

	// 补偿进行中并发注册多个新版本（与驱动的版本判定并发，竞态检测覆盖）。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = reg.Register(mkDef(fmt.Sprintf("1.%d.0", i+1), &v1Comp, false))
		}(i)
	}
	if err := reg.Register(mkDef("2.0.0", &v2Comp, false)); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// 新提交的执行用新版本；旧执行仍钉住 1.0.0。
	keyNew := "key-comp-new-v2"
	res2, err := rt.Start(context.Background(), "evo", "2.0.0", keyNew, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	close(release)

	e1 := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 5*time.Second)
	e2 := waitForStatus(t, rt, res2.ExecutionID, ExecFailed, 5*time.Second)

	if e1.DefVersion != "1.0.0" || e2.DefVersion != "2.0.0" {
		t.Fatalf("pinned versions: old=%s new=%s", e1.DefVersion, e2.DefVersion)
	}
	if got := atomic.LoadInt32(&v1Comp); got != 1 {
		t.Fatalf("v1 comp calls=%d, want exactly 1 (old exec kept pinned semantics)", got)
	}
	if got := atomic.LoadInt32(&v2Comp); got != 1 {
		t.Fatalf("v2 comp calls=%d, want exactly 1 (new exec uses new version)", got)
	}
	for _, e := range []Execution{e1, e2} {
		if e.Comp == nil || e.Comp.Steps[0].Status != CompSucceeded {
			t.Fatalf("exec %s comp not completed: %+v", e.ID, e.Comp)
		}
	}
	t.Logf("[test] idempKey=%q pinned=1.0.0 v1Comp=%d; idempKey=%q pinned=2.0.0 v2Comp=%d",
		keyOld, v1Comp, keyNew, v2Comp)
	waitInflightZero(t, rt, time.Second)
}
