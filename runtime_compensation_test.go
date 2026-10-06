package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fastCompPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 30 * time.Millisecond, Multiplier: 2}
}

func mustRuntimeComp(t *testing.T, dir string, reg *Registry, max int, compPol RetryPolicy) *Runtime {
	t.Helper()
	cfg := Config{Dir: dir, MaxConcurrent: max, Retry: fastPolicy(), CompRetry: compPol}
	rt, err := New(context.Background(), cfg, reg)
	if err != nil {
		t.Fatalf("New runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

// compRecorder 记录补偿调用顺序与次数（并发安全）。
type compRecorder struct {
	mu    sync.Mutex
	order []string
	count map[string]int
}

func newCompRecorder() *compRecorder { return &compRecorder{count: map[string]int{}} }

func (r *compRecorder) fn(name string) StepFunc {
	return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		r.mu.Lock()
		r.order = append(r.order, name)
		r.count[name]++
		r.mu.Unlock()
		return nil, nil
	}
}

func (r *compRecorder) snapshot() ([]string, map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	order := append([]string(nil), r.order...)
	cnt := map[string]int{}
	for k, v := range r.count {
		cnt[k] = v
	}
	return order, cnt
}

func permanentFailStep(name string) Step {
	return Step{
		Name: name,
		Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			return nil, &PermanentError{Err: errors.New("boom: " + name)}
		},
	}
}

// 正常逆序补偿：已成功且声明补偿的步骤按相反顺序补偿，原始失败原因保留。
func TestCompensationReverseOrder(t *testing.T) {
	reg := NewRegistry()
	rec := newCompRecorder()
	def := Definition{
		Name: "order", Version: "1.0.0",
		Steps: []Step{
			{Name: "charge", Fn: echoStep("charge").Fn, Compensate: rec.fn("charge")},
			{Name: "reserve", Fn: echoStep("reserve").Fn, Compensate: rec.fn("reserve")},
			permanentFailStep("ship"),
		},
	}
	if err := reg.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 4, fastCompPolicy())

	res, err := rt.Start(context.Background(), "order", "1.0.0", "idem-comp-order", jsonIn(`{"n":1}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 10*time.Second)

	order, cnt := rec.snapshot()
	if len(order) != 2 || order[0] != "reserve" || order[1] != "charge" {
		t.Fatalf("compensation order = %v, want [reserve charge]", order)
	}
	if cnt["charge"] != 1 || cnt["reserve"] != 1 {
		t.Fatalf("compensation counts = %v, want exactly once each", cnt)
	}
	if e.Compensation == nil || e.Compensation.Status != CompSucceeded {
		t.Fatalf("compensation status = %+v, want succeeded", e.Compensation)
	}
	if e.Compensation.Trigger != ReasonStepPermanent || e.Compensation.TriggerStep != "ship" {
		t.Fatalf("trigger = %q/%q, want StepPermanent/ship", e.Compensation.Trigger, e.Compensation.TriggerStep)
	}
	if e.Failure == nil || e.Failure.Code != ReasonStepPermanent {
		t.Fatalf("original failure not preserved: %+v", e.Failure)
	}
	for i, cs := range e.Compensation.Steps {
		want := CompSucceeded
		if i == 2 {
			want = CompSkipped // 失败步骤本身无需补偿
		}
		if e.Compensation.Steps[i].Status != want {
			t.Fatalf("comp step %d (%s) status = %s, want %s", i, cs.Name, cs.Status, want)
		}
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensationFinished, 5*time.Second)
	var sawStart, sawShipFail bool
	var compStarted []string
	for _, ev := range evs {
		if ev.Type == EvCompensationStarted && ev.Reason == ReasonStepPermanent {
			sawStart = true
		}
		if ev.Type == EvStepFailed && ev.Step == "ship" {
			sawShipFail = true
		}
		if ev.Type == EvCompStepStarted {
			compStarted = append(compStarted, ev.Step)
		}
	}
	if !sawStart || !sawShipFail {
		t.Fatalf("missing events: compensation_started=%v ship_failed=%v", sawStart, sawShipFail)
	}
	if len(compStarted) != 2 || compStarted[0] != "reserve" || compStarted[1] != "charge" {
		t.Fatalf("comp_started events = %v, want [reserve charge]", compStarted)
	}
	waitInflightZero(t, rt, 5*time.Second)
}

// 部分步骤未声明补偿：跳过不影响其它步骤的补偿。
func TestCompensationSkipsUndeclared(t *testing.T) {
	reg := NewRegistry()
	rec := newCompRecorder()
	def := Definition{
		Name: "partial", Version: "1.0.0",
		Steps: []Step{
			{Name: "a", Fn: echoStep("a").Fn, Compensate: rec.fn("a")},
			{Name: "b", Fn: echoStep("b").Fn}, // 未声明补偿
			{Name: "c", Fn: echoStep("c").Fn, Compensate: rec.fn("c")},
			permanentFailStep("d"),
		},
	}
	if err := reg.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 4, fastCompPolicy())

	res, err := rt.Start(context.Background(), "partial", "1.0.0", "idem-comp-partial", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecFailed, 10*time.Second)

	order, _ := rec.snapshot()
	if len(order) != 2 || order[0] != "c" || order[1] != "a" {
		t.Fatalf("compensation order = %v, want [c a] (b skipped)", order)
	}
	wantStatus := []CompStatus{CompSucceeded, CompSkipped, CompSucceeded, CompSkipped}
	for i, cs := range e.Compensation.Steps {
		if cs.Status != wantStatus[i] {
			t.Fatalf("comp step %s status = %s, want %s", cs.Name, cs.Status, wantStatus[i])
		}
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensationFinished, 5*time.Second)
	var skipped bool
	for _, ev := range evs {
		if ev.Type == EvCompStepSkipped && ev.Step == "b" {
			skipped = true
		}
	}
	if !skipped {
		t.Fatalf("expected comp_step_skipped event for step b")
	}
	waitInflightZero(t, rt, 5*time.Second)
}

// 补偿自身失败：独立退避重试到上限后进入可区分终态，原始原因不被覆盖。
func TestCompensationRetryExhausted(t *testing.T) {
	reg := NewRegistry()
	var compCalls atomic.Int32
	def := Definition{
		Name: "compfail", Version: "1.0.0",
		Steps: []Step{
			{
				Name: "charge",
				Fn:   echoStep("charge").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					compCalls.Add(1)
					return nil, errors.New("refund gateway down")
				},
			},
			permanentFailStep("ship"),
		},
	}
	if err := reg.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	compPol := RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Multiplier: 2}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 4, compPol)

	res, err := rt.Start(context.Background(), "compfail", "1.0.0", "idem-comp-retry", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	e := waitForStatus(t, rt, res.ExecutionID, ExecCompensationFailed, 10*time.Second)

	if got := compCalls.Load(); got != 3 {
		t.Fatalf("compensation attempts = %d, want 3", got)
	}
	if e.Failure == nil || e.Failure.Code != ReasonStepPermanent {
		t.Fatalf("original trigger overwritten: %+v", e.Failure)
	}
	if e.Compensation == nil || e.Compensation.Failure == nil ||
		e.Compensation.Failure.Code != ReasonCompensationFailed {
		t.Fatalf("compensation failure = %+v, want code CompensationFailed", e.Compensation)
	}
	if e.Compensation.Trigger != ReasonStepPermanent {
		t.Fatalf("compensation trigger = %q, want StepPermanent", e.Compensation.Trigger)
	}
	if e.Compensation.Steps[0].Status != CompFailed || e.Compensation.Steps[0].Attempts != 3 {
		t.Fatalf("comp step state = %+v", e.Compensation.Steps[0])
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensationFailed, 5*time.Second)
	var retries int
	for _, ev := range evs {
		if ev.Type == EvCompStepRetrying && ev.Step == "charge" {
			retries++
		}
	}
	if retries != 2 {
		t.Fatalf("comp retry events = %d, want 2", retries)
	}
	waitInflightZero(t, rt, 5*time.Second)
}

// 补偿过程中重启：已补偿成功的步骤不重复补偿，中断的补偿步骤恢复后继续。
func TestCompensationRestartRecovery(t *testing.T) {
	reg := NewRegistry()
	var compA, compB atomic.Int32
	aStarted := make(chan struct{}, 1)
	def := Definition{
		Name: "restart", Version: "1.0.0",
		Steps: []Step{
			{
				Name: "a",
				Fn:   echoStep("a").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					n := compA.Add(1)
					if n == 1 {
						// 第一次补偿：开始后被“崩溃”打断（runtime 关闭）。
						aStarted <- struct{}{}
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, nil // 恢复后的补偿直接成功
				},
			},
			{
				Name: "b",
				Fn:   echoStep("b").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					compB.Add(1)
					return nil, nil
				},
			},
			permanentFailStep("c"),
		},
	}
	if err := reg.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	dir := t.TempDir()
	cfg := Config{Dir: dir, MaxConcurrent: 4, Retry: fastPolicy(), CompRetry: fastCompPolicy()}

	rt1, err := New(context.Background(), cfg, reg)
	if err != nil {
		t.Fatalf("new rt1: %v", err)
	}
	res, err := rt1.Start(context.Background(), "restart", "1.0.0", "idem-comp-restart", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// 等补偿推进到步骤 a（b 已补偿成功，a 正在补偿中）。
	select {
	case <-aStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("compensation of step a did not start")
	}
	// 模拟进程崩溃/中断：直接关闭 runtime（a 的补偿中间态为 running）。
	if err := rt1.Close(); err != nil {
		t.Fatalf("close rt1: %v", err)
	}

	// 重启：恢复扫描应把 a 的 running 补偿重置回 pending 并继续。
	rt2, err := New(context.Background(), cfg, reg)
	if err != nil {
		t.Fatalf("new rt2: %v", err)
	}
	defer func() { _ = rt2.Close() }()

	e := waitForStatus(t, rt2, res.ExecutionID, ExecFailed, 10*time.Second)
	if e.Compensation == nil || e.Compensation.Status != CompSucceeded {
		t.Fatalf("compensation status = %+v, want succeeded", e.Compensation)
	}
	if got := compB.Load(); got != 1 {
		t.Fatalf("step b compensated %d times, want exactly 1 (no re-compensation after restart)", got)
	}
	if got := compA.Load(); got != 2 {
		t.Fatalf("step a compensation attempts = %d, want 2 (interrupted + resumed)", got)
	}
	if e.Compensation.Steps[0].Status != CompSucceeded || e.Compensation.Steps[1].Status != CompSucceeded {
		t.Fatalf("comp steps = %+v", e.Compensation.Steps)
	}
	waitInflightZero(t, rt2, 5*time.Second)
}

// 取消与超时同时发生：补偿只触发一次，结论收敛成同一份。
func TestCompensationCancelAndTimeoutConverge(t *testing.T) {
	reg := NewRegistry()
	var compCalls atomic.Int32
	release := make(chan struct{})
	def := Definition{
		Name: "race", Version: "1.0.0",
		Steps: []Step{
			{
				Name: "charge",
				Fn:   echoStep("charge").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					compCalls.Add(1)
					return nil, nil
				},
			},
			{
				Name:    "slow",
				Timeout: 60 * time.Millisecond,
				Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					<-release // 不理会取消与超时，测试结束时才放行
					return nil, nil
				},
			},
		},
	}
	if err := reg.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 4, fastCompPolicy())

	res, err := rt.Start(context.Background(), "race", "1.0.0", "idem-comp-race", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// 等 slow 步骤进入 running（超时窗口内），此时取消与超时几乎同时发生。
	deadline := time.Now().Add(5 * time.Second)
	for {
		e, _ := rt.GetExecution(context.Background(), res.ExecutionID)
		if len(e.Steps) == 2 && e.Steps[1].Status == StepRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("step slow did not start")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := rt.Cancel(context.Background(), res.ExecutionID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 取消期间再重复取消一次（重复触发必须幂等收敛）。
	_ = rt.Cancel(context.Background(), res.ExecutionID)
	// 让步骤越过自身的超时窗口后再放行：取消与超时同时成立，结论必须收敛。
	time.Sleep(120 * time.Millisecond)
	close(release)

	// 终态只能是 cancelled 或 timed_out 之一，且补偿恰好一次。
	deadline = time.Now().Add(10 * time.Second)
	var e Execution
	for {
		e, _ = rt.GetExecution(context.Background(), res.ExecutionID)
		if e.Status.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution did not terminate; status=%s", e.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if e.Status != ExecCancelled && e.Status != ExecTimedOut {
		t.Fatalf("final status = %s, want cancelled or timed_out", e.Status)
	}
	if e.Compensation == nil || e.Compensation.Status != CompSucceeded {
		t.Fatalf("compensation = %+v, want succeeded", e.Compensation)
	}
	if got := compCalls.Load(); got != 1 {
		t.Fatalf("compensation ran %d times, want exactly 1 (duplicate triggers converged)", got)
	}
	evs := waitForEvent(t, rt, res.ExecutionID, EvCompensationFinished, 5*time.Second)
	var starts int
	for _, ev := range evs {
		if ev.Type == EvCompensationStarted {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("compensation_started events = %d, want exactly 1", starts)
	}
	waitInflightZero(t, rt, 5*time.Second)
}

// 补偿期间定义版本不可用：明确拒绝，原因与补偿执行失败可区分，原始原因保留。
func TestCompensationDefinitionUnavailable(t *testing.T) {
	reg := NewRegistry()
	var v10Comp, v11Comp atomic.Int32
	defV10 := Definition{
		Name: "evolve", Version: "1.0.0",
		Steps: []Step{
			{
				Name: "charge",
				Fn:   echoStep("charge").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					v10Comp.Add(1)
					return nil, errors.New("refund failed, will retry")
				},
			},
			permanentFailStep("ship"),
		},
	}
	defV11 := Definition{
		Name: "evolve", Version: "1.1.0",
		Steps: []Step{
			{
				Name: "charge",
				Fn:   echoStep("charge").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					v11Comp.Add(1)
					return nil, nil
				},
			},
			permanentFailStep("ship"),
		},
	}
	if err := reg.Register(defV10); err != nil {
		t.Fatalf("register v1.0.0: %v", err)
	}
	if err := reg.Register(defV11); err != nil {
		t.Fatalf("register v1.1.0: %v", err)
	}
	// 补偿退避窗口拉长，便于在窗口内摘除旧版本。
	compPol := RetryPolicy{MaxAttempts: 5, InitialBackoff: 300 * time.Millisecond, MaxBackoff: 300 * time.Millisecond, Multiplier: 1}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 4, compPol)

	res, err := rt.Start(context.Background(), "evolve", "1.0.0", "idem-comp-gone", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// 等补偿第一次失败进入退避等待。
	waitForEvent(t, rt, res.ExecutionID, EvCompStepRetrying, 10*time.Second)
	// 摘除钉住的旧版本（同主版本仍有 v1.1.0 -> version_missing）。
	reg.Unregister("evolve", "1.0.0")

	e := waitForStatus(t, rt, res.ExecutionID, ExecRejected, 10*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonStepPermanent {
		t.Fatalf("original trigger overwritten: %+v", e.Failure)
	}
	if e.Compensation == nil || e.Compensation.Failure == nil ||
		e.Compensation.Failure.Code != ReasonDefVersionMissing {
		t.Fatalf("compensation rejection = %+v, want code DefVersionMissing", e.Compensation)
	}
	if e.Compensation.Failure.Code == ReasonCompensationFailed {
		t.Fatalf("rejection reason must differ from compensation execution failure")
	}
	if got := v11Comp.Load(); got != 0 {
		t.Fatalf("compensation silently fell back to v1.1.0 (%d calls)", got)
	}
	if got := v10Comp.Load(); got != 1 {
		t.Fatalf("v1.0.0 compensation attempts = %d, want 1 (no retry after rejection)", got)
	}
	waitInflightZero(t, rt, 5*time.Second)
}

// 运行期注册新版本与并发推进的执行并存：进行中的执行按钉住版本语义走，
// 新提交的执行用新版本；并发读写定义在竞态检测下必须干净。
func TestConcurrentRegisterWhileExecuting(t *testing.T) {
	reg := NewRegistry()
	var v1Comp, v2Comp atomic.Int32
	mkDef := func(version string, comp *atomic.Int32) Definition {
		return Definition{
			Name: "conc", Version: version,
			Steps: []Step{
				{
					Name: "charge",
					Fn:   echoStep("charge").Fn,
					Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
						comp.Add(1)
						return nil, nil
					},
				},
				permanentFailStep("ship"),
			},
		}
	}
	if err := reg.Register(mkDef("1.0.0", &v1Comp)); err != nil {
		t.Fatalf("register v1.0.0: %v", err)
	}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 8, fastCompPolicy())

	const n = 12
	ids := make([]string, n)
	var wg sync.WaitGroup
	// 并发提交一批钉住 v1.0.0 的执行。
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := rt.Start(context.Background(), "conc", "1.0.0", "idem-conc-v1-"+itoa(i), jsonIn(`{}`))
			if err != nil {
				t.Errorf("start v1: %v", err)
				return
			}
			ids[i] = res.ExecutionID
		}(i)
	}
	// 并发注册新版本（含新增/调整补偿动作）与并发读取定义。
	stop := make(chan struct{})
	regDone := make(chan struct{})
	go func() {
		defer close(regDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = reg.Register(mkDef("1.1.0", &v2Comp))
			_, _ = reg.CheckVersion("conc", "1.0.0")
			_, _ = reg.CheckVersion("conc", "1.1.0")
		}
	}()
	wg.Wait()
	close(stop)
	<-regDone
	// 注册落定后，新提交的执行使用新版本。
	if err := reg.Register(mkDef("1.1.0", &v2Comp)); err != nil {
		t.Fatalf("register v1.1.0: %v", err)
	}
	res2, err := rt.Start(context.Background(), "conc", "1.1.0", "idem-conc-v2", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start v2: %v", err)
	}

	for i, id := range ids {
		if id == "" {
			continue
		}
		e := waitForStatus(t, rt, id, ExecFailed, 15*time.Second)
		if e.DefVersion != "1.0.0" {
			t.Fatalf("exec %d pinned version = %s, want 1.0.0", i, e.DefVersion)
		}
		if e.Compensation == nil || e.Compensation.Status != CompSucceeded {
			t.Fatalf("exec %d compensation = %+v", i, e.Compensation)
		}
	}
	waitForStatus(t, rt, res2.ExecutionID, ExecFailed, 15*time.Second)

	if got := v1Comp.Load(); got != int32(n) {
		t.Fatalf("v1.0.0 compensation calls = %d, want %d (pinned semantics)", got, n)
	}
	if got := v2Comp.Load(); got != 1 {
		t.Fatalf("v1.1.0 compensation calls = %d, want 1", got)
	}
	waitInflightZero(t, rt, 5*time.Second)
}

// 补偿动作不理会取消信号时：步骤级超时后尽快给出结论并释放额度。
func TestCompensationTimeoutReleasesSlot(t *testing.T) {
	reg := NewRegistry()
	var compCalls atomic.Int32
	never := make(chan struct{})
	t.Cleanup(func() { close(never) })
	def := Definition{
		Name: "stubborn", Version: "1.0.0",
		Steps: []Step{
			{
				Name: "charge",
				Fn:   echoStep("charge").Fn,
				Compensate: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					compCalls.Add(1)
					<-never // 完全不理会取消信号
					return nil, nil
				},
				CompensateTimeout: 50 * time.Millisecond,
			},
			permanentFailStep("ship"),
		},
	}
	if err := reg.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	compPol := RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2}
	rt := mustRuntimeComp(t, t.TempDir(), reg, 2, compPol)

	res, err := rt.Start(context.Background(), "stubborn", "1.0.0", "idem-comp-stubborn", jsonIn(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	start := time.Now()
	e := waitForStatus(t, rt, res.ExecutionID, ExecCompensationFailed, 10*time.Second)
	// 补偿函数永不返回，但结论必须在秒级内给出（两次尝试各 50ms 超时 + 退避）。
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("compensation conclusion took %s, want prompt timeout conclusion", elapsed)
	}
	if e.Compensation == nil || e.Compensation.Failure == nil ||
		e.Compensation.Failure.Code != ReasonCompensationTimeout {
		t.Fatalf("compensation failure = %+v, want code CompensationTimeout", e.Compensation)
	}
	if got := compCalls.Load(); got != 2 {
		t.Fatalf("compensation attempts = %d, want 2", got)
	}
	// 超时后额度必须释放：补偿函数仍阻塞，但 inflight 必须归零。
	waitInflightZero(t, rt, 5*time.Second)
}
