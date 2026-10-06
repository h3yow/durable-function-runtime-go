package durable

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// execDriver 为一次执行的单线程 Actor：所有状态推进在它自己的 goroutine 中完成，
// 从而避免同一执行被并发重复调度；等待退避期间不占用并发额度。
type execDriver struct {
	rt   *Runtime
	id   string
	wake chan struct{}
	stop chan struct{}

	// curCancel 为“当前正在执行的那一次推进”的取消通道：
	// 每次 driveOnce 运行步骤前换新，避免关闭通道后污染后续步骤。
	cancelMu  sync.Mutex
	curCancel chan struct{}
}

func (rt *Runtime) newExecDriver(id string) *execDriver {
	d := &execDriver{
		rt:   rt,
		id:   id,
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
	}
	rt.wg.Add(1)
	go d.loop()
	return d
}

func (d *execDriver) notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *execDriver) interrupt() {
	// 取消当前正在运行的步骤（若有），并唤醒可能在退避等待中的 loop 去重新读盘。
	d.cancelMu.Lock()
	if d.curCancel != nil {
		close(d.curCancel)
		d.curCancel = nil
	}
	d.cancelMu.Unlock()
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// armCancel 安装当次推进的取消通道并返回。
func (d *execDriver) armCancel() chan struct{} {
	ch := make(chan struct{})
	d.cancelMu.Lock()
	d.curCancel = ch
	d.cancelMu.Unlock()
	return ch
}

func (d *execDriver) loop() {
	defer d.rt.wg.Done()
	for {
		select {
		case <-d.rt.ctx.Done():
			return
		case <-d.stop:
			return
		case <-d.wake:
		}

		// 持续推进直到“需要退避等待”或“终态”，避免步骤之间平白等一个 tick。
		for {
			wait, terminal := d.driveOnce()
			if terminal {
				close(d.stop)
				return
			}
			if wait <= 0 {
				// 无可立即推进的工作（如存储暂时读取失败/退避时刻边界），
				// 短暂让出以避免空转，随后继续尝试。
				select {
				case <-d.rt.ctx.Done():
					return
				case <-d.stop:
					return
				case <-d.wake:
				case <-time.After(20 * time.Millisecond):
				}
				break
			}
			t := time.NewTimer(wait)
			select {
			case <-d.rt.ctx.Done():
				t.Stop()
				return
			case <-d.stop:
				t.Stop()
				return
			case <-d.wake:
				// 取消等事件可能提前打断退避等待；唤醒后立即重读磁盘。
			case <-t.C:
			}
			t.Stop()
			break // 回到外层 select，等待信号后重新进入推进循环
		}
	}
}

// driveOnce 推进一次执行。
// 返回 wait>0 表示下一个步骤处于退避等待；terminal 表示执行已终态或驱动应停止。
func (d *execDriver) driveOnce() (wait time.Duration, terminal bool) {
	ctx := d.rt.ctx
	e, rev, err := d.rt.store.GetExecution(ctx, d.id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, true
		}
		return 0, false
	}
	if e.Status.Terminal() {
		return 0, true
	}

	// 补偿阶段：正向流程已收尾，按相反顺序执行已声明的补偿动作。
	if e.Status == ExecCompensating {
		return d.driveCompensation(e, rev)
	}

	// 定义版本在每次推进前重新判定：进行中的执行只能按启动时钉住的语义继续。
	def, decision := d.rt.reg.CheckVersion(e.DefName, e.DefVersion)
	if decision != VerAcceptCompatible {
		d.rejectInFlight(e, rev, decision)
		return 0, true
	}

	idx := nextStepIndex(e)
	if idx < 0 {
		// 所有步骤均已成功：进入 succeeded 终态（幂等，不会重复执行任何步骤）。
		if e.Status != ExecSucceeded {
			e.Status = ExecSucceeded
			e.UpdatedAt = time.Now().UTC()
			d.persist(e, rev)
			d.logEvent(e, Event{Type: EvSucceeded, Reason: "all steps succeeded"})
			log.Printf("[durable] execution=%s idempKey=%q SUCCEEDED", e.ID, e.IdempKey)
		}
		return 0, true
	}

	// 取消请求优先于任何新的步骤调度。
	if e.CancelRequested {
		return d.markCancelled(e, rev, def, idx)
	}

	st := &e.Steps[idx]

	// 退避窗口未到：计算剩余等待时间，期间不占并发额度。
	if st.NotBefore != nil && time.Now().UTC().Before(*st.NotBefore) {
		return time.Until(*st.NotBefore), false
	}

	// 准备步骤输入（首步=执行输入；后续=上一步持久化的输出）。
	input := e.Input
	if idx > 0 {
		input = e.Steps[idx-1].Output
	}
	if st.Status == StepPending {
		st.Input = input
	}

	// 可中断地获取并发额度：取消/关闭时不得长期占用或悬挂额度。
	if err := d.rt.acquireSlot(ctx); err != nil {
		return 0, false
	}

	now := time.Now().UTC()
	st.Status = StepRunning
	st.Attempts++
	st.StartedAt = &now
	st.NotBefore = nil
	e.Status = ExecRunning
	e.UpdatedAt = now
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:        EvStepStarted,
		Step:        st.Name,
		Attempt:     st.Attempts,
		MaxAttempts: d.rt.pol.MaxAttempts,
		Input:       st.Input,
	})

	// 执行步骤（步骤级超时；同时响应执行取消与进程关闭）。
	stepDef := def.Steps[idx]
	sctx, scancel := context.WithCancel(ctx)
	cur := d.armCancel()
	go func() {
		select {
		case <-cur:
			scancel()
		case <-sctx.Done():
		}
	}()
	var sctx2 context.Context = sctx
	var tcancel context.CancelFunc
	if stepDef.Timeout > 0 {
		sctx2, tcancel = context.WithTimeout(sctx, stepDef.Timeout)
	}
	out, callErr := stepDef.Fn(sctx2, st.Input)
	if tcancel != nil {
		tcancel()
	}
	scancel()
	d.rt.releaseSlot()

	// 步骤执行期间取消请求可能已由其它调用持久化：必须读盘取最新标记，
	// 不能信任本次推进开始时的内存快照，否则会把“取消”误判为瞬时错误去重试。
	if fresh, _, gerr := d.rt.store.GetExecution(ctx, d.id); gerr == nil {
		e.CancelRequested = fresh.CancelRequested
	}

	finish := time.Now().UTC()
	st.FinishedAt = &finish

	// 取消优先：无论是步骤返回 ctx 取消还是返回普通错误，只要取消意图已持久化，
	// 立即进入取消收尾（含补偿），绝不进入重试链路。
	if e.CancelRequested {
		return d.markCancelled(e, rev, def, idx)
	}

	// 超时识别为独立终态：立即终止，不进入重试链路；已成功步骤进入补偿。
	if sctx2.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		st.Status = StepTimedOut
		st.Failure = &Failure{Class: FailureTransient, Code: ReasonStepTimeout, Message: "step deadline exceeded"}
		e.Failure = &Failure{
			Class:   FailureTransient,
			Code:    ReasonStepTimeout,
			Message: "step " + st.Name + " timed out after " + stepDef.Timeout.String(),
		}
		log.Printf("[durable] execution=%s idempKey=%q TIMED_OUT step=%s attempt=%d timeout=%s",
			e.ID, e.IdempKey, st.Name, st.Attempts, stepDef.Timeout)
		return d.finishForward(e, rev, def, ExecTimedOut, st.Name, Event{
			Type: EvStepTimedOut, Step: st.Name, Attempt: st.Attempts, Reason: ReasonStepTimeout,
		})
	}

	// 进程关闭：不把关闭误判为步骤错误。running 步骤的副作用未得到确认，
	// 保守回退为 pending 并追加与重启恢复相同的重置事件，保证两条路径语义一致。
	if ctx.Err() != nil {
		st.Status = StepPending
		st.Attempts--
		st.StartedAt = nil
		st.FinishedAt = nil
		if e.Status == ExecRunning {
			e.Status = ExecPending
		}
		e.UpdatedAt = time.Now().UTC()
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:   EvRunningReset,
			Step:   st.Name,
			Reason: "runtime shutting down while step running; reset to pending for safe retry on restart",
		})
		return 0, false
	}

	if callErr == nil {
		st.Status = StepSucceeded
		st.Output = out
		st.Failure = nil
		e.Status = ExecPending
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvStepSucceeded, Step: st.Name, Attempt: st.Attempts, Output: out})
		log.Printf("[durable] execution=%s idempKey=%q step=%s SUCCEEDED attempt=%d",
			e.ID, e.IdempKey, st.Name, st.Attempts)
		return 0, false
	}

	// 失败分类：永久失败立即终态；瞬时失败按有上限退避重试。
	class, code := classify(callErr)
	f := &Failure{Class: class, Code: code, Message: callErr.Error()}
	st.Failure = f

	if class == FailurePermanent {
		st.Status = StepFailed
		e.Failure = &Failure{Class: FailurePermanent, Code: ReasonStepPermanent, Message: callErr.Error()}
		log.Printf("[durable] execution=%s idempKey=%q FAILED step=%s reason=%s permanent=true msg=%q",
			e.ID, e.IdempKey, st.Name, ReasonStepPermanent, callErr.Error())
		return d.finishForward(e, rev, def, ExecFailed, st.Name, Event{
			Type: EvStepFailed, Step: st.Name, Attempt: st.Attempts, Failure: f, Reason: ReasonStepPermanent,
		})
	}

	if st.Attempts < d.rt.pol.MaxAttempts {
		bo := nextBackoff(d.rt.pol, st.Attempts)
		nb := time.Now().UTC().Add(bo)
		st.Status = StepRetrying
		st.NotBefore = &nb
		f.Retriable = true
		e.Status = ExecPending
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:        EvStepRetrying,
			Step:        st.Name,
			Attempt:     st.Attempts,
			MaxAttempts: d.rt.pol.MaxAttempts,
			BackoffMS:   bo.Milliseconds(),
			NotBefore:   &nb,
			Failure:     f,
			Reason:      ReasonStepTransient,
			Detail:      "transient failure; scheduled retry with capped exponential backoff",
		})
		log.Printf("[durable] execution=%s idempKey=%q step=%s RETRY attempt=%d/%d backoff=%dms retryReason=%s",
			e.ID, e.IdempKey, st.Name, st.Attempts, d.rt.pol.MaxAttempts, bo.Milliseconds(), ReasonStepTransient)
		return bo, false
	}

	// 瞬时失败但已达重试上限：进入可区分终态，不残留重试调度；已成功步骤进入补偿。
	st.Status = StepFailed
	e.Failure = &Failure{Class: FailureTransient, Code: ReasonRetryLimitExceeded,
		Message: "step " + st.Name + " exhausted " + itoa(st.Attempts) + " attempts"}
	log.Printf("[durable] execution=%s idempKey=%q FAILED step=%s reason=%s attempts=%d/%d",
		e.ID, e.IdempKey, st.Name, ReasonRetryLimitExceeded, st.Attempts, d.rt.pol.MaxAttempts)
	return d.finishForward(e, rev, def, ExecFailed, st.Name, Event{
		Type:        EvStepFailed,
		Step:        st.Name,
		Attempt:     st.Attempts,
		MaxAttempts: d.rt.pol.MaxAttempts,
		Failure:     f,
		Reason:      ReasonRetryLimitExceeded,
	})
}

func (d *execDriver) rejectInFlight(e *Execution, rev int64, decision VersionDecision) {
	code := rejectCodeFor(decision)
	now := time.Now().UTC()
	e.Status = ExecRejected
	e.Failure = &Failure{Class: FailurePermanent, Code: code, Message: "in-flight definition check: " + string(decision)}
	e.UpdatedAt = now
	for i := range e.Steps {
		if e.Steps[i].Status != StepSucceeded {
			e.Steps[i].Status = StepCancelled
			e.Steps[i].FinishedAt = &now
		}
	}
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:            EvRejected,
		VersionDecision: string(decision),
		Reason:          code,
		Detail:          "in-flight execution must keep start-time semantics; incompatible definitions are never replayed",
	})
	log.Printf("[durable] execution=%s idempKey=%q REJECTED versionDecision=%s reason=%s",
		e.ID, e.IdempKey, decision, code)
}

func (d *execDriver) markCancelled(e *Execution, rev int64, def Definition, idx int) (time.Duration, bool) {
	now := time.Now().UTC()
	e.Failure = &Failure{Class: FailurePermanent, Code: ReasonExecutionCancelled, Message: "execution cancelled"}
	for i := idx; i < len(e.Steps); i++ {
		if e.Steps[i].Status != StepSucceeded {
			e.Steps[i].Status = StepCancelled
			e.Steps[i].FinishedAt = &now
		}
	}
	return d.finishForward(e, rev, def, ExecCancelled, "", Event{Type: EvCancelled, Reason: ReasonExecutionCancelled})
}

// finishForward 在正向流程无法继续时统一收尾：
//   - e.Failure 已由调用方写入原始触发原因（之后绝不被覆盖）；
//   - 若存在“已成功且声明了补偿”的步骤，则进入 compensating 继续驱动；
//   - 否则直接落入原始终态（failed/cancelled/timed_out）。
//
// 重复触发（如取消与超时同时发生、重启后恢复）在此收敛：
// 只有正向状态会走到这里，一旦进入 compensating 便不会重复建设计划。
func (d *execDriver) finishForward(e *Execution, rev int64, def Definition, finalStatus ExecStatus, triggerStep string, triggerEvent Event) (time.Duration, bool) {
	now := time.Now().UTC()
	e.UpdatedAt = now
	plan, hasComp := buildCompPlan(e, def)
	if !hasComp {
		e.Status = finalStatus
		d.persist(e, rev)
		if triggerEvent.Type != "" {
			d.logEvent(e, triggerEvent)
		}
		log.Printf("[durable] execution=%s idempKey=%q %s (no compensation declared)",
			e.ID, e.IdempKey, statusLogWord(finalStatus))
		return 0, true
	}
	e.Status = ExecCompensating
	e.Compensation = &Compensation{
		Status:      CompRunning,
		Trigger:     e.Failure.Code,
		TriggerStep: triggerStep,
		FinalStatus: finalStatus,
		Steps:       plan,
		StartedAt:   &now,
	}
	d.persist(e, rev)
	if triggerEvent.Type != "" {
		d.logEvent(e, triggerEvent)
	}
	for i := range plan {
		if plan[i].Status == CompSkipped && e.Steps[i].Status == StepSucceeded {
			d.logEvent(e, Event{Type: EvCompStepSkipped, Step: plan[i].Name, Reason: "step succeeded but declares no compensation; skipped"})
		}
	}
	d.logEvent(e, Event{
		Type:   EvCompensationStarted,
		Step:   triggerStep,
		Reason: e.Failure.Code,
		Detail: "forward flow cannot continue; compensating succeeded steps in reverse order",
	})
	log.Printf("[durable] execution=%s idempKey=%q COMPENSATING trigger=%s triggerStep=%q finalStatus=%s",
		e.ID, e.IdempKey, e.Failure.Code, triggerStep, finalStatus)
	return 0, false
}

// buildCompPlan 依据钉住的定义生成补偿计划：已成功且声明补偿的步骤为 pending，
// 其余（未成功或未声明补偿）为 skipped。返回是否存在待补偿步骤。
func buildCompPlan(e *Execution, def Definition) ([]CompState, bool) {
	plan := make([]CompState, len(e.Steps))
	has := false
	for i := range e.Steps {
		cs := CompState{Name: e.Steps[i].Name, Status: CompSkipped}
		if e.Steps[i].Status == StepSucceeded && i < len(def.Steps) && def.Steps[i].Compensate != nil {
			cs.Status = CompPending
			has = true
		}
		plan[i] = cs
	}
	return plan, has
}

func statusLogWord(s ExecStatus) string {
	switch s {
	case ExecFailed:
		return "FAILED"
	case ExecCancelled:
		return "CANCELLED"
	case ExecTimedOut:
		return "TIMED_OUT"
	}
	return string(s)
}

func nextStepIndex(e *Execution) int {
	for i := range e.Steps {
		if e.Steps[i].Status != StepSucceeded {
			return i
		}
	}
	return -1
}

// persist 以 CAS 保存；遇到并发修改（例如取消）时重新读取并重试有限次数。
func (d *execDriver) persist(e *Execution, rev int64) {
	for attempt := 0; ; attempt++ {
		newRev, err := d.rt.store.SaveExecution(d.rt.ctx, e, rev)
		if err == nil {
			rev = newRev
			e.Rev = newRev
			return
		}
		if !errors.Is(err, ErrConflict) || attempt >= 5 {
			log.Printf("[durable] execution=%s persist error: %v", e.ID, err)
			return
		}
		fresh, freshRev, gerr := d.rt.store.GetExecution(d.rt.ctx, e.ID)
		if gerr != nil {
			log.Printf("[durable] execution=%s reload after conflict error: %v", e.ID, gerr)
			return
		}
		// 以磁盘最新状态为准合并取消意图，避免与取消请求互相覆盖。
		e.CancelRequested = e.CancelRequested || fresh.CancelRequested
		rev = freshRev
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
}

func (d *execDriver) logEvent(e *Execution, ev Event) {
	// 事件序号由 Runtime 按日志行统一分配，保证恢复后仍单调连续。
	if err := d.rt.appendEvent(e, ev); err != nil {
		log.Printf("[durable] execution=%s append event error: %v", e.ID, err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// ---------------- 补偿（回滚）驱动 ----------------

// driveCompensation 推进一次补偿：按相反顺序依次执行已声明的补偿动作。
// 补偿状态全部落盘，重启后从未补偿完的地方继续；已成功的补偿不会重复执行。
func (d *execDriver) driveCompensation(e *Execution, rev int64) (wait time.Duration, terminal bool) {
	ctx := d.rt.ctx
	comp := e.Compensation
	if comp == nil {
		// 防御：状态不一致（不应发生）。按补偿失败收尾，绝不静默继续。
		e.Status = ExecCompensationFailed
		e.UpdatedAt = time.Now().UTC()
		d.persist(e, rev)
		return 0, true
	}

	// 版本判定：补偿必须按启动时钉住的定义版本语义执行；
	// 版本不可用时明确拒绝，绝不悄悄换成新版本去补偿。
	def, decision := d.rt.reg.CheckVersion(e.DefName, e.DefVersion)
	if decision != VerAcceptCompatible {
		d.rejectCompensation(e, rev, decision)
		return 0, true
	}

	// 逆序找下一个待补偿步骤（pending/retrying）。
	idx := -1
	for i := len(comp.Steps) - 1; i >= 0; i-- {
		s := comp.Steps[i].Status
		if s == CompPending || s == CompRetrying {
			idx = i
			break
		}
	}
	if idx < 0 {
		// 全部补偿完成：落入触发时记录的原始终态，原始失败原因保持不变。
		now := time.Now().UTC()
		comp.Status = CompSucceeded
		comp.FinishedAt = &now
		e.Status = comp.FinalStatus
		e.UpdatedAt = now
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:   EvCompensationFinished,
			Reason: comp.Trigger,
			Detail: "all declared compensations completed; original failure preserved",
		})
		log.Printf("[durable] execution=%s idempKey=%q COMPENSATION_FINISHED trigger=%s finalStatus=%s",
			e.ID, e.IdempKey, comp.Trigger, comp.FinalStatus)
		return 0, true
	}

	cs := &comp.Steps[idx]

	// 补偿退避窗口未到：等待期间不占并发额度。
	if cs.NotBefore != nil && time.Now().UTC().Before(*cs.NotBefore) {
		return time.Until(*cs.NotBefore), false
	}

	if err := d.rt.acquireSlot(ctx); err != nil {
		return 0, false
	}

	now := time.Now().UTC()
	cs.Status = CompRunning
	cs.Attempts++
	cs.StartedAt = &now
	cs.NotBefore = nil
	e.UpdatedAt = now
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:        EvCompStepStarted,
		Step:        cs.Name,
		Attempt:     cs.Attempts,
		MaxAttempts: d.rt.compPol.MaxAttempts,
		Reason:      comp.Trigger,
	})
	log.Printf("[durable] execution=%s idempKey=%q COMP step=%s attempt=%d/%d trigger=%s",
		e.ID, e.IdempKey, cs.Name, cs.Attempts, d.rt.compPol.MaxAttempts, comp.Trigger)

	// 补偿输入：该步骤成功时的输出（无输出时回退为其输入）。
	input := e.Steps[idx].Output
	if len(input) == 0 {
		input = e.Steps[idx].Input
	}
	stepDef := def.Steps[idx]

	// 异步执行 + 步骤级超时：即使补偿函数不理会取消信号，
	// 超时后也能尽快给出结论并释放额度（遗留 goroutine 的结果写入缓冲通道后被丢弃）。
	resCh := make(chan error, 1)
	cctx, ccancel := context.WithCancel(ctx)
	go func() {
		_, err := stepDef.Compensate(cctx, input)
		resCh <- err
	}()
	var timeout <-chan time.Time
	var timer *time.Timer
	if stepDef.CompensateTimeout > 0 {
		timer = time.NewTimer(stepDef.CompensateTimeout)
		timeout = timer.C
	}
	var callErr error
	timedOut := false
	shutdown := false
	select {
	case callErr = <-resCh:
	case <-timeout:
		timedOut = true
	case <-ctx.Done():
		shutdown = true
	}
	if timer != nil {
		timer.Stop()
	}
	ccancel()
	d.rt.releaseSlot()

	finish := time.Now().UTC()

	// 进程关闭：补偿副作用未确认，保守回退为 pending（崩溃恢复同款语义），
	// 重启后从该步骤继续补偿；不把中断的中间态当成已完成。
	if shutdown {
		cs.Status = CompPending
		cs.Attempts--
		cs.StartedAt = nil
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:   EvRunningReset,
			Step:   cs.Name,
			Reason: "runtime shutting down while compensation running; reset to pending for safe retry on restart",
		})
		return 0, false
	}

	cs.FinishedAt = &finish

	if timedOut {
		f := &Failure{Class: FailureTransient, Code: ReasonCompensationTimeout,
			Message: "compensation for step " + cs.Name + " exceeded " + stepDef.CompensateTimeout.String()}
		cs.Failure = f
		log.Printf("[durable] execution=%s idempKey=%q COMP step=%s TIMEOUT attempt=%d timeout=%s",
			e.ID, e.IdempKey, cs.Name, cs.Attempts, stepDef.CompensateTimeout)
		return d.failOrRetryComp(e, rev, cs, comp, f, finish)
	}

	if callErr == nil {
		cs.Status = CompSucceeded
		cs.Failure = nil
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvCompStepSucceeded, Step: cs.Name, Attempt: cs.Attempts})
		log.Printf("[durable] execution=%s idempKey=%q COMP step=%s SUCCEEDED attempt=%d",
			e.ID, e.IdempKey, cs.Name, cs.Attempts)
		return 0, false
	}

	class, code := classify(callErr)
	f := &Failure{Class: class, Code: code, Message: callErr.Error()}
	cs.Failure = f
	if class == FailurePermanent {
		return d.failCompensation(e, rev, cs, comp, f, finish)
	}
	return d.failOrRetryComp(e, rev, cs, comp, f, finish)
}

// failOrRetryComp 补偿失败：未达上限则按独立策略退避重试，否则整体补偿失败终态。
func (d *execDriver) failOrRetryComp(e *Execution, rev int64, cs *CompState, comp *Compensation, f *Failure, finish time.Time) (time.Duration, bool) {
	if cs.Attempts < d.rt.compPol.MaxAttempts {
		bo := nextBackoff(d.rt.compPol, cs.Attempts)
		nb := finish.Add(bo)
		cs.Status = CompRetrying
		cs.NotBefore = &nb
		f.Retriable = true
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:        EvCompStepRetrying,
			Step:        cs.Name,
			Attempt:     cs.Attempts,
			MaxAttempts: d.rt.compPol.MaxAttempts,
			BackoffMS:   bo.Milliseconds(),
			NotBefore:   &nb,
			Failure:     f,
			Reason:      f.Code,
			Detail:      "compensation failed; scheduled retry with capped exponential backoff",
		})
		log.Printf("[durable] execution=%s idempKey=%q COMP step=%s RETRY attempt=%d/%d backoff=%dms reason=%s",
			e.ID, e.IdempKey, cs.Name, cs.Attempts, d.rt.compPol.MaxAttempts, bo.Milliseconds(), f.Code)
		return bo, false
	}
	return d.failCompensation(e, rev, cs, comp, f, finish)
}

// failCompensation 补偿整体失败终态：与正向流程失败可区分；
// Execution.Failure 中的原始触发原因保留，不被覆盖。
func (d *execDriver) failCompensation(e *Execution, rev int64, cs *CompState, comp *Compensation, f *Failure, finish time.Time) (time.Duration, bool) {
	cs.Status = CompFailed
	comp.Status = CompFailed
	comp.FinishedAt = &finish
	code := ReasonCompensationFailed
	if f.Code == ReasonCompensationTimeout {
		code = ReasonCompensationTimeout
	}
	comp.Failure = &Failure{
		Class:   f.Class,
		Code:    code,
		Message: "compensation for step " + cs.Name + " failed after " + itoa(cs.Attempts) + " attempt(s): " + f.Message,
	}
	e.Status = ExecCompensationFailed
	e.UpdatedAt = finish
	d.persist(e, rev)
	d.logEvent(e, Event{Type: EvCompStepFailed, Step: cs.Name, Attempt: cs.Attempts, Failure: f, Reason: f.Code})
	d.logEvent(e, Event{
		Type:   EvCompensationFailed,
		Step:   cs.Name,
		Reason: code,
		Detail: "compensation exhausted; original trigger " + comp.Trigger + " preserved in execution failure",
	})
	log.Printf("[durable] execution=%s idempKey=%q COMPENSATION_FAILED step=%s reason=%s attempts=%d/%d trigger=%s",
		e.ID, e.IdempKey, cs.Name, code, cs.Attempts, d.rt.compPol.MaxAttempts, comp.Trigger)
	return 0, true
}

// rejectCompensation 补偿阶段发现钉住的定义版本不可用：明确拒绝。
// 拒绝原因记入补偿记录（与补偿执行失败可区分），原始触发原因保留在 Execution.Failure。
func (d *execDriver) rejectCompensation(e *Execution, rev int64, decision VersionDecision) {
	code := rejectCodeFor(decision)
	now := time.Now().UTC()
	e.Status = ExecRejected
	e.Compensation.Failure = &Failure{
		Class:   FailurePermanent,
		Code:    code,
		Message: "compensation requires the pinned definition: " + string(decision),
	}
	e.UpdatedAt = now
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:            EvRejected,
		VersionDecision: string(decision),
		Reason:          code,
		Detail:          "definition unavailable during compensation; never silently fall back to a new version",
	})
	log.Printf("[durable] execution=%s idempKey=%q COMPENSATION_REJECTED versionDecision=%s reason=%s trigger=%s",
		e.ID, e.IdempKey, decision, code, e.Compensation.Trigger)
}
