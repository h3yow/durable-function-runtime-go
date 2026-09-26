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
		d.markCancelled(e, rev, idx)
		return 0, true
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
	// 立即进入 cancelled 终态，绝不进入重试链路。
	if e.CancelRequested {
		d.markCancelled(e, rev, idx)
		return 0, true
	}

	// 超时识别为独立终态：立即终止，不进入重试链路。
	if sctx2.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		st.Status = StepTimedOut
		st.Failure = &Failure{Class: FailureTransient, Code: ReasonStepTimeout, Message: "step deadline exceeded"}
		e.Status = ExecTimedOut
		e.Failure = &Failure{
			Class:   FailureTransient,
			Code:    ReasonStepTimeout,
			Message: "step " + st.Name + " timed out after " + stepDef.Timeout.String(),
		}
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvStepTimedOut, Step: st.Name, Attempt: st.Attempts, Reason: ReasonStepTimeout})
		log.Printf("[durable] execution=%s idempKey=%q TIMED_OUT step=%s attempt=%d timeout=%s",
			e.ID, e.IdempKey, st.Name, st.Attempts, stepDef.Timeout)
		return 0, true
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
		e.Status = ExecFailed
		e.Failure = &Failure{Class: FailurePermanent, Code: ReasonStepPermanent, Message: callErr.Error()}
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvStepFailed, Step: st.Name, Attempt: st.Attempts, Failure: f, Reason: ReasonStepPermanent})
		log.Printf("[durable] execution=%s idempKey=%q FAILED step=%s reason=%s permanent=true msg=%q",
			e.ID, e.IdempKey, st.Name, ReasonStepPermanent, callErr.Error())
		return 0, true
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

	// 瞬时失败但已达重试上限：进入可区分终态，不残留重试调度。
	st.Status = StepFailed
	e.Status = ExecFailed
	e.Failure = &Failure{Class: FailureTransient, Code: ReasonRetryLimitExceeded,
		Message: "step " + st.Name + " exhausted " + itoa(st.Attempts) + " attempts"}
	e.UpdatedAt = finish
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:        EvStepFailed,
		Step:        st.Name,
		Attempt:     st.Attempts,
		MaxAttempts: d.rt.pol.MaxAttempts,
		Failure:     f,
		Reason:      ReasonRetryLimitExceeded,
	})
	log.Printf("[durable] execution=%s idempKey=%q FAILED step=%s reason=%s attempts=%d/%d",
		e.ID, e.IdempKey, st.Name, ReasonRetryLimitExceeded, st.Attempts, d.rt.pol.MaxAttempts)
	return 0, true
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

func (d *execDriver) markCancelled(e *Execution, rev int64, idx int) {
	now := time.Now().UTC()
	e.Status = ExecCancelled
	e.Failure = &Failure{Class: FailurePermanent, Code: ReasonExecutionCancelled, Message: "execution cancelled"}
	e.UpdatedAt = now
	for i := idx; i < len(e.Steps); i++ {
		if e.Steps[i].Status != StepSucceeded {
			e.Steps[i].Status = StepCancelled
			e.Steps[i].FinishedAt = &now
		}
	}
	d.persist(e, rev)
	d.logEvent(e, Event{Type: EvCancelled, Reason: ReasonExecutionCancelled})
	log.Printf("[durable] execution=%s idempKey=%q CANCELLED", e.ID, e.IdempKey)
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
