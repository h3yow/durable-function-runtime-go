package durable

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

// beginCompensation 在正向流程无法走完时尝试进入补偿阶段。
//
// 仅当“已成功步骤中至少有一步声明了补偿动作”时才进入 compensating：
//   - 已成功且声明补偿的步骤按相反顺序加入补偿队列（pending）；
//   - 已成功但未声明补偿的步骤标记为 skipped，不影响其它步骤的补偿；
//   - 没有任何已声明补偿时返回 false，调用方按原有逻辑直接进入正向终态。
//
// 触发补偿的原始原因（哪一步失败/超时/取消）保存在 Comp.Trigger 与 Execution.Failure 中，
// 补偿全过程不得覆盖。
func (d *execDriver) beginCompensation(e *Execution, rev int64, def Definition, origin ExecStatus, trigger Failure, triggerStep string) bool {
	if e.Comp != nil {
		// 补偿已被触发过（取消与超时同时发生、重启恢复等）：收敛到同一份进行中的补偿。
		return e.Status == ExecCompensating
	}
	now := time.Now().UTC()
	declared := 0
	states := make([]CompState, 0, len(e.Steps))
	for i := len(e.Steps) - 1; i >= 0; i-- {
		st := e.Steps[i]
		if st.Status != StepSucceeded {
			continue
		}
		cs := CompState{Step: st.Name, Status: CompPending}
		if findCompFn(def, st.Name) == nil {
			cs.Status = CompSkipped
			cs.FinishedAt = &now
		} else {
			declared++
		}
		states = append(states, cs)
	}
	if declared == 0 {
		return false
	}

	trig := trigger // 拷贝，避免与调用方共享指针
	e.Comp = &CompensationState{
		OriginStatus: origin,
		Trigger:      trig,
		TriggerStep:  triggerStep,
		Steps:        states,
	}
	e.Status = ExecCompensating
	e.Failure = &trig
	e.UpdatedAt = now
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:            EvCompStarted,
		Step:            triggerStep,
		Reason:          trigger.Code,
		Failure:         &trig,
		VersionDecision: string(VerAcceptCompatible),
		Detail:          "forward flow cannot complete; compensating succeeded steps in reverse order",
	})
	for _, cs := range states {
		if cs.Status == CompSkipped {
			d.logEvent(e, Event{
				Type:   EvCompStepSkipped,
				Step:   cs.Step,
				Reason: trigger.Code,
				Detail: "step declares no compensation; skipped without affecting others",
			})
		}
	}
	log.Printf("[durable] execution=%s idempKey=%q COMPENSATION started origin=%s trigger=%s triggerStep=%q declared=%d skipped=%d",
		e.ID, e.IdempKey, origin, trigger.Code, triggerStep, declared, len(states)-declared)
	return true
}

// driveCompensation 推进一次补偿。补偿队列已按相反顺序排列，逐个推进；
// 退避等待期间不占用并发额度；已补偿成功的步骤绝不重复补偿。
func (d *execDriver) driveCompensation(e *Execution, rev int64, def Definition) (wait time.Duration, terminal bool) {
	ctx := d.rt.ctx
	comp := e.Comp
	if comp == nil {
		// 防御：状态文件缺少补偿进度，不能当作已完成，明确落入补偿失败终态。
		d.failCompensation(e, rev, "", ReasonCompPermanent, "compensation state missing; cannot prove progress")
		return 0, true
	}

	idx := -1
	for i := range comp.Steps {
		s := comp.Steps[i].Status
		if s == CompPending || s == CompRetrying {
			idx = i
			break
		}
	}
	if idx < 0 {
		// 全部补偿完成（成功或跳过）：回到触发补偿时的正向终态，原始原因保持不变。
		now := time.Now().UTC()
		e.Status = comp.OriginStatus
		e.UpdatedAt = now
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:   EvCompensated,
			Reason: comp.Trigger.Code,
			Detail: "all declared compensations completed; original trigger preserved",
		})
		log.Printf("[durable] execution=%s idempKey=%q COMPENSATED -> %s trigger=%s",
			e.ID, e.IdempKey, comp.OriginStatus, comp.Trigger.Code)
		return 0, true
	}

	cs := &comp.Steps[idx]
	if cs.NotBefore != nil && time.Now().UTC().Before(*cs.NotBefore) {
		return time.Until(*cs.NotBefore), false
	}

	compFn := findCompFn(def, cs.Step)
	if compFn == nil {
		// 钉住版本的定义里找不到该步的补偿动作：不能静默按新语义补偿，明确失败。
		cs.Status = CompFailed
		now := time.Now().UTC()
		cs.FinishedAt = &now
		cs.Failure = &Failure{Class: FailurePermanent, Code: ReasonCompDefUnavailable,
			Message: "compensation action not found in pinned definition for step " + cs.Step}
		d.failCompensation(e, rev, cs.Step, ReasonCompDefUnavailable, cs.Failure.Message)
		return 0, true
	}

	// 可中断地获取并发额度；等待期间不占额度。
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
		Step:        cs.Step,
		Attempt:     cs.Attempts,
		MaxAttempts: d.rt.compPol.MaxAttempts,
		Reason:      comp.Trigger.Code,
	})
	log.Printf("[durable] execution=%s idempKey=%q COMP step=%s attempt=%d/%d trigger=%s",
		e.ID, e.IdempKey, cs.Step, cs.Attempts, d.rt.compPol.MaxAttempts, comp.Trigger.Code)

	// 补偿输入：该步已持久化的输出（无输出时回退为该步输入）。
	input := compInput(e, cs.Step)

	// 在独立 goroutine 中执行补偿动作：即使它不理会取消信号，
	// 步骤级超时到期后也能尽快给出结论并释放并发额度。
	type compResult struct {
		out json.RawMessage
		err error
	}
	resCh := make(chan compResult, 1)
	cctx, ccancel := context.WithCancel(context.Background())
	go func() {
		out, err := compFn(cctx, input)
		resCh <- compResult{out, err}
	}()
	compTimeout := findCompTimeout(def, cs.Step)
	var timer *time.Timer
	var timeoutC <-chan time.Time
	if compTimeout > 0 {
		timer = time.NewTimer(compTimeout)
		timeoutC = timer.C
	}

	var res compResult
	timedOut := false
	shutdown := false
	select {
	case res = <-resCh:
	case <-timeoutC:
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
	cs.FinishedAt = &finish

	switch {
	case shutdown:
		// 进程关闭：补偿副作用未确认，保守复位为 pending（不当作已完成），
		// 重启恢复后从未完成处继续，且不计入已用尝试。
		cs.Status = CompPending
		cs.Attempts--
		cs.StartedAt = nil
		cs.FinishedAt = nil
		e.UpdatedAt = time.Now().UTC()
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:   EvRunningReset,
			Step:   cs.Step,
			Reason: "runtime shutting down during compensation; reset to pending for safe resume",
		})
		return 0, false

	case timedOut:
		// 补偿超时：立即给出结论，额度已释放，不再重试。
		cs.Status = CompTimedOut
		cs.Failure = &Failure{Class: FailureTransient, Code: ReasonCompTimeout,
			Message: "compensation deadline exceeded after " + compTimeout.String()}
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvCompStepTimeout, Step: cs.Step, Attempt: cs.Attempts, Reason: ReasonCompTimeout})
		d.failCompensation(e, rev, cs.Step, ReasonCompTimeout,
			"compensation for step "+cs.Step+" timed out after "+compTimeout.String())
		return 0, true

	case res.err == nil:
		cs.Status = CompSucceeded
		cs.Failure = nil
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvCompStepOK, Step: cs.Step, Attempt: cs.Attempts, Output: res.out})
		log.Printf("[durable] execution=%s idempKey=%q COMP step=%s SUCCEEDED attempt=%d",
			e.ID, e.IdempKey, cs.Step, cs.Attempts)
		return 0, false
	}

	// 补偿失败分类：永久失败立即终态；瞬时失败按独立的有上限退避策略重试。
	class, code := classify(res.err)
	f := &Failure{Class: class, Code: code, Message: res.err.Error()}
	cs.Failure = f

	if class == FailurePermanent {
		cs.Status = CompFailed
		d.persist(e, rev)
		d.logEvent(e, Event{Type: EvCompStepFailed, Step: cs.Step, Attempt: cs.Attempts, Failure: f, Reason: ReasonCompPermanent})
		d.failCompensation(e, rev, cs.Step, ReasonCompPermanent, res.err.Error())
		return 0, true
	}

	if cs.Attempts < d.rt.compPol.MaxAttempts {
		bo := nextBackoff(d.rt.compPol, cs.Attempts)
		nb := time.Now().UTC().Add(bo)
		cs.Status = CompRetrying
		cs.NotBefore = &nb
		f.Retriable = true
		e.UpdatedAt = finish
		d.persist(e, rev)
		d.logEvent(e, Event{
			Type:        EvCompStepRetry,
			Step:        cs.Step,
			Attempt:     cs.Attempts,
			MaxAttempts: d.rt.compPol.MaxAttempts,
			BackoffMS:   bo.Milliseconds(),
			NotBefore:   &nb,
			Failure:     f,
			Reason:      ReasonStepTransient,
			Detail:      "transient compensation failure; independent capped backoff",
		})
		log.Printf("[durable] execution=%s idempKey=%q COMP step=%s RETRY attempt=%d/%d backoff=%dms",
			e.ID, e.IdempKey, cs.Step, cs.Attempts, d.rt.compPol.MaxAttempts, bo.Milliseconds())
		return bo, false
	}

	// 补偿重试上限耗尽：进入与正向失败可区分的终态。
	cs.Status = CompFailed
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:        EvCompStepFailed,
		Step:        cs.Step,
		Attempt:     cs.Attempts,
		MaxAttempts: d.rt.compPol.MaxAttempts,
		Failure:     f,
		Reason:      ReasonCompRetryExhausted,
	})
	d.failCompensation(e, rev, cs.Step, ReasonCompRetryExhausted,
		"compensation for step "+cs.Step+" exhausted "+itoa(cs.Attempts)+" attempts")
	return 0, true
}

// failCompensation 将执行置入 compensation_failed 终态。
// 原始触发原因保留在 Comp.Trigger 中，不被覆盖。
func (d *execDriver) failCompensation(e *Execution, rev int64, step, code, msg string) {
	now := time.Now().UTC()
	e.Status = ExecCompensationFailed
	e.Failure = &Failure{Class: FailurePermanent, Code: code, Message: msg}
	e.UpdatedAt = now
	d.persist(e, rev)
	trigger := ""
	if e.Comp != nil {
		trigger = e.Comp.Trigger.Code
	}
	d.logEvent(e, Event{
		Type:    EvCompFailed,
		Step:    step,
		Reason:  code,
		Failure: e.Failure,
		Detail:  "compensation failed; original trigger preserved: " + trigger,
	})
	log.Printf("[durable] execution=%s idempKey=%q COMPENSATION_FAILED step=%q reason=%s trigger=%s msg=%q",
		e.ID, e.IdempKey, step, code, trigger, msg)
}

// rejectCompensation 在补偿阶段发现钉住的定义版本不可用：明确拒绝，
// 原因码与补偿执行失败本身可区分，绝不静默切换到新版本补偿。
func (d *execDriver) rejectCompensation(e *Execution, rev int64, decision VersionDecision) {
	now := time.Now().UTC()
	e.Status = ExecCompensationFailed
	e.Failure = &Failure{
		Class:   FailurePermanent,
		Code:    ReasonCompDefUnavailable,
		Message: "pinned definition unavailable during compensation: " + string(decision),
	}
	e.UpdatedAt = now
	d.persist(e, rev)
	d.logEvent(e, Event{
		Type:            EvCompRejected,
		VersionDecision: string(decision),
		Reason:          ReasonCompDefUnavailable,
		Detail:          "compensation must keep start-time semantics; never falls back to a newer definition",
	})
	log.Printf("[durable] execution=%s idempKey=%q COMPENSATION_REJECTED versionDecision=%s reason=%s",
		e.ID, e.IdempKey, decision, ReasonCompDefUnavailable)
}

// findCompFn 在（钉住版本的）定义中查找某步声明的补偿动作。
func findCompFn(def Definition, step string) StepFunc {
	for i := range def.Steps {
		if def.Steps[i].Name == step {
			return def.Steps[i].Compensate
		}
	}
	return nil
}

func findCompTimeout(def Definition, step string) time.Duration {
	for i := range def.Steps {
		if def.Steps[i].Name == step {
			return def.Steps[i].CompTimeout
		}
	}
	return 0
}

// compInput 取补偿动作的输入：该步已持久化的输出，无输出时回退为该步输入。
func compInput(e *Execution, step string) json.RawMessage {
	for i := range e.Steps {
		if e.Steps[i].Name == step {
			if len(e.Steps[i].Output) > 0 {
				return e.Steps[i].Output
			}
			return e.Steps[i].Input
		}
	}
	return e.Input
}
