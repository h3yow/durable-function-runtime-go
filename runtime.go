package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const defaultMaxConcurrent = 8

// StartResult 为一次 Start 调用的结论。
//
// NewExecution=true 表示本次请求新建了执行；false 表示命中了既有幂等占用，
// 调用方应据 IdempKey 与既有执行收敛到同一结果。
type StartResult struct {
	ExecutionID  string
	IdempKey     string
	NewExecution bool
}

// Runtime 是单机持久化函数编排运行时。
type Runtime struct {
	cfg   Config
	reg   *Registry
	store Store
	pol   RetryPolicy
	// compPol 为补偿动作独立的退避重试策略，与正向流程互不影响。
	compPol RetryPolicy

	sem   chan struct{} // 并发额度（中断式信号量）
	slots int64         // 当前实际占用的步骤槽位数

	driversMu sync.Mutex
	drivers   map[string]*execDriver
	wakeAll   chan struct{}

	// recovered 在恢复扫描完成前保持开启；dispatcher 必须等它关闭后才调度，
	// 避免恢复期间与驱动 goroutine 并发写同一执行。
	recovered chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed atomic.Bool
}

// New 创建运行时：打开本地存储、注册定义集合，并对历史执行做恢复扫描。
// 恢复期间不会执行任何步骤副作用，只做状态校验、孤儿补建与重调度。
func New(ctx context.Context, cfg Config, reg *Registry) (*Runtime, error) {
	if reg == nil {
		return nil, errors.New("durable: nil registry")
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = defaultMaxConcurrent
	}
	pol := cfg.Retry
	if pol.MaxAttempts <= 0 {
		pol.MaxAttempts = 1
	}
	if pol.Multiplier <= 0 {
		pol.Multiplier = 2
	}
	compPol := cfg.CompRetry
	if compPol.MaxAttempts <= 0 {
		compPol = DefaultRetryPolicy()
	}
	if compPol.Multiplier <= 0 {
		compPol.Multiplier = 2
	}

	store, err := NewFileStore(ctx, cfg.Dir)
	if err != nil {
		return nil, err
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	rt := &Runtime{
		cfg:       cfg,
		reg:       reg,
		store:     store,
		pol:       pol,
		compPol:   compPol,
		sem:       make(chan struct{}, cfg.MaxConcurrent),
		drivers:   map[string]*execDriver{},
		wakeAll:   make(chan struct{}, 1),
		recovered: make(chan struct{}),
		ctx:       rootCtx,
		cancel:    cancel,
	}

	rt.wg.Add(1)
	go rt.dispatcher()

	if err := rt.recover(ctx); err != nil {
		close(rt.recovered) // 释放 dispatcher，使其能随 Close 退出
		_ = rt.Close()
		return nil, err
	}
	close(rt.recovered)
	rt.kick()
	return rt, nil
}

// Start 幂等提交一次编排执行。
func (rt *Runtime) Start(ctx context.Context, defName, defVersion, idempKey string, input json.RawMessage) (StartResult, error) {
	if rt.closed.Load() {
		return StartResult{}, errors.New("durable: runtime closed")
	}
	if idempKey == "" {
		return StartResult{}, errors.New("durable: idempotency key is required")
	}

	// 1) 定义版本校验：不兼容必须在产生任何副作用前明确拒绝。
	def, decision := rt.reg.CheckVersion(defName, defVersion)
	if decision != VerAcceptCompatible {
		code := rejectCodeFor(decision)
		log.Printf("[durable] reject start idempKey=%q def=%s@%s versionDecision=%s reason=%s",
			idempKey, defName, defVersion, decision, code)
		return StartResult{}, &RejectError{
			Code:    code,
			Message: fmt.Sprintf("def=%s@%s decision=%s", defName, defVersion, decision),
		}
	}

	fp := requestFingerprint(defName, defVersion, input)
	execID := executionID(idempKey)
	now := time.Now().UTC()
	claim := IdempotencyClaim{
		Key:         idempKey,
		ExecutionID: execID,
		RequestHash: fp,
		DefName:     defName,
		DefVersion:  defVersion,
		Input:       input,
		CreatedAt:   now,
	}

	// 2) 互斥建档。并发重复提交在此处收敛为唯一占用者。
	cerr := rt.store.CreateClaim(ctx, claim)
	if cerr != nil {
		if !errors.Is(cerr, ErrClaimExists) {
			return StartResult{}, cerr
		}
		existing, gerr := rt.store.GetClaim(ctx, idempKey)
		if gerr != nil {
			return StartResult{}, gerr
		}
		if existing.RequestHash != fp || existing.DefName != defName || existing.DefVersion != defVersion {
			log.Printf("[durable] idempotency CONFLICT idempKey=%q existingExec=%s: same key, different request",
				idempKey, existing.ExecutionID)
			return StartResult{}, &IdempotencyError{
				Code:        ReasonIdempotencyConflict,
				Message:     fmt.Sprintf("idempotency key %q already bound to execution %s with a different request", idempKey, existing.ExecutionID),
				ExecutionID: existing.ExecutionID,
				Duplicate:   false,
			}
		}
		log.Printf("[durable] idempotency DEDUP idempKey=%q -> execution %s (converged)", idempKey, existing.ExecutionID)
		rt.kick()
		return StartResult{
			ExecutionID:  existing.ExecutionID,
			IdempKey:     idempKey,
			NewExecution: false,
		}, nil
	}

	// 3) 唯一占用者：持久化 pending 执行（按钉住的定义版本初始化步骤）。
	e := newExecution(execID, idempKey, def, input, now)
	if _, err := rt.store.SaveExecution(ctx, e, 0); err != nil {
		return StartResult{}, fmt.Errorf("durable: persist new execution: %w", err)
	}
	if err := rt.appendEvent(e, Event{
		Type:            EvCreated,
		IdempKey:        idempKey,
		DefName:         defName,
		DefVersion:      defVersion,
		VersionDecision: string(VerAcceptCompatible),
		Input:           input,
	}); err != nil {
		return StartResult{}, err
	}
	log.Printf("[durable] created idempKey=%q execution=%s def=%s@%s steps=%d",
		idempKey, execID, defName, defVersion, len(def.Steps))

	rt.kick()
	return StartResult{ExecutionID: execID, IdempKey: idempKey, NewExecution: true}, nil
}

func newExecution(id, idempKey string, def Definition, input json.RawMessage, now time.Time) *Execution {
	steps := make([]StepState, len(def.Steps))
	for i, s := range def.Steps {
		steps[i] = StepState{Name: s.Name, Status: StepPending}
	}
	return &Execution{
		ID:         id,
		IdempKey:   idempKey,
		DefName:    def.Name,
		DefVersion: def.Version,
		Input:      input,
		Status:     ExecPending,
		Steps:      steps,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// GetExecution 返回执行的持久化快照。
func (rt *Runtime) GetExecution(ctx context.Context, id string) (Execution, error) {
	e, _, err := rt.store.GetExecution(ctx, id)
	if err != nil {
		return Execution{}, err
	}
	return *e, nil
}

// Events 返回某执行追加式事件轨迹（可解释依据）。
func (rt *Runtime) Events(ctx context.Context, id string) ([]Event, error) {
	return rt.store.ReadEvents(ctx, id)
}

// Cancel 请求取消执行：持久化取消意图并尽快驱动其进入终态。
func (rt *Runtime) Cancel(ctx context.Context, id string) error {
	for attempt := 0; ; attempt++ {
		e, rev, err := rt.store.GetExecution(ctx, id)
		if err != nil {
			return err
		}
		if e.Status.Terminal() {
			return &RejectError{
				Code:    "TerminalNoCancel",
				Message: fmt.Sprintf("execution %s already in terminal state %s", id, e.Status),
			}
		}
		if e.CancelRequested {
			break // 取消意图已持久化（并发重复取消幂等收敛）
		}
		e.CancelRequested = true
		e.UpdatedAt = time.Now().UTC()
		if _, err := rt.store.SaveExecution(ctx, e, rev); err == nil {
			break
		} else if !errors.Is(err, ErrConflict) || attempt >= 5 {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	log.Printf("[durable] cancel requested execution=%s", id)
	if d := rt.driver(id, false); d != nil {
		d.interrupt()
	}
	rt.kick()
	return nil
}

// Inflight 返回当前实际占用并发额度的步骤数（用于验证无悬挂额度）。
func (rt *Runtime) Inflight() int { return int(atomic.LoadInt64(&rt.slots)) }

// Close 停止调度循环并释放存储锁。
func (rt *Runtime) Close() error {
	if !rt.closed.CompareAndSwap(false, true) {
		return nil
	}
	rt.cancel()
	rt.driversMu.Lock()
	for _, d := range rt.drivers {
		d.interrupt()
	}
	rt.driversMu.Unlock()
	rt.wg.Wait()
	return rt.store.Close()
}

// ---------------- 恢复 ----------------

// recover 在启动时扫描状态目录：
//  1. 有占用但无执行文件 -> 按占用确定性补建 pending 执行；
//  2. 执行处于 running 的步骤 -> 重置为 pending（崩溃时副作用未确认，重试是安全的）；
//  3. 定义版本按恢复时规则校验，不兼容/未知 -> 明确进入 rejected 终态，不重放；
//  4. 记录 recovered 事件，随后统一重调度。
func (rt *Runtime) recover(ctx context.Context) error {
	claims, err := rt.store.ListClaims(ctx)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, c := range claims {
		known[c.ExecutionID] = true
		if _, _, gerr := rt.store.GetExecution(ctx, c.ExecutionID); errors.Is(gerr, ErrNotFound) {
			now := time.Now().UTC()
			e := &Execution{
				ID:         c.ExecutionID,
				IdempKey:   c.Key,
				DefName:    c.DefName,
				DefVersion: c.DefVersion,
				Input:      c.Input,
				Status:     ExecPending,
				CreatedAt:  c.CreatedAt,
				UpdatedAt:  now,
			}
			if _, serr := rt.store.SaveExecution(ctx, e, 0); serr != nil {
				return fmt.Errorf("durable: restore orphan claim %s: %w", c.Key, serr)
			}
			log.Printf("[durable] recovery: rebuilt orphan execution=%s idempKey=%q", c.ExecutionID, c.Key)
		}
	}

	ids, err := rt.store.ListExecutionIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		e, rev, gerr := rt.store.GetExecution(ctx, id)
		if gerr != nil {
			return gerr
		}
		known[id] = true
		if e.Status.Terminal() {
			continue
		}
		changed := false

		def, decision := rt.reg.CheckVersion(e.DefName, e.DefVersion)
		if decision != VerAcceptCompatible {
			if e.Status == ExecCompensating {
				// 补偿阶段钉住版本不可用：明确拒绝，原因与补偿执行失败可区分，
				// 绝不静默切换到新版本去补偿。
				now := time.Now().UTC()
				e.Status = ExecCompensationFailed
				e.Failure = &Failure{Class: FailurePermanent, Code: ReasonCompDefUnavailable,
					Message: "on recovery during compensation: " + string(decision)}
				e.UpdatedAt = now
				if _, serr := rt.store.SaveExecution(ctx, e, rev); serr != nil {
					return serr
				}
				_ = rt.appendEvent(e, Event{
					Type:            EvCompRejected,
					IdempKey:        e.IdempKey,
					VersionDecision: string(decision),
					Reason:          ReasonCompDefUnavailable,
					Detail:          "compensation must keep start-time semantics; never falls back to a newer definition",
				})
				log.Printf("[durable] recovery: COMPENSATION_REJECTED execution=%s idempKey=%q def=%s@%s versionDecision=%s reason=%s",
					id, e.IdempKey, e.DefName, e.DefVersion, decision, ReasonCompDefUnavailable)
				continue
			}
			code := rejectCodeFor(decision)
			now := time.Now().UTC()
			e.Status = ExecRejected
			e.Failure = &Failure{Class: FailurePermanent, Code: code, Message: "on recovery: " + string(decision)}
			e.UpdatedAt = now
			for i := range e.Steps {
				if e.Steps[i].Status != StepSucceeded {
					e.Steps[i].Status = StepCancelled
					e.Steps[i].FinishedAt = &now
				}
			}
			if _, serr := rt.store.SaveExecution(ctx, e, rev); serr != nil {
				return serr
			}
			_ = rt.appendEvent(e, Event{
				Type:            EvRejected,
				IdempKey:        e.IdempKey,
				DefName:         e.DefName,
				DefVersion:      e.DefVersion,
				VersionDecision: string(decision),
				Reason:          code,
				Detail:          "in-flight execution rejected on recovery; pinned semantics not available",
			})
			log.Printf("[durable] recovery: REJECT execution=%s idempKey=%q def=%s@%s versionDecision=%s reason=%s",
				id, e.IdempKey, e.DefName, e.DefVersion, decision, code)
			continue
		}
		_ = def

		resetSteps := []string{}
		for i := range e.Steps {
			if e.Steps[i].Status == StepRunning {
				e.Steps[i].Status = StepPending
				e.Steps[i].StartedAt = nil
				resetSteps = append(resetSteps, e.Steps[i].Name)
				changed = true
			}
		}
		// 崩溃时处于 running 的补偿动作同样副作用未确认：复位为 pending，
		// 恢复后从未补偿完的地方继续；已成功的补偿绝不重复执行。
		resetComps := []string{}
		if e.Comp != nil {
			for i := range e.Comp.Steps {
				if e.Comp.Steps[i].Status == CompRunning {
					e.Comp.Steps[i].Status = CompPending
					e.Comp.Steps[i].StartedAt = nil
					resetComps = append(resetComps, e.Comp.Steps[i].Step)
					changed = true
				}
			}
		}
		if e.Status == ExecRunning {
			e.Status = ExecPending
			changed = true
		}
		// 先持久化重置后的状态（状态是恢复正确性的唯一事实来源），再追加解释性事件。
		if changed {
			e.UpdatedAt = time.Now().UTC()
			if _, serr := rt.store.SaveExecution(ctx, e, rev); serr != nil {
				return serr
			}
		}
		for _, name := range resetSteps {
			_ = rt.appendEvent(e, Event{
				Type:   EvRunningReset,
				Step:   name,
				Reason: "process restarted while step running; reset to pending for safe retry",
			})
		}
		for _, name := range resetComps {
			_ = rt.appendEvent(e, Event{
				Type:   EvRunningReset,
				Step:   name,
				Reason: "process restarted while compensation running; reset to pending for safe resume",
			})
		}
		log.Printf("[durable] recovery: resumed execution=%s idempKey=%q status=%s resetSteps=%v resetComps=%v",
			id, e.IdempKey, e.Status, resetSteps, resetComps)
		_ = rt.appendEvent(e, Event{Type: EvRecovered, IdempKey: e.IdempKey})
	}
	return nil
}

func rejectCodeFor(d VersionDecision) string {
	switch d {
	case VerDefUnknown:
		return ReasonDefUnknown
	case VerVersionIncompatible:
		return ReasonDefVersionIncompat
	case VerVersionMissing:
		return ReasonDefVersionMissing
	default:
		return ReasonDefUnknown
	}
}

// ---------------- 调度（每执行单 goroutine actor） ----------------

func (rt *Runtime) dispatcher() {
	defer rt.wg.Done()
	// 恢复完成前不调度任何执行，杜绝恢复与驱动并发写状态。
	select {
	case <-rt.ctx.Done():
		return
	case <-rt.recovered:
	}
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rt.ctx.Done():
			return
		case <-t.C:
			rt.tick()
		case <-rt.wakeAll:
			rt.tick()
		}
	}
}

func (rt *Runtime) kick() {
	select {
	case rt.wakeAll <- struct{}{}:
	default:
	}
}

func (rt *Runtime) tick() {
	if rt.closed.Load() {
		return
	}
	ids, err := rt.store.ListExecutionIDs(rt.ctx)
	if err != nil {
		return
	}
	for _, id := range ids {
		if rt.ctx.Err() != nil {
			return
		}
		rt.driver(id, true).notify()
	}
}

func (rt *Runtime) driver(id string, create bool) *execDriver {
	rt.driversMu.Lock()
	defer rt.driversMu.Unlock()
	d, ok := rt.drivers[id]
	if !ok && create {
		d = rt.newExecDriver(id)
		rt.drivers[id] = d
	}
	return d
}

// acquireSlot 可中断地等待并发额度；等待期间不占额度，取消/关闭立即返回。
func (rt *Runtime) acquireSlot(ctx context.Context) error {
	select {
	case rt.sem <- struct{}{}:
		atomic.AddInt64(&rt.slots, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSlot 必须与每次成功的 acquireSlot 配对，确保终态/取消后不残留占用额度。
func (rt *Runtime) releaseSlot() {
	atomic.AddInt64(&rt.slots, -1)
	<-rt.sem
}

func (rt *Runtime) appendEvent(e *Execution, ev Event) error {
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	ev.IdempKey = e.IdempKey
	ev.DefName = e.DefName
	ev.DefVersion = e.DefVersion
	// 序号在存储层的事件互斥区内“计数+追加”原子分配，
	// 避免恢复期间多条事件并发读到相同行数而互相覆盖。
	return rt.store.AppendEvent(context.Background(), e.ID, ev)
}

func executionID(idempKey string) string {
	sum := sha256String(idempKey)
	return "exec-" + sum[:16]
}
