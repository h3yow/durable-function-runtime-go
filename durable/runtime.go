package durable

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Logger 是可注入的日志接口，测试用它打印幂等键、重试依据与版本判定。
type Logger interface {
	Printf(format string, args ...any)
}

// Options 配置运行时。
type Options struct {
	Workers      int           // 并发执行额度（信号量）
	PollInterval time.Duration // 恢复/重试扫描间隔
	Logger       Logger        // 为空则用 log.Default()
}

// Runtime 是编排执行引擎。
type Runtime struct {
	store Store
	reg   *Registry
	opts  Options
	log   Logger

	mu       sync.Mutex
	sem      chan struct{}
	inflight map[string]struct{}      // 正在被执行 goroutine 处理的执行 ID
	cancel   map[string]chan struct{} // executionID -> 取消信号
	wg       sync.WaitGroup
}

// NewRuntime 创建运行时。
func NewRuntime(store Store, reg *Registry, opts Options) *Runtime {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 50 * time.Millisecond
	}
	lg := opts.Logger
	if lg == nil {
		lg = log.Default()
	}
	return &Runtime{
		store:    store,
		reg:      reg,
		opts:     opts,
		log:      lg,
		sem:      make(chan struct{}, opts.Workers),
		inflight: map[string]struct{}{},
		cancel:   map[string]chan struct{}{},
	}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func hashInput(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}

// Submit 提交一次编排执行。
// 幂等键已存在且定义与输入一致时返回已存在执行（created=false）；
// 幂等键冲突、定义未知或版本不兼容时返回可区分的错误。
func (r *Runtime) Submit(ctx context.Context, defName, defVersion, idempotencyKey string, input []byte) (*Execution, bool, error) {
	if idempotencyKey == "" {
		return nil, false, errors.New("durable: idempotency key required")
	}
	def, err := r.reg.Lookup(defName, defVersion)
	if err != nil {
		// 版本判定：未知定义与不兼容版本在提交期即被拒绝。
		r.log.Printf("submit rejected idem=%s def=%s@%s reason=%v", idempotencyKey, defName, defVersion, err)
		return nil, false, err
	}
	if len(input) > 0 && !json.Valid(input) {
		return nil, false, errors.New("durable: input must be valid JSON")
	}
	exec := &Execution{
		ID:                newID(),
		IdempotencyKey:    idempotencyKey,
		DefinitionName:    def.Name,
		DefinitionVersion: def.Version,
		InputHash:         hashInput(input),
		Input:             append(json.RawMessage(nil), input...),
		Status:            StatusPending,
		Steps:             make([]StepRecord, len(def.Steps)),
	}
	for i, s := range def.Steps {
		exec.Steps[i] = StepRecord{Name: s.Name, Status: StatusPending, UpdatedAt: time.Now()}
	}
	stored, created, err := r.store.CreateIfAbsent(ctx, exec)
	if err != nil {
		return nil, false, err
	}
	if !created {
		// 幂等判定：键已存在时，仅当定义与输入完全一致才收敛到同一执行。
		if stored.DefinitionName != defName || stored.DefinitionVersion != defVersion || stored.InputHash != exec.InputHash {
			r.log.Printf("idempotency conflict idem=%s existing=%s@%s hash=%s requested=%s@%s hash=%s",
				idempotencyKey, stored.DefinitionName, stored.DefinitionVersion, stored.InputHash,
				defName, defVersion, exec.InputHash)
			return nil, false, fmt.Errorf("%w: key=%s", ErrIdempotencyConflict, idempotencyKey)
		}
		r.log.Printf("idempotency hit idem=%s converged to execution=%s status=%s", idempotencyKey, stored.ID, stored.Status)
		return stored, false, nil
	}
	r.log.Printf("submitted idem=%s execution=%s def=%s@%s", idempotencyKey, stored.ID, defName, defVersion)
	return stored, true, nil
}

// Get 读取执行状态。
func (r *Runtime) Get(ctx context.Context, id string) (*Execution, error) {
	return r.store.Get(ctx, id)
}

// Cancel 请求取消执行；已进入终态时返回 ErrExecutionTerminal。
func (r *Runtime) Cancel(ctx context.Context, id string) error {
	exec, err := r.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if exec.Status.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrExecutionTerminal, id, exec.Status)
	}
	exec.Status = StatusCanceled
	exec.FailureKind = FailureCanceled
	exec.FailureReason = "canceled by caller"
	for i := range exec.Steps {
		if !exec.Steps[i].Status.Terminal() {
			exec.Steps[i].Status = StatusCanceled
			exec.Steps[i].FailureKind = FailureCanceled
			exec.Steps[i].UpdatedAt = time.Now()
		}
	}
	if err := r.store.CompareAndSwap(ctx, exec); err != nil {
		return err
	}
	r.log.Printf("canceled execution=%s idem=%s", exec.ID, exec.IdempotencyKey)
	r.mu.Lock()
	if ch, ok := r.cancel[id]; ok {
		close(ch)
		delete(r.cancel, id)
	}
	r.mu.Unlock()
	return nil
}

// Run 启动调度循环：恢复未完成执行并持续调度，直到 ctx 取消。
// 进程重启后再次调用 Run 即可从未完成的步骤继续。
func (r *Runtime) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.opts.PollInterval)
	defer ticker.Stop()
	for {
		r.scheduleOnce(ctx)
		select {
		case <-ctx.Done():
			r.wg.Wait()
			return nil
		case <-ticker.C:
		}
	}
}

// scheduleOnce 扫描未完成执行，为可调度者占用并发额度并启动执行 goroutine。
func (r *Runtime) scheduleOnce(ctx context.Context) {
	execs, err := r.store.ListIncomplete(ctx)
	if err != nil {
		r.log.Printf("list incomplete failed: %v", err)
		return
	}
	now := time.Now()
	for _, exec := range execs {
		if exec.NextRetryAt.After(now) {
			continue // 退避中，未到重试时间
		}
		r.mu.Lock()
		if _, ok := r.inflight[exec.ID]; ok {
			r.mu.Unlock()
			continue // 已在执行，避免重复调度
		}
		select {
		case r.sem <- struct{}{}:
			r.inflight[exec.ID] = struct{}{}
			ch := make(chan struct{})
			r.cancel[exec.ID] = ch
			r.wg.Add(1)
			go func(id string, cancelCh chan struct{}) {
				defer r.wg.Done()
				defer func() { <-r.sem }() // 终态或让出时释放并发额度
				defer func() {
					r.mu.Lock()
					delete(r.inflight, id)
					delete(r.cancel, id)
					r.mu.Unlock()
				}()
				r.drive(ctx, id, cancelCh)
			}(exec.ID, ch)
			r.mu.Unlock()
		default:
			r.mu.Unlock()
			return // 并发额度占满，下轮再调度
		}
	}
}

// drive 推进单个执行直到终态、退避或让出。
func (r *Runtime) drive(ctx context.Context, id string, cancelCh chan struct{}) {
	for {
		exec, err := r.store.Get(ctx, id)
		if err != nil {
			r.log.Printf("drive load failed execution=%s err=%v", id, err)
			return
		}
		if exec.Status.Terminal() {
			return
		}
		select {
		case <-cancelCh:
			return // 已被取消（Cancel 已写终态）
		default:
		}
		// 退避等待：到期前让出并发额度，下轮调度再续。
		if wait := time.Until(exec.NextRetryAt); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-cancelCh:
				timer.Stop()
				return
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		// 版本判定：按执行启动时锁定的版本查找定义。
		def, err := r.reg.Lookup(exec.DefinitionName, exec.DefinitionVersion)
		if err != nil {
			kind := FailureDefinitionUnknown
			if errors.Is(err, ErrVersionIncompatible) {
				kind = FailureVersionIncompat
			}
			r.log.Printf("version check failed execution=%s idem=%s def=%s@%s kind=%s",
				exec.ID, exec.IdempotencyKey, exec.DefinitionName, exec.DefinitionVersion, kind)
			r.failExecution(ctx, exec, kind, err.Error())
			return
		}
		if exec.Status == StatusPending {
			exec.Status = StatusRunning
			if err := r.store.CompareAndSwap(ctx, exec); err != nil {
				continue // CAS 冲突：重载后再判定
			}
		}
		r.runSteps(ctx, exec, def, cancelCh)
		return
	}
}

// runSteps 从 NextStep 起顺序执行步骤；遇退避、终态或取消即让出。
func (r *Runtime) runSteps(ctx context.Context, exec *Execution, def *Definition, cancelCh chan struct{}) {
	for exec.NextStep < len(def.Steps) {
		select {
		case <-cancelCh:
			return
		default:
		}
		stepDef := def.Steps[exec.NextStep]
		rec := &exec.Steps[exec.NextStep]
		// 步骤输入接线：首步取执行输入，后续步取上一步已持久化的输出。
		if rec.Input == nil {
			if exec.NextStep == 0 {
				rec.Input = exec.Input
			} else {
				rec.Input = exec.Steps[exec.NextStep-1].Output
			}
		}
		rec.Status = StatusRunning
		rec.Attempts++
		rec.UpdatedAt = time.Now()
		if err := r.store.CompareAndSwap(ctx, exec); err != nil {
			return // CAS 冲突：让出，下轮重载
		}
		output, err := r.invoke(ctx, stepDef, exec, rec, cancelCh)
		if err == nil {
			rec.Status = StatusSucceeded
			rec.Output = output
			rec.UpdatedAt = time.Now()
			exec.NextStep++
			r.log.Printf("step succeeded execution=%s idem=%s step=%s attempt=%d",
				exec.ID, exec.IdempotencyKey, rec.Name, rec.Attempts)
			if err := r.store.CompareAndSwap(ctx, exec); err != nil {
				return
			}
			continue
		}
		r.handleStepError(ctx, exec, stepDef, rec, err)
		return
	}
	// 全部步骤完成：执行成功，末步骤输出即执行输出。
	if n := len(exec.Steps); n > 0 {
		exec.Output = exec.Steps[n-1].Output
	}
	exec.Status = StatusSucceeded
	exec.UpdatedAt = time.Now()
	if err := r.store.CompareAndSwap(ctx, exec); err != nil {
		return
	}
	r.log.Printf("execution succeeded execution=%s idem=%s", exec.ID, exec.IdempotencyKey)
}

// invoke 调用步骤函数，应用单次尝试超时与取消信号。
func (r *Runtime) invoke(ctx context.Context, stepDef StepDef, exec *Execution, rec *StepRecord, cancelCh chan struct{}) (json.RawMessage, error) {
	sctx := StepContext{
		ExecutionID: exec.ID,
		StepName:    stepDef.Name,
		Attempt:     rec.Attempts,
	}
	if stepDef.Timeout > 0 {
		sctx.Deadline = time.Now().Add(stepDef.Timeout)
	}
	type result struct {
		out json.RawMessage
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := stepDef.Func(sctx, rec.Input)
		done <- result{out, err}
	}()
	var timer *time.Timer
	var timeout <-chan time.Time
	if stepDef.Timeout > 0 {
		timer = time.NewTimer(stepDef.Timeout)
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case res := <-done:
		return res.out, res.err
	case <-timeout:
		return nil, &timeoutError{step: stepDef.Name}
	case <-cancelCh:
		return nil, errCanceled
	case <-ctx.Done():
		return nil, errShutdown
	}
}

var (
	errCanceled = errors.New("durable: step canceled")
	// errShutdown 表示运行时停机导致本次尝试中断；状态保持非终态，待重启恢复。
	errShutdown = errors.New("durable: runtime shutting down")
)

type timeoutError struct{ step string }

func (e *timeoutError) Error() string { return "step timed out: " + e.step }

// handleStepError 按失败分类决定重试或进入终态；处理后一律让出并发额度。
func (r *Runtime) handleStepError(ctx context.Context, exec *Execution, stepDef StepDef, rec *StepRecord, err error) {
	now := time.Now()
	rec.LastError = err.Error()
	rec.UpdatedAt = now
	switch {
	case errors.Is(err, errCanceled):
		return // 取消已由 Cancel 写终态
	case errors.Is(err, errShutdown):
		return // 停机：保持非终态，重启后从当前步骤恢复
	case isTimeout(err):
		rec.Status = StatusTimedOut
		rec.FailureKind = FailureTimeout
		r.log.Printf("step timeout execution=%s idem=%s step=%s attempt=%d",
			exec.ID, exec.IdempotencyKey, rec.Name, rec.Attempts)
		r.failExecution(ctx, exec, FailureTimeout, err.Error())
		return
	}
	kind := classifyError(err)
	rec.FailureKind = kind
	if kind == FailurePermanent {
		rec.Status = StatusFailed
		r.log.Printf("step permanent failure execution=%s idem=%s step=%s err=%q (no retry)",
			exec.ID, exec.IdempotencyKey, rec.Name, rec.LastError)
		r.failExecution(ctx, exec, FailurePermanent, err.Error())
		return
	}
	// 瞬时失败：按有上限的退避策略重试。
	max := maxAttempts(stepDef.Retry)
	if rec.Attempts >= max {
		rec.Status = StatusFailed
		rec.FailureKind = FailureRetryExhausted
		r.log.Printf("retry exhausted execution=%s idem=%s step=%s attempts=%d/%d",
			exec.ID, exec.IdempotencyKey, rec.Name, rec.Attempts, max)
		r.failExecution(ctx, exec, FailureRetryExhausted,
			fmt.Sprintf("step %s failed after %d attempts: %s", rec.Name, rec.Attempts, rec.LastError))
		return
	}
	delay := backoff(stepDef.Retry, rec.Attempts)
	rec.Status = StatusPending
	exec.NextRetryAt = now.Add(delay)
	r.log.Printf("transient failure, will retry execution=%s idem=%s step=%s attempt=%d/%d backoff=%s err=%q",
		exec.ID, exec.IdempotencyKey, rec.Name, rec.Attempts, max, delay, rec.LastError)
	// 让出并发额度，退避到期后重新调度。
	_ = r.store.CompareAndSwap(ctx, exec)
}

func isTimeout(err error) bool {
	var te *timeoutError
	return errors.As(err, &te)
}

// failExecution 将执行写入可区分的失败终态。
func (r *Runtime) failExecution(ctx context.Context, exec *Execution, kind FailureKind, reason string) {
	exec.Status = StatusFailed
	if kind == FailureTimeout {
		exec.Status = StatusTimedOut
	}
	exec.FailureKind = kind
	exec.FailureReason = reason
	exec.UpdatedAt = time.Now()
	if err := r.store.CompareAndSwap(ctx, exec); err != nil {
		r.log.Printf("fail execution persist failed execution=%s err=%v", exec.ID, err)
		return
	}
	r.log.Printf("execution terminal execution=%s idem=%s status=%s kind=%s reason=%q",
		exec.ID, exec.IdempotencyKey, exec.Status, kind, reason)
}
