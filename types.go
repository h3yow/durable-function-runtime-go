package durable

import (
	"context"
	"encoding/json"
	"time"
)

// ExecStatus 是执行（一次函数编排实例）的顶层状态。
type ExecStatus string

const (
	// ExecPending 已创建（含幂等建档），尚未开始执行。
	ExecPending ExecStatus = "pending"
	// ExecRunning 至少有一个步骤正在推进。
	ExecRunning ExecStatus = "running"
	// ExecSucceeded 终态：全部步骤成功。
	ExecSucceeded ExecStatus = "succeeded"
	// ExecFailed 终态：永久失败或重试上限耗尽。
	ExecFailed ExecStatus = "failed"
	// ExecCancelled 终态：被外部取消。
	ExecCancelled ExecStatus = "cancelled"
	// ExecTimedOut 终态：某个步骤超时（超时不可重试）。
	ExecTimedOut ExecStatus = "timed_out"
	// ExecRejected 终态：定义未知/版本不兼容等，在启动或恢复时被明确拒绝。
	ExecRejected ExecStatus = "rejected"
	// ExecCompensating 正向流程已失败，正在按相反顺序执行已声明的补偿。
	ExecCompensating ExecStatus = "compensating"
	// ExecCompensationFailed 终态：补偿本身失败（重试上限/超时/永久失败），
	// 与正向流程失败（failed/cancelled/timed_out）可区分。
	ExecCompensationFailed ExecStatus = "compensation_failed"
)

// Terminal 报告状态是否为终态。终态执行不会再被调度。
func (s ExecStatus) Terminal() bool {
	switch s {
	case ExecSucceeded, ExecFailed, ExecCancelled, ExecTimedOut, ExecRejected, ExecCompensationFailed:
		return true
	}
	return false
}

// StepStatus 是单个步骤的状态。
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepRetrying  StepStatus = "retrying"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepTimedOut  StepStatus = "timed_out"
	StepCancelled StepStatus = "cancelled"
)

// FailureClass 对失败进行分类，决定是否进入重试链路。
type FailureClass string

const (
	// FailureTransient 瞬时失败：可按退避策略重试。
	FailureTransient FailureClass = "transient"
	// FailurePermanent 永久失败：必须立即进入可区分终态，不得重试。
	FailurePermanent FailureClass = "permanent"
)

// 失败原因码：终态之间、拒绝原因之间必须可区分。
const (
	ReasonStepTransient       = "StepTransient"          // 普通瞬时错误（重试中）
	ReasonStepPermanent       = "StepPermanent"          // 永久失败
	ReasonRetryLimitExceeded  = "RetryLimitExceeded"     // 重试上限耗尽
	ReasonStepTimeout         = "StepTimeout"            // 步骤超时
	ReasonExecutionCancelled  = "ExecutionCancelled"     // 被取消
	ReasonDefUnknown          = "DefUnknown"             // 定义名未知
	ReasonDefVersionIncompat  = "DefVersionIncompatible" // 主版本不兼容
	ReasonDefVersionMissing   = "DefVersionMissing"      // 主版本兼容但精确版本已不存在
	ReasonIdempotencyConflict = "IdempotencyConflict"    // 同幂等键不同请求负载

	// 补偿相关原因码：与正向流程失败可区分。
	ReasonCompPermanent      = "CompensationPermanent"          // 补偿动作永久失败
	ReasonCompRetryExhausted = "CompensationRetryLimitExceeded" // 补偿重试上限耗尽
	ReasonCompTimeout        = "CompensationTimeout"            // 补偿动作超时
	ReasonCompDefUnavailable = "CompDefVersionUnavailable"      // 补偿期间钉住的定义版本不可用
)

// Failure 记录一次失败的分类、原因码与信息。
type Failure struct {
	Class   FailureClass `json:"class"`
	Code    string       `json:"code"`
	Message string       `json:"message"`
	// Retriable 为 true 表示当前仍在退避重试链路中。
	Retriable bool `json:"retriable,omitempty"`
}

// EventType 是追加式事件日志中的事件类型。
type EventType string

const (
	EvCreated        EventType = "created"
	EvIdempotencyHit EventType = "idempotency_hit"
	EvRejected       EventType = "rejected"
	EvStepStarted    EventType = "step_started"
	EvStepSucceeded  EventType = "step_succeeded"
	EvStepRetrying   EventType = "step_retrying"
	EvStepFailed     EventType = "step_failed"
	EvStepTimedOut   EventType = "step_timed_out"
	EvCancelled      EventType = "cancelled"
	EvSucceeded      EventType = "succeeded"
	EvRecovered      EventType = "recovered"
	EvRunningReset   EventType = "running_step_reset"

	// 补偿（回滚）相关事件。
	EvCompStarted     EventType = "compensation_started"      // 进入补偿阶段（含触发原因）
	EvCompStepStarted EventType = "compensation_step_started" // 某步补偿开始（含第几次尝试）
	EvCompStepOK      EventType = "compensation_step_succeeded"
	EvCompStepSkipped EventType = "compensation_step_skipped" // 该步未声明补偿，跳过
	EvCompStepRetry   EventType = "compensation_step_retrying"
	EvCompStepFailed  EventType = "compensation_step_failed"
	EvCompStepTimeout EventType = "compensation_step_timed_out"
	EvCompensated     EventType = "compensated"           // 补偿全部完成，回到正向终态
	EvCompFailed      EventType = "compensation_failed"   // 补偿本身失败（独立终态）
	EvCompRejected    EventType = "compensation_rejected" // 补偿期间钉住版本不可用，明确拒绝
)

// Event 是可解释执行轨迹的一条记录。
type Event struct {
	Seq             int64           `json:"seq"`
	Time            time.Time       `json:"time"`
	Type            EventType       `json:"type"`
	IdempKey        string          `json:"idemp_key,omitempty"`
	DefName         string          `json:"def_name,omitempty"`
	DefVersion      string          `json:"def_version,omitempty"`
	VersionDecision string          `json:"version_decision,omitempty"`
	Step            string          `json:"step,omitempty"`
	Attempt         int             `json:"attempt,omitempty"`
	MaxAttempts     int             `json:"max_attempts,omitempty"`
	BackoffMS       int64           `json:"backoff_ms,omitempty"`
	NotBefore       *time.Time      `json:"not_before,omitempty"`
	Reason          string          `json:"reason,omitempty"`
	Failure         *Failure        `json:"failure,omitempty"`
	Detail          string          `json:"detail,omitempty"`
	Input           json.RawMessage `json:"input,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
}

// StepFunc 是一个编排步骤的可执行函数。
// 输入为首步的执行输入，或上一步的输出；ctx 取消时应尽快返回。
type StepFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

// Step 是编排定义中的一个步骤。
type Step struct {
	Name    string
	Timeout time.Duration // 0 表示不设步骤级超时
	Fn      StepFunc

	// Compensate 为该步声明的失败补偿动作；nil 表示不声明补偿（补偿阶段直接跳过）。
	// 输入为该步已持久化的输出（无输出时回退为该步输入）。
	Compensate StepFunc
	// CompTimeout 为补偿动作的步骤级超时；0 表示不设超时。
	// 即使补偿函数不理会取消信号，超时后也会尽快给出结论并释放并发额度。
	CompTimeout time.Duration
}

// Definition 是一个函数编排的不可变定义（按 名称+语义化版本 标识）。
type Definition struct {
	Name    string
	Version string // 语义化版本，如 "1.2.0"
	Steps   []Step
}

// RetryPolicy 描述瞬时失败的有上限退避策略。
type RetryPolicy struct {
	// MaxAttempts 总尝试次数（含首次），<=0 表示不重试。
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// StepState 是某个步骤在一次执行中的持久化进度。
type StepState struct {
	Name       string          `json:"name"`
	Status     StepStatus      `json:"status"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	Attempts   int             `json:"attempts"`
	NotBefore  *time.Time      `json:"not_before,omitempty"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	Failure    *Failure        `json:"failure,omitempty"`
}

// CompStatus 是单个步骤补偿动作的状态。
type CompStatus string

const (
	CompPending   CompStatus = "pending"   // 待补偿
	CompRunning   CompStatus = "running"   // 补偿动作执行中（崩溃恢复时会复位为 pending）
	CompRetrying  CompStatus = "retrying"  // 补偿退避等待中
	CompSucceeded CompStatus = "succeeded" // 补偿成功（绝不重复补偿）
	CompSkipped   CompStatus = "skipped"   // 该步未声明补偿，已跳过
	CompFailed    CompStatus = "failed"    // 补偿失败（永久/重试上限）
	CompTimedOut  CompStatus = "timed_out" // 补偿动作超时
)

// CompState 是某一步补偿动作的持久化进度。
type CompState struct {
	Step       string     `json:"step"`
	Status     CompStatus `json:"status"`
	Attempts   int        `json:"attempts"`
	NotBefore  *time.Time `json:"not_before,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Failure    *Failure   `json:"failure,omitempty"`
}

// CompensationState 是一次执行的补偿（回滚）进度。
// Steps 按正向步骤的相反顺序排列；Trigger 保留触发补偿的原始原因，绝不覆盖。
type CompensationState struct {
	// OriginStatus 为补偿全部完成后应进入的正向终态（failed/cancelled/timed_out）。
	OriginStatus ExecStatus `json:"origin_status"`
	// Trigger 为触发补偿的原始失败原因（哪一步失败/超时/取消）。
	Trigger Failure `json:"trigger"`
	// TriggerStep 为触发补偿的正向步骤名（取消时可能为空）。
	TriggerStep string      `json:"trigger_step,omitempty"`
	Steps       []CompState `json:"steps"`
}

// Execution 是一次编排实例的完整持久化状态。
type Execution struct {
	ID         string          `json:"id"`
	IdempKey   string          `json:"idemp_key,omitempty"`
	DefName    string          `json:"def_name"`
	DefVersion string          `json:"def_version"`
	Input      json.RawMessage `json:"input,omitempty"`
	Status     ExecStatus      `json:"status"`
	Steps      []StepState     `json:"steps"`
	Failure    *Failure        `json:"failure,omitempty"`
	// Comp 为补偿（回滚）进度；仅当执行进入补偿阶段后非空。
	Comp            *CompensationState `json:"comp,omitempty"`
	CancelRequested bool               `json:"cancel_requested,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
	Rev             int64              `json:"rev"`
}

// IdempotencyClaim 是幂等键的持久化占用记录。
type IdempotencyClaim struct {
	Key         string `json:"key"`
	ExecutionID string `json:"execution_id"`
	// RequestHash 为请求指纹（def+version+input），用于区分“同键不同请求”。
	RequestHash string `json:"request_hash"`
	DefName     string `json:"def_name"`
	DefVersion  string `json:"def_version"`
	// Input 为建档时的请求输入：若崩溃发生在“占用已建、执行尚未落盘”的窗口，
	// 恢复时据此确定性地补建 pending 执行（执行 ID 由幂等键派生）。
	Input     json.RawMessage `json:"input,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// Store 是执行状态、幂等占用与事件日志的持久化抽象。
// SaveExecution 使用期望版本号做 CAS，防止状态丢失更新。
type Store interface {
	SaveExecution(ctx context.Context, e *Execution, expectedRev int64) (int64, error)
	GetExecution(ctx context.Context, id string) (*Execution, int64, error)
	ListExecutionIDs(ctx context.Context) ([]string, error)

	// CreateClaim 必须是互斥建档：键已存在时返回 ErrClaimExists。
	CreateClaim(ctx context.Context, c IdempotencyClaim) error
	GetClaim(ctx context.Context, key string) (IdempotencyClaim, error)
	ListClaims(ctx context.Context) ([]IdempotencyClaim, error)

	AppendEvent(ctx context.Context, execID string, ev Event) error
	ReadEvents(ctx context.Context, execID string) ([]Event, error)

	Close() error
}

// Config 为 Runtime 的配置。
type Config struct {
	Dir           string // 本地状态目录
	MaxConcurrent int    // 同时执行步骤的并发额度，<=0 时取默认值
	Retry         RetryPolicy
	// CompRetry 为补偿动作独立的退避重试策略（与正向流程互不影响）；
	// 零值时使用 DefaultRetryPolicy。
	CompRetry RetryPolicy
}
