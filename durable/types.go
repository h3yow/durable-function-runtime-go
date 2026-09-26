// Package durable 提供可在单机运行的持久化函数编排组件。
package durable

import (
	"encoding/json"
	"time"
)

// Status 表示执行或步骤的状态机状态。
type Status string

const (
	StatusPending   Status = "Pending"
	StatusRunning   Status = "Running"
	StatusSucceeded Status = "Succeeded"
	StatusFailed    Status = "Failed"
	StatusCanceled  Status = "Canceled"
	StatusTimedOut  Status = "TimedOut"
)

// Terminal 报告状态是否为终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusTimedOut:
		return true
	}
	return false
}

// FailureKind 对失败原因做可区分的分类。
type FailureKind string

const (
	FailureNone              FailureKind = ""
	FailureTransient         FailureKind = "Transient"           // 瞬时失败，可重试
	FailurePermanent         FailureKind = "Permanent"           // 永久失败，不可重试
	FailureTimeout           FailureKind = "Timeout"             // 步骤或执行超时
	FailureCanceled          FailureKind = "Canceled"            // 被取消
	FailureRetryExhausted    FailureKind = "RetryExhausted"      // 达到重试上限
	FailureDefinitionUnknown FailureKind = "DefinitionUnknown"   // 定义不存在
	FailureVersionIncompat   FailureKind = "VersionIncompatible" // 定义版本不再兼容
)

// RetryPolicy 描述有上限的退避重试策略。
type RetryPolicy struct {
	MaxAttempts int           // 最大尝试次数（含首次），<=0 表示 1 次
	BaseDelay   time.Duration // 退避基数
	MaxDelay    time.Duration // 退避上限
}

// StepFunc 是步骤函数：输入 JSON，输出 JSON 或错误。
// 返回 *PermanentError 表示永久失败；其他错误按瞬时失败处理。
type StepFunc func(ctx StepContext, input json.RawMessage) (json.RawMessage, error)

// StepContext 携带步骤执行上下文。
type StepContext struct {
	ExecutionID string
	StepName    string
	Attempt     int
	Deadline    time.Time
}

// StepDef 定义一个步骤。
type StepDef struct {
	Name    string
	Func    StepFunc
	Timeout time.Duration // 单次尝试超时，<=0 表示不限制
	Retry   RetryPolicy
}

// Definition 是函数编排定义的一个不可变版本。
type Definition struct {
	Name    string
	Version string
	Steps   []StepDef
}

// StepRecord 持久化单个步骤的执行状态。
type StepRecord struct {
	Name        string          `json:"name"`
	Status      Status          `json:"status"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Attempts    int             `json:"attempts"`
	LastError   string          `json:"lastError,omitempty"`
	FailureKind FailureKind     `json:"failureKind,omitempty"`
	UpdatedAt   time.Time       `json:"updatedAt"`
}

// Execution 是一次编排执行的持久化状态。
type Execution struct {
	ID                string          `json:"id"`
	IdempotencyKey    string          `json:"idempotencyKey"`
	DefinitionName    string          `json:"definitionName"`
	DefinitionVersion string          `json:"definitionVersion"`
	InputHash         string          `json:"inputHash"`
	Input             json.RawMessage `json:"input,omitempty"`
	Output            json.RawMessage `json:"output,omitempty"`
	Status            Status          `json:"status"`
	Steps             []StepRecord    `json:"steps"`
	NextStep          int             `json:"nextStep"`
	FailureKind       FailureKind     `json:"failureKind,omitempty"`
	FailureReason     string          `json:"failureReason,omitempty"`
	// NextRetryAt 是瞬时失败后的下次重试时间；零值表示可立即调度。
	NextRetryAt time.Time `json:"nextRetryAt,omitempty"`
	// Version 用于乐观并发控制，每次写回自增。
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
