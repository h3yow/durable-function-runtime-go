package durable

import "errors"

// 持久层错误。
var (
	// ErrNotFound 执行或幂等占用不存在。
	ErrNotFound = errors.New("durable: not found")
	// ErrClaimExists 幂等键已被占用（建档竞争时用于收敛）。
	ErrClaimExists = errors.New("durable: idempotency claim exists")
	// ErrConflict CAS 版本不匹配，状态已被并发修改。
	ErrConflict = errors.New("durable: state revision conflict")
)

// RejectError 表示请求/恢复被明确拒绝。Code 为可区分的拒绝原因码。
type RejectError struct {
	Code    string
	Message string
}

func (e *RejectError) Error() string {
	if e == nil {
		return "durable: rejected"
	}
	return "durable: rejected: " + e.Code + ": " + e.Message
}

// AsRejectError 将错误断言为 *RejectError。
func AsRejectError(err error) (*RejectError, bool) {
	var r *RejectError
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// IdempotencyError 表示同幂等键重复提交的处理结果。
//
// Duplicate 为 true 表示是同一请求的重复投递，会收敛到既有执行
// （结果中携带 ExecutionID）；为 false 表示同键不同请求，被明确拒绝。
type IdempotencyError struct {
	Code        string
	Message     string
	ExecutionID string
	Duplicate   bool
}

func (e *IdempotencyError) Error() string {
	if e == nil {
		return "durable: idempotency"
	}
	return "durable: idempotency: " + e.Code + ": " + e.Message
}

// AsIdempotencyError 将错误断言为 *IdempotencyError。
func AsIdempotencyError(err error) (*IdempotencyError, bool) {
	var ie *IdempotencyError
	if errors.As(err, &ie) {
		return ie, true
	}
	return nil, false
}

// PermanentError 显式标记一个错误为永久失败（不可重试）。
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string {
	if e != nil && e.Err != nil {
		return e.Err.Error()
	}
	return "durable: permanent failure"
}

func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
