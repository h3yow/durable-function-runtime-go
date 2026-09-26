package durable

import "errors"

// PermanentError 包装一个错误，标记其为永久失败，不应重试。
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent 将 err 标记为永久失败。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent 判断错误是否为永久失败。
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// 提交阶段的可区分拒绝原因。
var (
	// ErrIdempotencyConflict 表示幂等键已存在但请求内容不一致。
	ErrIdempotencyConflict = errors.New("durable: idempotency key conflict")
	// ErrDefinitionUnknown 表示编排定义不存在。
	ErrDefinitionUnknown = errors.New("durable: definition unknown")
	// ErrVersionIncompatible 表示定义版本不再兼容。
	ErrVersionIncompatible = errors.New("durable: definition version incompatible")
	// ErrExecutionNotFound 表示执行不存在。
	ErrExecutionNotFound = errors.New("durable: execution not found")
	// ErrExecutionTerminal 表示执行已处于终态，无法接受变更。
	ErrExecutionTerminal = errors.New("durable: execution already terminal")
)

// ErrConcurrencyConflict 表示乐观并发写回时版本不匹配。
var ErrConcurrencyConflict = errors.New("durable: concurrency conflict")
