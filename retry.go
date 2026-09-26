package durable

import (
	"errors"
	"time"
)

// DefaultRetryPolicy 为缺省的有上限退避策略：最多 4 次尝试（3 次重试）。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    4,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
	}
}

// classify 判定一个步骤错误属于瞬时失败还是永久失败。
// 未显式标记为永久的错误一律按瞬时失败处理（默认倾向于重试）。
func classify(err error) (FailureClass, string) {
	if err == nil {
		return FailureTransient, ReasonStepTransient
	}
	var pe *PermanentError
	if errors.As(err, &pe) {
		return FailurePermanent, ReasonStepPermanent
	}
	return FailureTransient, ReasonStepTransient
}

// nextBackoff 根据已完成的尝试次数（attempts 从 1 起）计算下一次退避时长。
// attempts 为已发生的尝试次数；例如 attempts==1 表示首次失败后的首次重试。
// 退避 = min(initial * multiplier^(attempts-1), maxBackoff)。
func nextBackoff(p RetryPolicy, attempts int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	mult := p.Multiplier
	if mult <= 1 {
		mult = 2
	}
	d := float64(p.InitialBackoff)
	for i := 1; i < attempts; i++ {
		d *= mult
		if p.MaxBackoff > 0 && d >= float64(p.MaxBackoff) {
			d = float64(p.MaxBackoff)
			break
		}
	}
	if p.MaxBackoff > 0 && d > float64(p.MaxBackoff) {
		d = float64(p.MaxBackoff)
	}
	return time.Duration(d)
}
