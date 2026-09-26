package durable

import (
	"time"
)

// backoff 计算第 attempt 次失败后的退避时长（attempt 从 1 开始）。
// 策略：BaseDelay * 2^(attempt-1)，封顶 MaxDelay；不引入抖动以保证测试可复现。
func backoff(p RetryPolicy, attempt int) time.Duration {
	base := p.BaseDelay
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	max := p.MaxDelay
	if max <= 0 {
		max = 5 * time.Second
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

// maxAttempts 归一化重试上限。
func maxAttempts(p RetryPolicy) int {
	if p.MaxAttempts <= 0 {
		return 1
	}
	return p.MaxAttempts
}

// classifyError 将步骤错误分类为失败类别：
// *PermanentError -> Permanent；其余非 nil 错误 -> Transient。
func classifyError(err error) FailureKind {
	if err == nil {
		return FailureNone
	}
	if IsPermanent(err) {
		return FailurePermanent
	}
	return FailureTransient
}
