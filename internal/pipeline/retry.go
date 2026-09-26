package pipeline

import "time"

// BackoffPolicy 描述可重试失败的重投退避策略。
type BackoffPolicy struct {
	Initial    time.Duration // 首次重试前的等待
	Max        time.Duration // 单次等待上限
	Multiplier float64       // 每次重试的放大倍数
	MaxRetries int           // 可重试失败的最大重投次数，超出后进入死信
}

// Delay 返回第 attempt 次重试（从 1 开始）前应等待的时长，
// 按 Initial * Multiplier^(attempt-1) 指数增长并以 Max 封顶。
func (p BackoffPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Initial
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Multiplier)
		if d >= p.Max {
			return p.Max
		}
	}
	if d > p.Max {
		return p.Max
	}
	return d
}

// Failure 是 handler 返回的、可区分是否可重试的错误。
type Failure struct {
	Reason    string
	Retryable bool
}

func (f *Failure) Error() string { return f.Reason }

// AsFailure 将 err 归一化为 *Failure：普通 error 视为可重试失败。
func AsFailure(err error) *Failure {
	if err == nil {
		return nil
	}
	if f, ok := err.(*Failure); ok {
		return f
	}
	return &Failure{Reason: err.Error(), Retryable: true}
}
