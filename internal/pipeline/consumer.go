package pipeline

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// Handler 处理消息的业务函数；返回 *Failure 以区分可重试与不可重试失败，
// 返回普通 error 一律按可重试处理。
type Handler func(ctx context.Context, m *Message) error

// Consumer 按分区并发消费：分区内严格按写入顺序处理，跨分区并行推进；
// 位点只在消息确认（成功 / 进入死信）后提交，未确认消息不会被位点越过。
type Consumer struct {
	broker  *Broker
	state   *StateStore
	handler Handler
	policy  BackoffPolicy
	logger  *log.Logger

	// PollInterval 是分区日志无新消息时的轮询间隔。
	PollInterval time.Duration

	wg sync.WaitGroup
}

// NewConsumer 创建消费者。logger 为 nil 时使用 log.Default()。
func NewConsumer(b *Broker, s *StateStore, h Handler, p BackoffPolicy, logger *log.Logger) *Consumer {
	if logger == nil {
		logger = log.Default()
	}
	return &Consumer{
		broker:       b,
		state:        s,
		handler:      h,
		policy:       p,
		logger:       logger,
		PollInterval: 20 * time.Millisecond,
	}
}

// Start 为每个分区启动消费循环，ctx 取消后各循环退出。
func (c *Consumer) Start(ctx context.Context) {
	for p := 0; p < c.broker.NumPartitions(); p++ {
		c.wg.Add(1)
		go func(partition int) {
			defer c.wg.Done()
			c.consumePartition(ctx, partition)
		}(p)
	}
}

// Wait 等待全部分区循环退出。
func (c *Consumer) Wait() { c.wg.Wait() }

// consumePartition 是单分区的顺序消费循环。
func (c *Consumer) consumePartition(ctx context.Context, partition int) {
	for {
		if ctx.Err() != nil {
			return
		}
		offset := c.state.CommittedOffset(partition)
		m, err := c.broker.Read(partition, offset)
		if errors.Is(err, ErrOffsetBeyondEnd) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.PollInterval):
				continue
			}
		}
		if err != nil {
			c.logger.Printf("[consumer] partition=%d offset=%d read error: %v", partition, offset, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.PollInterval):
				continue
			}
		}
		c.process(ctx, m)
	}
}

// process 处理单条消息：幂等去重 -> 调用 handler -> 按结果确认/重试/死信。
// 只有确认后（成功或进入死信）才提交位点，保证未确认消息不被越过。
func (c *Consumer) process(ctx context.Context, m *Message) {
	if m.CompatNote != "" {
		if err := c.state.NoteCompat(m.ID, m.CompatNote); err != nil {
			c.logger.Printf("[consumer] partition=%d offset=%d id=%s compat note persist error: %v",
				m.Partition, m.Offset, m.ID, err)
		}
		c.logger.Printf("[consumer] partition=%d offset=%d id=%s compat: %s",
			m.Partition, m.Offset, m.ID, m.CompatNote)
	}

	// 版本不受支持：不可重试，直接转入死信。
	if !SupportedVersions[m.Version] {
		c.logger.Printf("[consumer] partition=%d offset=%d id=%s unsupported version=%d -> dead letter",
			m.Partition, m.Offset, m.ID, m.Version)
		c.dead(m, "unsupported_version", "unsupported message version")
		c.commit(m)
		return
	}

	// 幂等去重：已产生业务效果的消息直接确认，重复投递不重复生效。
	if c.state.IsApplied(m.ID) {
		c.logger.Printf("[consumer] partition=%d offset=%d id=%s duplicate delivery, skipping effect",
			m.Partition, m.Offset, m.ID)
		c.commit(m)
		return
	}

	for {
		err := c.handler(ctx, m)
		if err == nil {
			if err := c.state.MarkApplied(m.ID); err != nil {
				c.logger.Printf("[consumer] partition=%d offset=%d id=%s mark applied error: %v",
					m.Partition, m.Offset, m.ID, err)
			}
			c.logger.Printf("[consumer] partition=%d offset=%d id=%s applied",
				m.Partition, m.Offset, m.ID)
			c.commit(m)
			return
		}

		f := AsFailure(err)
		if !f.Retryable {
			c.logger.Printf("[consumer] partition=%d offset=%d id=%s non-retryable failure: %s -> dead letter",
				m.Partition, m.Offset, m.ID, f.Reason)
			c.dead(m, "non_retryable", f.Reason)
			c.commit(m)
			return
		}

		attempt, aerr := c.state.IncrAttempt(m.ID)
		if aerr != nil {
			c.logger.Printf("[consumer] partition=%d offset=%d id=%s attempt persist error: %v",
				m.Partition, m.Offset, m.ID, aerr)
		}
		if attempt > c.policy.MaxRetries {
			c.logger.Printf("[consumer] partition=%d offset=%d id=%s retries exhausted (%d/%d): %s -> dead letter",
				m.Partition, m.Offset, m.ID, attempt-1, c.policy.MaxRetries, f.Reason)
			c.dead(m, "max_retries_exceeded", f.Reason)
			c.commit(m)
			return
		}

		delay := c.policy.Delay(attempt)
		c.logger.Printf("[consumer] partition=%d offset=%d id=%s retryable failure: %s, retry %d/%d after %s",
			m.Partition, m.Offset, m.ID, f.Reason, attempt, c.policy.MaxRetries, delay)
		// 退避等待只阻塞本分区循环，其他分区不受影响。
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// commit 将分区位点推进到 m.Offset+1。
func (c *Consumer) commit(m *Message) {
	if err := c.state.Commit(m.Partition, m.Offset+1); err != nil {
		c.logger.Printf("[consumer] partition=%d offset=%d id=%s commit error: %v",
			m.Partition, m.Offset, m.ID, err)
	}
}

// dead 将消息转入死信并保留原因。
func (c *Consumer) dead(m *Message, reason, detail string) {
	d := DeadLetter{Message: *m, Reason: reason, Detail: detail, At: time.Now().Unix()}
	if err := c.state.AddDeadLetter(d); err != nil {
		c.logger.Printf("[consumer] partition=%d offset=%d id=%s dead letter persist error: %v",
			m.Partition, m.Offset, m.ID, err)
	}
}
