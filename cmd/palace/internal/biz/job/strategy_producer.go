package job

import (
	"github.com/aide-family/moon/cmd/palace/internal/biz/repository"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"

	"github.com/aide-family/moon/cmd/palace/internal/biz/do"
	"github.com/aide-family/moon/pkg/plugin/server/cron_server"
)

// 双触发条件设计
// 数量触发:当累积数据量达到阈值时立即发送。
// 时间触发:当累积时间达到阈值时立即发送。

var _ cron_server.CronJob = (*StrategyProducer)(nil)

type StrategyProducer struct {
	data          *do.PushStrategyInfo
	maxSize       int
	flushInterval time.Duration
	mutex         sync.Mutex
	stopCh        chan struct{}
	sendFunc      func(*do.PushStrategyInfo)
	index         string
	id            cron.EntryID
	spec          cron_server.CronSpec
	helper        *log.Helper
	strategy      repository.TeamStrategy
}

func (b *StrategyProducer) Index() string {
	return b.index
}

func (b *StrategyProducer) Spec() cron_server.CronSpec {
	return b.spec
}

func (b *StrategyProducer) WithID(id cron.EntryID) cron_server.CronJob {
	b.id = id
	return b
}

func (b *StrategyProducer) IsImmediate() bool {
	return false
}

func NewStrategyProducer(maxSize int, sendFunc func(*do.PushStrategyInfo)) *StrategyProducer {
	b := &StrategyProducer{
		data:     &do.PushStrategyInfo{},
		maxSize:  maxSize,
		stopCh:   make(chan struct{}),
		sendFunc: sendFunc,
	}
	go b.backgroundFlush()
	return b
}

func (b *StrategyProducer) Run() {

	// 1.获取team列表

	// 2.team中获取策略列表

}

func (b *StrategyProducer) ID() cron.EntryID {
	return b.id
}

func (b *StrategyProducer) AddStrategy(strategy *do.PushStrategyInfo) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.data = strategy
	if b.shouldFlush() {
		b.flush()
	}
}

// shouldFlush 检查触发条件
func (b *StrategyProducer) shouldFlush() bool {
	total := b.data.GetCount()
	return total >= b.maxSize
}

// backgroundFlush 定时刷新

func (b *StrategyProducer) backgroundFlush() {
	b.mutex.Lock()
	if len(b.data.GetStrategyMetric()) > 0 {
		b.flush()
	}
	b.mutex.Unlock()
}

func (b *StrategyProducer) flush() {
	dataToSend := &do.PushStrategyInfo{
		StrategyMetric: make([]do.StrategyMetric, len(b.data.GetStrategyMetric())),
	}
	copy(dataToSend.StrategyMetric, b.data.StrategyMetric)

	b.data.SetStrategyMetric(nil)

	go b.sendFunc(dataToSend)
}

func (b *StrategyProducer) Stop() {
	close(b.stopCh)
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.data.StrategyMetric) > 0 {
		b.flush()
	}
}
