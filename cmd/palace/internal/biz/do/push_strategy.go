package do

import "sync"

type PushStrategy interface {
	GetStrategyMetric() []StrategyMetric
}

type PushStrategyInfo struct {
	StrategyMetric []StrategyMetric
	mutex          sync.Mutex
}

func (p *PushStrategyInfo) GetStrategyMetric() []StrategyMetric {
	if p == nil {
		return nil
	}
	return p.StrategyMetric
}

func (p *PushStrategyInfo) SetStrategyMetric(metrics []StrategyMetric) {
	if p == nil {
		return
	}
	p.StrategyMetric = metrics
}

func (p *PushStrategyInfo) GetCount() int {
	if p == nil {
		return 0
	}
	return len(p.StrategyMetric)
}

func (p *PushStrategyInfo) AddMetric(metric StrategyMetric) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.StrategyMetric == nil {
		p.StrategyMetric = make([]StrategyMetric, 0)
	}
	p.StrategyMetric = append(p.StrategyMetric, metric)
}
