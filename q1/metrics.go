package q1

import (
	"sort"
	"strings"
	"sync"
)

type Labels map[string]string

type Metrics interface {
	IncCounter(name string, labels Labels)
	Observe(name string, value float64, labels Labels)
	SetGauge(name string, value float64, labels Labels)
}

type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, Labels)        {}
func (NoopMetrics) Observe(string, float64, Labels)  {}
func (NoopMetrics) SetGauge(string, float64, Labels) {}

type MemoryMetrics struct {
	mu         sync.Mutex
	counters   map[string]float64
	gauges     map[string]float64
	histograms map[string][]float64
}

func NewMemoryMetrics() *MemoryMetrics {
	return &MemoryMetrics{
		counters:   make(map[string]float64),
		gauges:     make(map[string]float64),
		histograms: make(map[string][]float64),
	}
}

func (m *MemoryMetrics) IncCounter(name string, labels Labels) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[metricKey(name, labels)]++
}

func (m *MemoryMetrics) Observe(name string, value float64, labels Labels) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := metricKey(name, labels)
	m.histograms[key] = append(m.histograms[key], value)
}

func (m *MemoryMetrics) SetGauge(name string, value float64, labels Labels) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[metricKey(name, labels)] = value
}

func (m *MemoryMetrics) Counter(name string, labels Labels) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[metricKey(name, labels)]
}

func (m *MemoryMetrics) Gauge(name string, labels Labels) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[metricKey(name, labels)]
}

func (m *MemoryMetrics) Histogram(name string, labels Labels) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := m.histograms[metricKey(name, labels)]
	return append([]float64(nil), values...)
}

func (m *MemoryMetrics) Snapshot() map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]float64, len(m.counters)+len(m.gauges))
	for k, v := range m.counters {
		out[k] = v
	}
	for k, v := range m.gauges {
		out[k] = v
	}
	return out
}

func metricKey(name string, labels Labels) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte('|')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}
