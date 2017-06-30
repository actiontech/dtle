package agent

import (
	"strings"
	"sync"
	"time"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

type PrometheusSink struct {
	mu        sync.Mutex
	gauges    map[string]prometheus.Gauge
	summaries map[string]prometheus.Summary
	counters  map[string]prometheus.Counter
}

func NewPrometheusSink(metricsAddr string,metricsInterval time.Duration) (*PrometheusSink, error) {
	s := &PrometheusSink{
		gauges:    make(map[string]prometheus.Gauge),
		summaries: make(map[string]prometheus.Summary),
		counters:  make(map[string]prometheus.Counter),
	}
	s.pushMetric(metricsAddr, metricsInterval)
	return s, nil
}

func (p *PrometheusSink) flattenKey(parts []string) string {
	joined := strings.Join(parts, "_")
	joined = strings.Replace(joined, " ", "_", -1)
	joined = strings.Replace(joined, ".", "_", -1)
	joined = strings.Replace(joined, "-", "_", -1)
	joined = strings.Replace(joined, "=", "_", -1)
	return joined
}

func (p *PrometheusSink) SetGauge(parts []string, val float32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := p.flattenKey(parts)
	g, ok := p.gauges[key]
	if !ok {
		g = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: key,
			Help: key,
		})
		prometheus.MustRegister(g)
		p.gauges[key] = g
	}
	g.Set(float64(val))
}

func (p *PrometheusSink) AddSample(parts []string, val float32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := p.flattenKey(parts)
	g, ok := p.summaries[key]
	if !ok {
		g = prometheus.NewSummary(prometheus.SummaryOpts{
			Name:   key,
			Help:   key,
			MaxAge: 10 * time.Second,
		})
		prometheus.MustRegister(g)
		p.summaries[key] = g
	}
	g.Observe(float64(val))
}

// EmitKey is not implemented. Prometheus doesn’t offer a type for which an
// arbitrary number of values is retained, as Prometheus works with a pull
// model, rather than a push model.
func (p *PrometheusSink) EmitKey(key []string, val float32) {
}

func (p *PrometheusSink) IncrCounter(parts []string, val float32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := p.flattenKey(parts)
	g, ok := p.counters[key]
	if !ok {
		g = prometheus.NewCounter(prometheus.CounterOpts{
			Name: key,
			Help: key,
		})
		prometheus.MustRegister(g)
		p.counters[key] = g
	}
	g.Add(float64(val))
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushs metircs in background.
func (p *PrometheusSink) pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Printf("[INFO] disable Prometheus push client")
		return
	}
	log.Printf("[INFO] start Prometheus push client with server addr %s and interval %s", addr, interval)
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushs metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: udup do not have uniq name, so we use host+port to compose a name.
	job := "udup"
	for {
		err := push.AddFromGatherer(
			job, push.HostnameGroupingKey(),
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Printf("[ERR] could not push metrics to Prometheus Pushgateway: %v", err)
		}
		time.Sleep(interval)
	}
}