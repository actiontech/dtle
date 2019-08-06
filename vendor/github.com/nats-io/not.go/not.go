// Copyright 2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package not

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-lib/metrics"

	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
)

// TraceMsg will be used as an io.Writer and io.Reader for the span's context and
// the payload. The span will have to be written first and read first.
type TraceMsg struct {
	bytes.Buffer
}

// NewTraceMsg creates a trace msg from a NATS message's data payload.
func NewTraceMsg(m *nats.Msg) *TraceMsg {
	b := bytes.NewBuffer(m.Data)
	return &TraceMsg{*b}
}

// InitTracing handles the common tracing setup functionality, and keeps
// implementation specific (Jaeger) configuration here.
func InitTracing(serviceName string) (opentracing.Tracer, io.Closer) {
	// Sample configuration for testing. Use constant sampling to sample every trace
	// and enable LogSpan to log every span via configured Logger.
	cfg := jaegercfg.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	// Example logger and metrics factory. Use github.com/uber/jaeger-client-go/log
	// and github.com/uber/jaeger-lib/metrics respectively to bind to real logging and metrics
	// frameworks.
	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	// Initialize tracer with a logger and a metrics factory
	tracer, closer, err := cfg.NewTracer(
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	if err != nil {
		log.Fatalf("couldn't setup tracing: %v", err)
	}
	return tracer, closer
}

// SetupConnOptions sets up connection options with a tracer to trace
// salient events.
func SetupConnOptions(tracer opentracing.Tracer, opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		span := tracer.StartSpan("Disconnect Handler")
		s := fmt.Sprintf("Disconnected: will attempt reconnects for %.0fm", totalWait.Minutes())
		span.LogEvent(s)
		span.Finish()
		log.Printf(s)
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		span := tracer.StartSpan("Reconnect Handler")
		s := fmt.Sprintf("Reconnected [%s]", nc.ConnectedUrl())
		span.LogEvent(s)
		span.Finish()
		log.Printf(s)
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		span := tracer.StartSpan("Closed Handler")
		s := "Exiting, no servers available"
		span.LogEvent(s)
		span.Finish()
		log.Printf(s)
	}))
	return opts
}
