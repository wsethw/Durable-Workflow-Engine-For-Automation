package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Metrics struct {
	workflowInstances *prometheus.CounterVec
	stepDuration      *prometheus.HistogramVec
	instanceDuration  prometheus.Histogram
}

func NewMetrics() *Metrics {
	metrics := &Metrics{
		workflowInstances: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_instances_total",
			Help: "Total workflow instances by terminal or transition status.",
		}, []string{"status"}),
		stepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "step_execution_duration_seconds",
			Help:    "Step execution duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"step_type", "status"}),
		instanceDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "workflow_instance_duration_seconds",
			Help:    "Workflow instance duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	register(metrics.workflowInstances)
	register(metrics.stepDuration)
	register(metrics.instanceDuration)
	return metrics
}

func (m *Metrics) IncInstance(status string) {
	if m == nil {
		return
	}
	m.workflowInstances.WithLabelValues(status).Inc()
}

func (m *Metrics) ObserveStep(stepType string, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.stepDuration.WithLabelValues(stepType, status).Observe(duration.Seconds())
}

func (m *Metrics) ObserveInstance(duration time.Duration) {
	if m == nil {
		return
	}
	m.instanceDuration.Observe(duration.Seconds())
}

func Handler() http.Handler {
	return promhttp.Handler()
}

func InitTracing(ctx context.Context, serviceName string, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	exporter, err := otlptracehttp.New(exportCtx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.Default()),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func register(collector prometheus.Collector) {
	if err := prometheus.Register(collector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			return
		}
		panic(err)
	}
}
