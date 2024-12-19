package stats

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type WindowMetrics struct {
	Min    metric.Int64ObservableGauge
	Max    metric.Int64ObservableGauge
	Avg    metric.Int64ObservableGauge
	Count  metric.Int64ObservableGauge
	Stddev metric.Int64ObservableGauge
	Sum    metric.Int64ObservableGauge
}

func (m *WindowMetrics) MetricsToObserve() []metric.Observable {
	metricsToObserve := []metric.Observable{m.Min, m.Max, m.Avg, m.Count, m.Stddev, m.Sum}
	return metricsToObserve
}
func NewWindowMetrics(meter metric.Meter, prefix string, unit string) *WindowMetrics {

	metrics := WindowMetrics{}

	metrics.Min, _ = meter.Int64ObservableGauge(
		prefix+".min",
		metric.WithUnit(unit),
	)

	metrics.Max, _ = meter.Int64ObservableGauge(
		prefix+".max",
		metric.WithUnit(unit),
	)

	metrics.Avg, _ = meter.Int64ObservableGauge(
		prefix+".avg",
		metric.WithUnit(unit),
	)

	metrics.Count, _ = meter.Int64ObservableGauge(
		prefix + ".count",
	)

	metrics.Stddev, _ = meter.Int64ObservableGauge(
		prefix + ".stddev",
	)

	metrics.Sum, _ = meter.Int64ObservableGauge(
		prefix + ".sum",
	)

	return &metrics

}

func (windowMetrics WindowMetrics) HandleWindowMetrics(ctx context.Context, o metric.Observer, stats *WindowStats, attributes []attribute.KeyValue) {

	commonLabels := metric.WithAttributeSet(attribute.NewSet(attributes...))

	o.ObserveInt64(windowMetrics.Min, stats.Min, commonLabels)
	o.ObserveInt64(windowMetrics.Max, stats.Max, commonLabels)
	o.ObserveInt64(windowMetrics.Avg, stats.Avg, commonLabels)
	o.ObserveInt64(windowMetrics.Count, stats.Cnt, commonLabels)
	o.ObserveInt64(windowMetrics.Stddev, stats.Stddev, commonLabels)
	o.ObserveInt64(windowMetrics.Sum, stats.Sum, commonLabels)

}
