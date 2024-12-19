package stats

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ConsumerStatsMetric struct {
	KafkaStatsMetrics
	StateAge       metric.Int64ObservableGauge
	State          metric.Int64ObservableGauge
	RebalanceAge   metric.Int64ObservableGauge
	RebalanceCnt   metric.Int64ObservableCounter
	AssignmentSize metric.Int64ObservableGauge

	lastReport   string
	registration metric.Registration
}

func (p *ConsumerStatsMetric) SetLastReport(report string) {
	p.lastReport = report
}

func NewConsumerStatsMetrics(meter metric.Meter) *ConsumerStatsMetric {
	consumerMetrics := ConsumerStatsMetric{}

	consumerMetrics.RebalanceAge, _ = meter.Int64ObservableGauge(
		"kafka.consumer.stats.rebalance.age",
		metric.WithUnit("ms"),
		metric.WithDescription("Time elapsed since last rebalance (assign or revoke)."),
	)

	consumerMetrics.StateAge, _ = meter.Int64ObservableGauge(
		"kafka.consumer.stats.state.age",
		metric.WithUnit("ms"),
		metric.WithDescription("Time elapsed since last state change"),
	)

	consumerMetrics.RebalanceCnt, _ = meter.Int64ObservableCounter(
		"kafka.consumer.stats.rebalance",
		metric.WithUnit("{rebalance}"),
		metric.WithDescription("Total number of rebalances (assign or revoke)."),
	)

	consumerMetrics.AssignmentSize, _ = meter.Int64ObservableGauge(
		"kafka.consumer.stats.assignment",
		metric.WithUnit("{assignment}"),
		metric.WithDescription("Current assignment's partition count."),
	)

	consumerMetrics.StatsType = Consumer
	initSharedMetrics(meter, &consumerMetrics.KafkaStatsMetrics)

	metricsToObserve := consumerMetrics.KafkaStatsMetrics.MetricsToObserve()

	metricsToObserve = append(metricsToObserve, consumerMetrics.RebalanceAge, consumerMetrics.RebalanceCnt)

	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		model := ParseKafkaStats(consumerMetrics.lastReport)
		HandleSharedStatsMetrics(&consumerMetrics.KafkaStatsMetrics, ctx, o, model)
		HandleStatsConsumer(&consumerMetrics, ctx, o, model)
		return nil
	}, metricsToObserve...)

	if err != nil {
		fmt.Println(err)
	}

	consumerMetrics.registration = reg

	return &consumerMetrics
}

func HandleStatsConsumer(consumerMetrics *ConsumerStatsMetric, ctx context.Context, o metric.Observer, stats *Model) {

	commonLabels := metric.WithAttributeSet(attribute.NewSet(GetCommonAttributes(stats)...))

	o.ObserveInt64(consumerMetrics.RebalanceAge, stats.Cgpr.RebalanceAge, commonLabels)
	o.ObserveInt64(consumerMetrics.RebalanceCnt, stats.Cgpr.RebalanceCnt, commonLabels)
	o.ObserveInt64(consumerMetrics.AssignmentSize, stats.Cgpr.AssignmentSize, commonLabels)
	o.ObserveInt64(consumerMetrics.StateAge, stats.Cgpr.StateAge, commonLabels)

}
