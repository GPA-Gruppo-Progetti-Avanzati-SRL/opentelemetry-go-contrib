package stats

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type StatsType int

const (
	Producer StatsType = iota
	Consumer
)

type KafkaStatsMetrics struct {
	TotalRequestsSent     metric.Int64ObservableCounter
	TotalRequestsReceived metric.Int64ObservableCounter
	TotalBytesSent        metric.Int64ObservableCounter
	TotalBytesReceived    metric.Int64ObservableCounter
	BrokerStatsMetrics    BrokerStatsMetrics
	registration          metric.Registration
	TopicsMetrics         TopicStatsMetrics
	StatsType             StatsType
}

func (m *KafkaStatsMetrics) Close() {
	m.registration.Unregister()
}

func GetCommonAttributes(stats *Model) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("client_id", stats.ClientID),
		attribute.String("name", stats.Name),
	}

}

func (m *KafkaStatsMetrics) MetricsToObserve() []metric.Observable {
	metricsToObserve := []metric.Observable{m.TotalBytesReceived, m.TotalBytesSent, m.TotalRequestsReceived, m.TotalRequestsSent}

	metricsToObserve = append(metricsToObserve, m.BrokerStatsMetrics.MetricsToObserve()...)
	metricsToObserve = append(metricsToObserve, m.TopicsMetrics.MetricsToObserve()...)

	return metricsToObserve
}

func initSharedMetrics(meter metric.Meter, sharedMetrics *KafkaStatsMetrics) {

	sharedMetrics.TotalRequestsSent, _ = meter.Int64ObservableCounter(
		"kafka.shared.stats.total.requests.sent",
		metric.WithUnit("{requests}"),
		metric.WithDescription("Total number of requests sent to Kafka brokers"),
	)

	sharedMetrics.TotalRequestsReceived, _ = meter.Int64ObservableCounter(
		"kafka.shared.stats.total.requests.received",
		metric.WithUnit("{requests}"),
		metric.WithDescription("Total number of responses received from Kafka brokers"),
	)

	sharedMetrics.TotalBytesSent, _ = meter.Int64ObservableCounter(
		"kafka.shared.stats.total.bytes.sent",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Total number of bytes transmitted to Kafka brokers"),
	)

	sharedMetrics.TotalBytesReceived, _ = meter.Int64ObservableCounter(
		"kafka.shared.stats.total.bytes.received",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Total number of bytes received from Kafka brokers"),
	)

	sharedMetrics.BrokerStatsMetrics = newSharedBrokerStatsMetrics(meter)
	sharedMetrics.TopicsMetrics = newTopicStatsMetrics(meter, sharedMetrics.StatsType)
}

func ParseKafkaStats(s string) *Model {
	data := Model{}
	json.Unmarshal([]byte(s), &data)
	return &data
}

func HandleSharedStatsMetrics(sharedMetrics *KafkaStatsMetrics, ctx context.Context, o metric.Observer, stats *Model) {

	commonLabels := metric.WithAttributeSet(attribute.NewSet(GetCommonAttributes(stats)...))

	o.ObserveInt64(sharedMetrics.TotalRequestsSent, stats.Tx, commonLabels)
	o.ObserveInt64(sharedMetrics.TotalBytesSent, stats.TxBytes, commonLabels)
	o.ObserveInt64(sharedMetrics.TotalRequestsReceived, stats.Rx, commonLabels)
	o.ObserveInt64(sharedMetrics.TotalBytesReceived, stats.RxBytes, commonLabels)

	sharedMetrics.BrokerStatsMetrics.HandleMetrics(ctx, o, stats)
	sharedMetrics.TopicsMetrics.HandleMetrics(ctx, o, stats)

}
