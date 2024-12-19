package stats

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ProducerStatsMetric struct {
	KafkaStatsMetrics
	MessageCountInQueue   metric.Int64Gauge
	MessageMaxInQueue     metric.Int64Gauge
	MessageSizeInQueue    metric.Int64Gauge
	MessageSizeMaxInQueue metric.Int64Gauge
	TotalMessagesProduced metric.Int64ObservableCounter
	TotalBytesProduced    metric.Int64ObservableCounter
	lastReport            string
}

func (p *ProducerStatsMetric) SetLastReport(report string) {
	p.lastReport = report
}

func NewProducerStatsMetrics(meter metric.Meter) *ProducerStatsMetric {

	statsMetrics := ProducerStatsMetric{}

	statsMetrics.MessageCountInQueue, _ = meter.Int64Gauge(
		"kafka.producer.stats.message.queue.usage",
		metric.WithUnit("{message}"),
		metric.WithDescription("Current number of messages in producer queues"),
	)

	statsMetrics.MessageMaxInQueue, _ = meter.Int64Gauge(
		"kafka.producer.stats.message.queue.limit",
		metric.WithUnit("{message}"),
		metric.WithDescription("Threshold: maximum number of messages allowed on the producer queues"),
	)

	statsMetrics.MessageSizeInQueue, _ = meter.Int64Gauge(
		"kafka.producer.stats.message.size.queue.usage",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Current total size of messages in producer queues"),
	)

	statsMetrics.MessageSizeMaxInQueue, _ = meter.Int64Gauge(
		"kafka.producer.stats.message.size.queue.limit",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Threshold: maximum total size of messages allowed on the producer queues"),
	)

	statsMetrics.TotalMessagesProduced, _ = meter.Int64ObservableCounter(
		"kafka.producer.stats.total.messages.produced",
		metric.WithUnit("{message}"),
		metric.WithDescription("Total number of messages transmitted (produced) to Kafka brokers"),
	)

	statsMetrics.TotalBytesProduced, _ = meter.Int64ObservableCounter(
		"kafka.producer.stats.total.bytes.produced",
		metric.WithUnit("bytes"),
		metric.WithDescription("Total number of bytes transmitted for produced messages to Kafka brokers"),
	)

	statsMetrics.StatsType = Producer
	initSharedMetrics(meter, &statsMetrics.KafkaStatsMetrics)

	metricsToObserve := statsMetrics.KafkaStatsMetrics.MetricsToObserve()

	metricsToObserve = append(metricsToObserve, statsMetrics.TotalMessagesProduced, statsMetrics.TotalBytesProduced)

	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		model := ParseKafkaStats(statsMetrics.lastReport)
		HandleSharedStatsMetrics(&statsMetrics.KafkaStatsMetrics, ctx, o, model)
		HandleStatsProducer(&statsMetrics, ctx, o, model)
		return nil
	}, metricsToObserve...)

	if err != nil {
		fmt.Println(err)
	}

	statsMetrics.registration = reg

	return &statsMetrics
}

func HandleStatsProducer(producerMetrics *ProducerStatsMetric, ctx context.Context, o metric.Observer, stats *Model) {

	commonLabels := metric.WithAttributeSet(attribute.NewSet(GetCommonAttributes(stats)...))

	// Record producer-specific metrics
	producerMetrics.MessageCountInQueue.Record(ctx, int64(stats.MsgCnt), commonLabels)
	producerMetrics.MessageSizeInQueue.Record(ctx, int64(stats.MsgSize), commonLabels)
	producerMetrics.MessageMaxInQueue.Record(ctx, int64(stats.MsgMax), commonLabels)
	producerMetrics.MessageSizeMaxInQueue.Record(ctx, stats.MsgSizeMax, commonLabels)

	o.ObserveInt64(producerMetrics.TotalBytesProduced, stats.TxBytes, commonLabels)
	o.ObserveInt64(producerMetrics.TotalBytesProduced, stats.TxmsgBytes, commonLabels)

}

/*
func handleStatsConsumer(meter metric.Meter, stats Model) {
	consumerMetrics := newConsumerStatsMetrics(meter)
	sharedBrokerMetrics := newSharedBrokerStatsMetrics(meter)
	sharedMetrics := newSharedStatsMetrics(meter)

	ctx := context.Background()

	// Create common attributes for all metrics
	commonAttributes := []attribute.KeyValue{
		attribute.String("client_id", stats.ClientID),
		attribute.String("consumer_name", stats.Name),
	}

	// Extract consumer-specific metrics and populate
	consumerMetrics.MessagesConsumed.Add(ctx, int64(stats.RxMsgs), commonAttributes...)
	consumerMetrics.BytesConsumed.Add(ctx, stats.RxMsgBytes, commonAttributes...)

	// Extract consumer lag metrics from each partition if available
	for topicName, topicData := range stats.Topics {
		for _, partitionData := range topicData.Partitions {
			partitionAttributes := append(commonAttributes,
				attribute.String("topic_name", topicName),
				attribute.Int("partition_id", partitionData.Partition),
			)
			consumerMetrics.ConsumerLag.Set(ctx, int64(partitionData.ConsumerLag), partitionAttributes...)
			consumerMetrics.ConsumerLagStored.Set(ctx, int64(partitionData.StoredOffset), partitionAttributes...)
			consumerMetrics.FetchQueueCount.Set(ctx, int64(partitionData.FetchQCnt), partitionAttributes...)
			consumerMetrics.FetchQueueSize.Set(ctx, int64(partitionData.FetchQSize), partitionAttributes...)
		}
	}

	// Extract broker metrics and populate


	// Extract consumer group metrics if available and populate
	if stats.Cgrp != nil {
		cgAttributes := append(commonAttributes,
			attribute.String("consumer_group_state", stats.Cgrp.State),
		)

		cgMetrics := newConsumerGroupStatsMetrics(meter)
		cgMetrics.State.Set(ctx, stats.Cgrp.State, cgAttributes...)
		cgMetrics.JoinState.Set(ctx, stats.Cgrp.JoinState, cgAttributes...)
		cgMetrics.RebalanceCount.Add(ctx, int64(stats.Cgrp.RebalanceCnt), cgAttributes...)
		cgMetrics.AssignmentSize.Set(ctx, int64(stats.Cgrp.AssignmentSize), cgAttributes...)
		cgMetrics.RebalanceAge.Set(ctx, int64(stats.Cgrp.RebalanceAge), cgAttributes...)
	}
}
*/
