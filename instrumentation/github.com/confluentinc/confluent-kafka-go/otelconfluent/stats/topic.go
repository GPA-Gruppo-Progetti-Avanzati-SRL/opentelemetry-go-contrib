package stats

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/otel/attribute"
)

type TopicStatsMetrics struct {
	//	TopicName   metric.StringObservableGauge
	Age         metric.Int64ObservableGauge
	MetadataAge metric.Int64ObservableGauge
	BatchSize   WindowMetrics
	BatchCount  WindowMetrics
	Partitions  PartitionStatsMetrics
}

type PartitionStatsMetrics struct {
	StatsType StatsType

	PartitionID           metric.Int64ObservableGauge
	BrokerID              metric.Int64ObservableGauge
	LeaderID              metric.Int64ObservableGauge
	MsgQueueCount         metric.Int64ObservableGauge
	MsgQueueBytes         metric.Int64ObservableGauge
	TransmitMsgQueueCount metric.Int64ObservableGauge
	TransmitMsgQueueBytes metric.Int64ObservableGauge
	FetchQueueCount       metric.Int64ObservableGauge
	FetchQueueSize        metric.Int64ObservableGauge
	//	FetchState              metric.StringObservableGauge TODO implement
	QueryOffset             metric.Int64ObservableGauge
	NextOffset              metric.Int64ObservableGauge
	AppOffset               metric.Int64ObservableGauge
	StoredOffset            metric.Int64ObservableGauge
	StoredLeaderEpoch       metric.Int64ObservableGauge
	CommittedOffset         metric.Int64ObservableGauge
	CommittedLeaderEpoch    metric.Int64ObservableGauge
	EOFOffset               metric.Int64ObservableGauge
	LowOffset               metric.Int64ObservableGauge
	HighOffset              metric.Int64ObservableGauge
	LastStableOffset        metric.Int64ObservableGauge
	ConsumerLag             metric.Int64ObservableGauge
	ConsumerLagStored       metric.Int64ObservableGauge
	LeaderEpoch             metric.Int64ObservableGauge
	TransmittedMessages     metric.Int64ObservableCounter
	TransmittedBytes        metric.Int64ObservableCounter
	ReceivedMessages        metric.Int64ObservableCounter
	ReceivedBytes           metric.Int64ObservableCounter
	DroppedOutdatedMessages metric.Int64ObservableCounter
	MessagesInFlight        metric.Int64ObservableGauge
	NextAckSequence         metric.Int64ObservableGauge
	NextErrorSequence       metric.Int64ObservableGauge
	LastAckedMessageID      metric.Int64ObservableGauge

	MessageReceivedOrSend metric.Int64ObservableGauge
}

func newTopicStatsMetrics(meter metric.Meter, statsType StatsType) TopicStatsMetrics {
	topicStats := TopicStatsMetrics{}

	topicStats.Age, _ = meter.Int64ObservableGauge(
		"kafka.topic.age",
		metric.WithUnit("{milliseconds}"),
		metric.WithDescription("Age of the topic object in milliseconds"),
	)

	topicStats.MetadataAge, _ = meter.Int64ObservableGauge(
		"kafka.topic.metadata_age",
		metric.WithUnit("{milliseconds}"),
		metric.WithDescription("Age of metadata from broker for this topic in milliseconds"),
	)

	topicStats.BatchSize = *NewWindowMetrics(meter, "kafka.topic.batch.size", "bytes")
	topicStats.BatchCount = *NewWindowMetrics(meter, "kafka.topic.batch.count", "{messages}")

	topicStats.Partitions = newPartitionStatsMetrics(meter, statsType)

	return topicStats
}

func newPartitionStatsMetrics(meter metric.Meter, statsType StatsType) PartitionStatsMetrics {
	partitionStats := PartitionStatsMetrics{}

	partitionStats.StatsType = statsType

	partitionStats.PartitionID, _ = meter.Int64ObservableGauge(
		"kafka.partition.id",
		metric.WithDescription("Partition ID (-1 for UA/UnAssigned)"),
	)

	partitionStats.BrokerID, _ = meter.Int64ObservableGauge(
		"kafka.partition.broker_id",
		metric.WithDescription("Broker ID messages are being fetched from"),
	)

	partitionStats.LeaderID, _ = meter.Int64ObservableGauge(
		"kafka.partition.leader_id",
		metric.WithDescription("Current leader broker ID"),
	)

	partitionStats.MsgQueueCount, _ = meter.Int64ObservableGauge(
		"kafka.partition.msg_queue_count",
		metric.WithUnit("{messages}"),
		metric.WithDescription("Number of messages in the first-level queue"),
	)

	partitionStats.MsgQueueBytes, _ = meter.Int64ObservableGauge(
		"kafka.partition.msg_queue_bytes",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Number of bytes in the first-level queue"),
	)

	partitionStats.TransmitMsgQueueCount, _ = meter.Int64ObservableGauge(
		"kafka.partition.transmit_msg_queue_count",
		metric.WithUnit("{messages}"),
		metric.WithDescription("Number of messages ready to be produced in transmit queue"),
	)

	partitionStats.TransmitMsgQueueBytes, _ = meter.Int64ObservableGauge(
		"kafka.partition.transmit_msg_queue_bytes",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Number of bytes in the transmit queue"),
	)

	partitionStats.FetchQueueCount, _ = meter.Int64ObservableGauge(
		"kafka.partition.fetch_queue_count",
		metric.WithUnit("{messages}"),
		metric.WithDescription("Number of pre-fetched messages in fetch queue"),
	)

	partitionStats.FetchQueueSize, _ = meter.Int64ObservableGauge(
		"kafka.partition.fetch_queue_size",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Bytes in fetch queue"),
	)

	/*partitionStats.FetchState, _ = meter.StringObservableGauge(
		"kafka.partition.fetch_state",
		metric.WithDescription("Consumer fetch state for this partition"),
	)*/

	partitionStats.QueryOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.query_offset",
		metric.WithDescription("Current/Last logical offset query"),
	)

	partitionStats.NextOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.next_offset",
		metric.WithDescription("Next offset to fetch"),
	)

	partitionStats.AppOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.app_offset",
		metric.WithDescription("Offset of last message passed to application + 1"),
	)

	partitionStats.StoredOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.stored_offset",
		metric.WithDescription("Offset to be committed"),
	)

	partitionStats.StoredLeaderEpoch, _ = meter.Int64ObservableGauge(
		"kafka.partition.stored_leader_epoch",
		metric.WithDescription("Partition leader epoch of stored offset"),
	)

	partitionStats.CommittedOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.committed_offset",
		metric.WithDescription("Last committed offset"),
	)

	partitionStats.CommittedLeaderEpoch, _ = meter.Int64ObservableGauge(
		"kafka.partition.committed_leader_epoch",
		metric.WithDescription("Partition leader epoch of committed offset"),
	)

	partitionStats.EOFOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.eof_offset",
		metric.WithDescription("Last PARTITION_EOF signaled offset"),
	)

	partitionStats.LowOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.low_offset",
		metric.WithDescription("Partition's low watermark offset on broker"),
	)

	partitionStats.HighOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.high_offset",
		metric.WithDescription("Partition's high watermark offset on broker"),
	)

	partitionStats.LastStableOffset, _ = meter.Int64ObservableGauge(
		"kafka.partition.last_stable_offset",
		metric.WithDescription("Partition's last stable offset on broker"),
	)

	partitionStats.ConsumerLag, _ = meter.Int64ObservableGauge(
		"kafka.partition.consumer_lag",
		metric.WithDescription("Difference between hi_offset or ls_offset and committed_offset"),
	)

	partitionStats.ConsumerLagStored, _ = meter.Int64ObservableGauge(
		"kafka.partition.consumer_lag_stored",
		metric.WithDescription("Difference between hi_offset or ls_offset and stored_offset"),
	)

	partitionStats.LeaderEpoch, _ = meter.Int64ObservableGauge(
		"kafka.partition.leader_epoch",
		metric.WithDescription("Last known partition leader epoch"),
	)

	partitionStats.TransmittedMessages, _ = meter.Int64ObservableCounter(
		"kafka.partition.transmitted_messages",
		metric.WithDescription("Total number of messages transmitted"),
	)

	partitionStats.TransmittedBytes, _ = meter.Int64ObservableCounter(
		"kafka.partition.transmitted_bytes",
		metric.WithDescription("Total number of bytes transmitted"),
	)

	partitionStats.ReceivedMessages, _ = meter.Int64ObservableCounter(
		"kafka.partition.received_messages",
		metric.WithDescription("Total number of messages received"),
	)

	partitionStats.ReceivedBytes, _ = meter.Int64ObservableCounter(
		"kafka.partition.received_bytes",
		metric.WithDescription("Total number of bytes received"),
	)

	partitionStats.DroppedOutdatedMessages, _ = meter.Int64ObservableCounter(
		"kafka.partition.dropped_outdated_messages",
		metric.WithDescription("Dropped outdated messages"),
	)

	partitionStats.MessagesInFlight, _ = meter.Int64ObservableGauge(
		"kafka.partition.messages_in_flight",
		metric.WithDescription("Current number of messages in-flight to/from broker"),
	)

	partitionStats.NextAckSequence, _ = meter.Int64ObservableGauge(
		"kafka.partition.next_ack_sequence",
		metric.WithDescription("Next expected acked sequence (idempotent producer)"),
	)

	partitionStats.NextErrorSequence, _ = meter.Int64ObservableGauge(
		"kafka.partition.next_error_sequence",
		metric.WithDescription("Next expected errored sequence (idempotent producer)"),
	)

	partitionStats.LastAckedMessageID, _ = meter.Int64ObservableGauge(
		"kafka.partition.last_acked_message_id",
		metric.WithDescription("Last acked internal message ID (idempotent producer)"),
	)

	partitionStats.MessageReceivedOrSend, _ = meter.Int64ObservableGauge(
		"kafka.partition.message",
		metric.WithDescription("Total number of messages received (consumer, same as rxmsgs), or total number of messages produced (possibly not yet transmitted) (producer)."))

	return partitionStats
}

func (m *TopicStatsMetrics) HandleMetrics(ctx context.Context, o metric.Observer, model *Model) {

	for topicName, topic := range model.Topics {
		attributes := []attribute.KeyValue{
			attribute.String("topic", topicName),
		}

		attributes = append(attributes, GetCommonAttributes(model)...)
		commonAttributes := metric.WithAttributeSet(attribute.NewSet(attributes...))

		//o.ObserveInt64(m.Age, topic.Age, commonAttributes)
		o.ObserveInt64(m.MetadataAge, topic.MetadataAge, commonAttributes)

		m.BatchSize.HandleWindowMetrics(ctx, o, &topic.Batchsize, attributes)
		m.BatchCount.HandleWindowMetrics(ctx, o, &topic.Batchcnt, attributes)

		for id, partition := range topic.Partitions {
			if id == -1 {
				continue
			}

			partitionAttributes := append(attributes, attribute.Int("partition_id", id))
			m.Partitions.HandlePartitionMetrics(ctx, o, &partition, partitionAttributes)
		}
	}

}

func (m *PartitionStatsMetrics) HandlePartitionMetrics(ctx context.Context, o metric.Observer, partition *Partition, attributes []attribute.KeyValue) {

	commonAttributes := metric.WithAttributeSet(attribute.NewSet(attributes...))
	if m.StatsType == Consumer {
		o.ObserveInt64(m.FetchQueueCount, partition.FetchqCnt, commonAttributes)
		o.ObserveInt64(m.FetchQueueSize, partition.FetchqSize, commonAttributes)

		//o.ObserveInt64(m.QueryOffset, partition.QueryOffset, commonAttributes)
		o.ObserveInt64(m.NextOffset, partition.NextOffset, commonAttributes)
		o.ObserveInt64(m.AppOffset, partition.AppOffset, commonAttributes)
		o.ObserveInt64(m.CommittedOffset, partition.CommittedOffset, commonAttributes)
		o.ObserveInt64(m.StoredOffset, partition.StoredOffset, commonAttributes)

		//o.ObserveInt64(m.StoredLeaderEpoch, partition.StoredLeaderEpoch, commonAttributes)
		o.ObserveInt64(m.EOFOffset, partition.EOFOffset, commonAttributes)
		o.ObserveInt64(m.LowOffset, partition.LoOffset, commonAttributes)
		o.ObserveInt64(m.HighOffset, partition.HiOffset, commonAttributes)
		o.ObserveInt64(m.ConsumerLag, partition.ConsumerLag, commonAttributes)
		o.ObserveInt64(m.ReceivedMessages, partition.Rxmsgs, commonAttributes)
		o.ObserveInt64(m.ReceivedBytes, partition.Rxbytes, commonAttributes)

		//o.ObserveInt64(m.LastStableOffset, partition.LastStableOffset, commonAttributes)

		//o.ObserveInt64(m.CommittedLeaderEpoch, partition.CommittedLeaderEpoch, commonAttributes)
		//o.ObserveInt64(m.ConsumerLagStored, partition.ConsumerLagStored, commonAttributes)

	} else if m.StatsType == Producer {
		o.ObserveInt64(m.TransmitMsgQueueCount, partition.Txmsgs, commonAttributes)
		o.ObserveInt64(m.TransmitMsgQueueBytes, partition.Txbytes, commonAttributes)
		o.ObserveInt64(m.TransmittedMessages, partition.Txmsgs, commonAttributes)
		o.ObserveInt64(m.TransmittedBytes, partition.Txbytes, commonAttributes)
		o.ObserveInt64(m.MsgQueueCount, partition.MsgqCnt, commonAttributes)
		o.ObserveInt64(m.MsgQueueBytes, partition.MsgqBytes, commonAttributes)

	}
	o.ObserveInt64(m.BrokerID, partition.Broker, commonAttributes)
	o.ObserveInt64(m.LeaderID, partition.Leader, commonAttributes)
	o.ObserveInt64(m.MessageReceivedOrSend, partition.Msgs, commonAttributes)

	//TODO implement different levels of debug and fetch state
	//o.ObserveString(m.FetchState, partition.FetchState, commonAttributes)

	//o.ObserveInt64(m.LeaderEpoch, partition.LeaderEpoch, commonAttributes)

	//o.ObserveInt64(m.DroppedOutdatedMessages, partition.DroppedOutdatedMessages, commonAttributes)
	//o.ObserveInt64(m.MessagesInFlight, partition.MessagesInFlight, commonAttributes)
	//o.ObserveInt64(m.NextAckSequence, partition.NextAckSequence, commonAttributes)
	//o.ObserveInt64(m.NextErrorSequence, partition.NextErrorSequence, commonAttributes)
	//o.ObserveInt64(m.LastAckedMessageID, partition.LastAckedMessageID, commonAttributes)
}

func (m *TopicStatsMetrics) MetricsToObserve() []metric.Observable {
	metricsToObserve := []metric.Observable{m.Age, m.MetadataAge}
	metricsToObserve = append(metricsToObserve, m.BatchSize.MetricsToObserve()...)
	metricsToObserve = append(metricsToObserve, m.BatchCount.MetricsToObserve()...)
	metricsToObserve = append(metricsToObserve, m.Partitions.MetricsToObserve()...)
	return metricsToObserve
}

func (m *PartitionStatsMetrics) MetricsToObserve() []metric.Observable {
	return []metric.Observable{
		m.PartitionID, m.BrokerID, m.LeaderID,
		m.MsgQueueCount, m.MsgQueueBytes, m.TransmitMsgQueueCount, m.TransmitMsgQueueBytes,
		m.FetchQueueCount, m.FetchQueueSize, m.QueryOffset, m.NextOffset,
		m.AppOffset, m.StoredOffset, m.StoredLeaderEpoch, m.CommittedOffset, m.CommittedLeaderEpoch,
		m.EOFOffset, m.LowOffset, m.HighOffset, m.LastStableOffset, m.ConsumerLag,
		m.ConsumerLagStored, m.LeaderEpoch, m.TransmittedMessages, m.TransmittedBytes,
		m.ReceivedMessages, m.ReceivedBytes, m.DroppedOutdatedMessages, m.MessagesInFlight,
		m.NextAckSequence, m.NextErrorSequence, m.LastAckedMessageID, m.MessageReceivedOrSend,
	}
}
