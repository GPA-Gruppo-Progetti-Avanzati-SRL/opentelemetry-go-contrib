package stats

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type BrokerStatsMetrics struct {
	Status          metric.Int64ObservableGauge
	ReceiveError    metric.Int64ObservableCounter
	TrasmisionError metric.Int64ObservableCounter
	Connect         metric.Int64ObservableCounter
	Disconnect      metric.Int64ObservableCounter

	TotalRequestsSent      metric.Int64ObservableCounter
	TotalResponsesReceived metric.Int64ObservableCounter
	TotalBytesSent         metric.Int64ObservableCounter
	TotalBytesReceived     metric.Int64ObservableCounter
	StateAge               metric.Int64Gauge
	Rtt                    WindowMetrics
}

func (m *BrokerStatsMetrics) MetricsToObserve() []metric.Observable {
	metricsToObserve := []metric.Observable{m.TotalBytesReceived, m.TotalBytesSent,
		m.TotalResponsesReceived, m.TotalRequestsSent, m.Status, m.ReceiveError, m.Connect, m.Disconnect}

	metricsToObserve = append(metricsToObserve, m.Rtt.MetricsToObserve()...)

	return metricsToObserve
}

func newSharedBrokerStatsMetrics(meter metric.Meter) BrokerStatsMetrics {
	sharedBrokerMetrics := BrokerStatsMetrics{}

	sharedBrokerMetrics.TotalRequestsSent, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.total.requests.sent",
		metric.WithUnit("{requests}"),
		metric.WithDescription("Total number of requests sent to Kafka brokers"),
	)

	sharedBrokerMetrics.TotalResponsesReceived, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.total.responses.received",
		metric.WithUnit("{responses}"),
		metric.WithDescription("Total number of responses received from Kafka brokers"),
	)

	sharedBrokerMetrics.TotalBytesSent, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.total.bytes.sent",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Total number of bytes sent to Kafka brokers"),
	)

	sharedBrokerMetrics.TotalBytesReceived, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.total.bytes.received",
		metric.WithUnit("{bytes}"),
		metric.WithDescription("Total number of bytes received from Kafka brokers"),
	)

	sharedBrokerMetrics.StateAge, _ = meter.Int64Gauge(
		"kafka.broker.stats.state.age",
		metric.WithUnit("{microseconds}"),
		metric.WithDescription("Time since last broker state change"),
	)

	sharedBrokerMetrics.Status, _ = meter.Int64ObservableGauge(
		"kafka.broker.stats.state",
		metric.WithUnit("{status}"),
		metric.WithDescription(StateDescription))

	sharedBrokerMetrics.ReceiveError, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.receive_error",
		metric.WithUnit("{errors}"),
		metric.WithDescription("Total number of receive errors"),
	)

	sharedBrokerMetrics.TrasmisionError, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.transmission_error",
		metric.WithUnit("{errors}"),
		metric.WithDescription("Total number of transmission errors"),
	)

	sharedBrokerMetrics.Connect, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.connect",
		metric.WithUnit("{connections}"),
		metric.WithDescription("Number of connection attempts, including successful and failed, and name resolution failures."),
	)

	sharedBrokerMetrics.Disconnect, _ = meter.Int64ObservableCounter(
		"kafka.broker.stats.disconnect",
		metric.WithUnit("{disconnections}"),
		metric.WithDescription("Number of disconnects (triggered by broker, network, load-balancer, etc.)."),
	)

	sharedBrokerMetrics.Rtt = *NewWindowMetrics(meter, "kafka.broker.stats.rtt", "us")

	return sharedBrokerMetrics
}

func (sharedBrokerMetrics *BrokerStatsMetrics) HandleMetrics(ctx context.Context, o metric.Observer, stats *Model) {

	commonAttributes := GetCommonAttributes(stats)

	for _, brokerData := range stats.Brokers {

		brokerAttributes := []attribute.KeyValue{
			attribute.String("broker_name", brokerData.Nodename),
			//attribute.Int64("broker_node_id", brokerData.Nodeid),
			//attribute.String("broker_state", brokerData.State),
			//attribute.String("broker_source", brokerData.Source),
		}

		brokerAttributes = append(brokerAttributes, commonAttributes...)
		attributes := metric.WithAttributeSet(attribute.NewSet(brokerAttributes...))

		sharedBrokerMetrics.StateAge.Record(ctx, brokerData.Stateage, attributes)

		o.ObserveInt64(sharedBrokerMetrics.Connect, brokerData.Connect, attributes)
		o.ObserveInt64(sharedBrokerMetrics.Disconnect, brokerData.Disconnects, attributes)

		o.ObserveInt64(sharedBrokerMetrics.ReceiveError, brokerData.Rxerrs, attributes)
		o.ObserveInt64(sharedBrokerMetrics.TrasmisionError, brokerData.Txerrs, attributes)

		o.ObserveInt64(sharedBrokerMetrics.TotalRequestsSent, brokerData.Tx, attributes)
		o.ObserveInt64(sharedBrokerMetrics.TotalBytesSent, brokerData.Txbytes, attributes)
		o.ObserveInt64(sharedBrokerMetrics.TotalResponsesReceived, int64(brokerData.Rx), attributes)
		o.ObserveInt64(sharedBrokerMetrics.TotalBytesReceived, brokerData.Rxbytes, attributes)

		state, err := BrokerStateFromString(brokerData.State)

		if err != nil {
			fmt.Println(err)
		} else {
			o.ObserveInt64(sharedBrokerMetrics.Status, int64(state), attributes)
		}

		sharedBrokerMetrics.Rtt.HandleWindowMetrics(ctx, o, &brokerData.Rtt, brokerAttributes)
	}

}
