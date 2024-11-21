/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package otelconfluent

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/opentelemetry-go-contrib/instrumentation/github.com/confluentinc/confluent-kafka-go/otelconfluent/internal"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"go.opentelemetry.io/contrib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type ConsumeFunc func(consumer *kafka.Consumer, msg *kafka.Message, ctx context.Context) error

type Consumer struct {
	*kafka.Consumer
	ctx             context.Context
	tracer          oteltrace.Tracer
	propagator      propagation.TextMapPropagator
	consumerGroupID string
	metric          metric.Meter
	metrics         struct {
		messageCounter metric.Int64Counter
	}
}

func NewConsumerWithTracing(consumer *kafka.Consumer, opts ...Option) *Consumer {
	cfg := &config{
		tracerProvider: otel.GetTracerProvider(),
		propagator:     otel.GetTextMapPropagator(),
		tracerName:     tracerName,
	}

	for _, o := range opts {
		o.apply(cfg)
	}

	c := &Consumer{
		Consumer: consumer,
		ctx:      context.Background(),
		tracer: cfg.tracerProvider.Tracer(
			cfg.tracerName,
			oteltrace.WithInstrumentationVersion(contrib.SemVersion()),
		),
		propagator:      cfg.propagator,
		metric:          otel.Meter(cfg.tracerName),
		consumerGroupID: cfg.consumerGroupID,
	}

	c.metrics.messageCounter, _ = c.metric.Int64Counter(
		"kafka.consumed.message",
		metric.WithUnit("1"),
		metric.WithDescription("Message consumed by topic"),
	)
	return c

}

func (c *Consumer) attrsByOperationAndMessage(operation internal.Operation, msg *kafka.Message) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingKafkaConsumerGroup(c.consumerGroupID),
		semconv.MessagingOperationName("poll"),
		semconv.MessagingOperationTypeReceive,
	}

	if msg != nil {
		attributes = append(attributes, semconv.MessagingKafkaMessageKey(string(msg.Key)))
		attributes = append(attributes, internal.KafkaMessageHeaders(msg.Headers)...)
		attributes = append(attributes, semconv.MessagingDestinationPartitionIDKey.Int(int(msg.TopicPartition.Partition)))
		attributes = append(attributes, semconv.MessagingKafkaMessageOffset(int(msg.TopicPartition.Offset)))

		if topic := msg.TopicPartition.Topic; topic != nil {
			attributes = append(attributes, semconv.MessagingDestinationName(*topic))
		}
	}

	return attributes
}

func (c *Consumer) startSpan(operationName internal.Operation, msg *kafka.Message) (oteltrace.Span, context.Context) {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
	}

	carrier := NewMessageCarrier(msg)
	ctx := c.propagator.Extract(c.ctx, carrier)

	ctx, span := c.tracer.Start(ctx, string(operationName), opts...)

	c.propagator.Inject(ctx, carrier)

	span.SetAttributes(c.attrsByOperationAndMessage(operationName, msg)...)

	return span, ctx
}

func (c *Consumer) metricHandle(msg *kafka.Message) {

	attributes := []attribute.KeyValue{attribute.String("topic", *msg.TopicPartition.Topic),
		attribute.Int64("partition", int64(msg.TopicPartition.Partition))}

	c.metrics.messageCounter.Add(context.Background(), 1, metric.WithAttributes(attributes...))

}

// ReadMessage creates a new span and reads a Kafka message from current consumer.
func (c *Consumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	msg, err := c.Consumer.ReadMessage(timeout)

	if msg != nil {
		s, _ := c.startSpan(internal.OperationConsume, msg)
		endSpan(s, err)
	}

	return msg, err
}

// ReadMessageWithHandler reads a message and runs the given handler by tracing it.
func (c *Consumer) ReadMessageWithHandler(timeout time.Duration, handler ConsumeFunc) (*kafka.Message, error) {
	msg, err := c.Consumer.ReadMessage(timeout)

	if msg != nil {
		s, ctx := c.startSpan(internal.OperationConsume, msg)
		err = handler(c.Consumer, msg, ctx)
		c.metricHandle(msg)
		endSpan(s, err)
	}

	return msg, err
}

// Poll retrieves an event from current consumer and creates a new span
// if it is a kafka.Message event type.
func (c *Consumer) Poll(timeoutMs int) kafka.Event {
	event := c.Consumer.Poll(timeoutMs)

	switch ev := event.(type) {
	case *kafka.Message:
		msg := ev
		if msg != nil {
			s, _ := c.startSpan(internal.OperationConsume, msg)
			endSpan(s, nil)
		}
	}

	return event
}

// PollWithHandler retrieves an event from current consumer, creates a new span
// if it is a kafka.Message event type and also runs the given handler.
func (c *Consumer) PollWithHandler(timeoutMs int, handler ConsumeFunc) kafka.Event {
	event := c.Consumer.Poll(timeoutMs)

	switch ev := event.(type) {
	case *kafka.Message:
		msg := ev
		if msg != nil {
			s, ctx := c.startSpan(internal.OperationConsume, msg)
			err := handler(c.Consumer, msg, ctx)
			endSpan(s, err)
		}
	}

	return event
}
