package kafka

import (
	"context"
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config   PublisherConfig
	producer *kafka.Writer
	logger   watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}
	//config.OverwriteWriter.Logger = logger

	producer := config.OverwriteWriter

	/* if config.TracingEnabled {
		producer = ktrace.WrapWriter(producer,
			ktrace.WithServiceName(config.ServiceName),
			ktrace.WithAnalytics(true))
	} */

	return &Publisher{
		config:   config,
		producer: producer,
		logger:   logger,
	}, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteWriter holds additional writer settings.
	OverwriteWriter *kafka.Writer

	// A transport used to send messages to kafka clusters. Defaults to kafka.DefaultTransport.
	Transport *kafka.Transport

	// If true then each sent message will be wrapped with Datadog tracing, provided by dd-trace-go.
	//TracingEnabled bool

	// Name of the service, needed for tracing
	ServiceName string
}

func (c *PublisherConfig) setDefaults() {
	if c.Transport == nil {
		c.Transport = &kafka.Transport{
			TLS: &tls.Config{},
		}
	}
	if c.OverwriteWriter == nil {
		c.OverwriteWriter = &kafka.Writer{
			Addr:                   kafka.TCP(c.Brokers...),
			AllowAutoTopicCreation: false,
			Transport:              c.Transport,
		}
	} else {
		c.OverwriteWriter.Addr = kafka.TCP(c.Brokers...)
		c.OverwriteWriter.AllowAutoTopicCreation = false
		c.OverwriteWriter.Transport = c.Transport
	}
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}
	/* if c.TracingEnabled && c.ServiceName == "" {
		return errors.New("missing service name")
	} */

	return nil
}

// Publish publishes message to Kafka.
//
// Publish is blocking and wait for ack from Kafka.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	ctx := context.Background()

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		err = p.producer.WriteMessages(ctx, *kafkaMsg)
		if err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}
