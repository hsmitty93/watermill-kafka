package kafka

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config      PublisherConfig
	credentials *KafkaCredentials
	logger      watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new Kafka Publisher.
func (c *KafkaClient) NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		config:      config,
		credentials: c.credentials,
		logger:      logger,
	}, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

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

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(p.config.Brokers...),
		AllowAutoTopicCreation: true,
		Topic:                  topic,
		Transport:              p.credentials.Transport(),
	}

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		err = kafkaWriter.WriteMessages(context.Background(), *kafkaMsg)
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

	return nil
}
