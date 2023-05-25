package kafka

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to Kafka message.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*kafka.Message, error)
}

// Unmarshaler unmarshals Kafka's message to Watermill's message.
type Unmarshaler interface {
	Unmarshal(*kafka.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*kafka.Message, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	headers := []kafka.Header{{
		Key:   UUIDHeaderKey,
		Value: []byte(msg.UUID),
	}}
	for key, value := range msg.Metadata {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	return &kafka.Message{
		Topic:   topic,
		Value:   []byte(msg.Payload),
		Headers: headers,
	}, nil
}

func (DefaultMarshaler) Unmarshal(kafkaMsg *kafka.Message) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(kafkaMsg.Headers))

	for _, header := range kafkaMsg.Headers {
		if string(header.Key) == UUIDHeaderKey {
			messageID = string(header.Value)
		} else {
			metadata.Set(string(header.Key), string(header.Value))
		}
	}

	msg := message.NewMessage(messageID, kafkaMsg.Value)
	msg.Metadata = metadata

	return msg, nil
}

type GeneratePartitionKey func(topic string, msg *message.Message) (string, error)

type kafkaJsonWithPartitioning struct {
	DefaultMarshaler

	generatePartitionKey GeneratePartitionKey
}

func NewWithPartitioningMarshaler(generatePartitionKey GeneratePartitionKey) MarshalerUnmarshaler {
	return kafkaJsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j kafkaJsonWithPartitioning) Marshal(topic string, msg *message.Message) (*kafka.Message, error) {
	kafkaMsg, err := j.DefaultMarshaler.Marshal(topic, msg)
	if err != nil {
		return nil, err
	}

	key, err := j.generatePartitionKey(topic, msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate partition key")
	}
	kafkaMsg.Key = []byte(key)

	return kafkaMsg, nil
}
