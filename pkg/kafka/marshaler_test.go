package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(marshaled)
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		_, _ = m.Marshal("foo", msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, _ = m.Unmarshal(marshaled)
	}
}

func TestWithPartitioningMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
		return msg.Metadata.Get("partition"), nil
	})

	partitionKey := "1"
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("partition", partitionKey)

	producerMsg, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(producerMsg)
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))

	assert.NoError(t, err)

	producerKey := producerMsg.Key
	require.NoError(t, err)

	assert.Equal(t, string(producerKey), partitionKey)
}
