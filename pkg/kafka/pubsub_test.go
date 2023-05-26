package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	goKafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9091", "localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"}
}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) (*kafka.Publisher, *kafka.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *kafka.Publisher

	writerConfig := &goKafka.Writer{
		//RequiredAcks: -1,
		//BatchSize: 10240,
	}

	retriesLeft := 5
	for {
		publisher, err = kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:         kafkaBrokers(),
			Marshaler:       marshaler,
			OverwriteWriter: writerConfig,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	readerConfig := &goKafka.ReaderConfig{
		HeartbeatInterval: time.Millisecond * 500,
		RebalanceTimeout:  time.Second * 3,
		QueueCapacity:     10240,
		StartOffset:       goKafka.FirstOffset,
	}

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           marshaler,
				OverwriteReaderConfig: readerConfig,
				ConsumerGroup:         consumerGroup,
				InitializeTopicDetails: &goKafka.TopicConfig{
					NumPartitions:     8,
					ReplicationFactor: 1,
				},
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return msg.Metadata.Get("partition_key"), nil
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGrup(t, "test")
}

func createPartitionedPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test")
}

func createNoGroupPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, "")
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                   false,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		createNoGroupPubSub,
		nil,
	)
}

func TestCtxValues(t *testing.T) {
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	for i := 0; i < 20; i++ {
		id := watermill.NewUUID()
		messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
	}
	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	expectedPartitionsOffsets := map[int32]int64{}
	for _, msg := range receivedMessages {
		partition, ok := kafka.MessagePartitionFromCtx(msg.Context())
		assert.True(t, ok)

		messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(msg.Context())
		assert.True(t, ok)

		kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(msg.Context())
		assert.True(t, ok)
		assert.NotZero(t, kafkaMsgTimestamp)

		if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
			// kafka partition offset is offset of the last message + 1
			expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
		}
	}
	assert.NotEmpty(t, expectedPartitionsOffsets)

	offsets, err := sub.PartitionOffset(topicName)
	require.NoError(t, err)
	assert.NotEmpty(t, offsets)

	assert.EqualValues(t, expectedPartitionsOffsets, offsets)

	require.NoError(t, pub.Close())
}
