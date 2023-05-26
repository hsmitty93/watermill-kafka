package kafka

import (
	"context"
	"crypto/tls"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

// NewSubscriber creates a new Kafka Subscriber.
func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	return &Subscriber{
		config: config,
		logger: logger,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler Unmarshaler

	// OverwriteReaderConfig holds additional Reader settings.
	OverwriteReaderConfig *kafka.ReaderConfig

	// A transport used to send messages to kafka clusters. Defaults to kafka.DefaultTransport.
	Dialer *kafka.Dialer

	// A transport used to send messages to kafka clusters. Defaults to kafka.DefaultTransport.
	Transport *kafka.Transport

	// If true then each sent message will be wrapped with Datadog tracing, provided by dd-trace-go.
	TracingEnabled bool

	// Kafka consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration

	InitializeTopicDetails *kafka.TopicConfig
}

// NoSleep can be set to SubscriberConfig.NackResendSleep and SubscriberConfig.ReconnectRetrySleep.
const NoSleep time.Duration = -1

func (c *SubscriberConfig) setDefaults() {
	if c.Dialer == nil {
		c.Dialer = &kafka.Dialer{
			TLS:       &tls.Config{},
			DualStack: true,
		}
	}

	if c.Transport == nil {
		c.Transport = &kafka.Transport{
			TLS: &tls.Config{},
		}
	}

	if c.OverwriteReaderConfig == nil {
		c.OverwriteReaderConfig = &kafka.ReaderConfig{
			Brokers: c.Brokers,
			Dialer:  c.Dialer,
			GroupID: c.ConsumerGroup,
		}
	} else {
		c.OverwriteReaderConfig.Brokers = c.Brokers
		c.OverwriteReaderConfig.Dialer = c.Dialer
		c.OverwriteReaderConfig.GroupID = c.ConsumerGroup
	}
	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaler")
	}

	return nil
}

// Subscribe subscribers for messages in Kafka.
//
// There are multiple subscribers spawned
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		// blocking, until s.closing is closed
		s.handleReconnects(ctx, topic, output, consumeClosed, logFields)
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) handleReconnects(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	consumeClosed chan struct{},
	logFields watermill.LogFields,
) {
	for {
		// nil channel will cause deadlock
		if consumeClosed != nil {
			<-consumeClosed
			s.logger.Debug("consumeMessages stopped", logFields)
		} else {
			s.logger.Debug("empty consumeClosed", logFields)
		}

		select {
		// it's important to don't exit before consumeClosed,
		// to not trigger s.subscribersWg.Done() before consumer is closed
		case <-s.closing:
			s.logger.Debug("Closing subscriber, no reconnect needed", logFields)
			return
		case <-ctx.Done():
			s.logger.Debug("Ctx cancelled, no reconnect needed", logFields)
			return
		default:
			s.logger.Debug("Not closing, reconnecting", logFields)
		}

		s.logger.Info("Reconnecting consumer", logFields)

		var err error
		consumeClosed, err = s.consumeMessages(ctx, topic, output, logFields)
		if err != nil {
			s.logger.Error("Cannot reconnect messages consumer", err, logFields)

			if s.config.ReconnectRetrySleep != NoSleep {
				time.Sleep(s.config.ReconnectRetrySleep)
			}
			continue
		}
	}
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (consumeMessagesClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	// Set Readers topic
	s.config.OverwriteReaderConfig.Topic = topic

	// Start with a reader
	reader := kafka.NewReader(*s.config.OverwriteReaderConfig)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
			// avoid goroutine leak
		}
	}()

	consumeMessagesClosed, err = s.consumeFromReader(ctx, reader, output, logFields)

	if err != nil {
		s.logger.Debug(
			"Starting consume failed, cancelling context",
			logFields.Add(watermill.LogFields{"err": err}),
		)
		cancel()
		return nil, err
	}

	go func() {
		<-consumeMessagesClosed
		if err := reader.Close(); err != nil {
			s.logger.Error("Cannot close reader", err, logFields)
		} else {
			s.logger.Debug("reader closed", logFields)
		}
	}()

	return consumeMessagesClosed, nil
}

func (s *Subscriber) consumeFromReader(ctx context.Context, reader *kafka.Reader, output chan *message.Message, logFields watermill.LogFields) (chan struct{}, error) {
	messageHandler := s.createMessagesHandler(output)

	readerClosed := make(chan struct{})

	go func() {
		for {
			select {
			default:
				s.logger.Debug("Not closing", logFields)
			case <-s.closing:
				s.logger.Debug("Subscriber is closing, stopping group.Consume loop", logFields)
				break
			case <-ctx.Done():
				s.logger.Debug("Ctx was cancelled, stopping group.Consume loop", logFields)
				break
			}

			for {
				kafkaMessage, err := reader.FetchMessage(ctx)
				if errors.Is(err, context.Canceled) {
					close(readerClosed)
					return

				} else if err == io.EOF {
					// end of input (broker closed connection, etc)
					s.logger.Error("EOF received, stopping listener", err, logFields)
					close(readerClosed)
					return

				} else if err != nil {
					kerr, ok := err.(kafka.Error)
					if !ok {
						// unexpected (for now), bail
						s.logger.Error("unexpected Kafka error", err, logFields)
						return
					}

					logFields.Add(watermill.LogFields{
						"kafka_error_code": int(kerr),
						"kafka_error":      kerr.Title(),
						"retryable":        kerr.Temporary(),
					})

					// https://kafka.apache.org/protocol#protocol_error_codes
					s.logger.Error("failed to fetch message", kerr, logFields)

					close(readerClosed)
					return

				} else {
					if err := messageHandler.processMessage(ctx, reader, &kafkaMessage, logFields); err != nil {
						s.logger.Error("failed to process message", err, logFields)
						return
					}
				}
				// this is expected behaviour to run Consume again after it exited
				// see: https://github.com/ThreeDotsLabs/watermill/issues/210
				s.logger.Debug("Consume stopped without any error, running consume again", logFields)
			}

		}
	}()

	return readerClosed, nil
}

func (s *Subscriber) createMessagesHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel:   output,
		unmarshaler:     s.config.Unmarshaler,
		nackResendSleep: s.config.NackResendSleep,
		logger:          s.logger,
		closing:         s.closing,
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("Kafka subscriber closed", nil)

	return nil
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	unmarshaler   Unmarshaler

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h messageHandler) processMessage(
	ctx context.Context,
	reader *kafka.Reader,
	kafkaMsg *kafka.Message,
	messageLogFields watermill.LogFields,
) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"kafka_partition_offset": kafkaMsg.Offset,
		"kafka_partition":        kafkaMsg.Partition,
	})

	h.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	ctx = setPartitionToCtx(ctx, kafkaMsg.Partition)
	ctx = setPartitionOffsetToCtx(ctx, kafkaMsg.Offset)
	ctx = setMessageTimestampToCtx(ctx, kafkaMsg.Time)

	msg, err := h.unmarshaler.Unmarshal(kafkaMsg)
	if err != nil {
		// resend will make no sense, stopping consumerGroupHandler
		return errors.Wrap(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case h.outputChannel <- msg:
			h.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			err := reader.CommitMessages(ctx, *kafkaMsg)
			if err == nil {
				h.logger.Trace("Message Acked", receivedMsgLogFields)
				break ResendLoop
			}
			h.logger.Error("failed to commit message", err, receivedMsgLogFields)
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", receivedMsgLogFields)

			// reset acks, etc.
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	if s.config.InitializeTopicDetails == nil {
		return errors.New("s.config.InitializeTopicDetails is empty, cannot SubscribeInitialize")
	}

	s.config.InitializeTopicDetails.Topic = topic

	client := &kafka.Client{
		Addr:      kafka.TCP(s.config.Brokers...),
		Transport: s.config.Transport,
	}

	req := &kafka.CreateTopicsRequest{
		Addr:   kafka.TCP(s.config.Brokers...),
		Topics: []kafka.TopicConfig{*s.config.InitializeTopicDetails},
	}

	if _, err := client.CreateTopics(context.Background(), req); err != nil {
		return errors.Wrap(err, "cannot create topic")
	}

	s.logger.Info("Created Kafka topic", watermill.LogFields{"topic": topic})

	return nil
}

type PartitionOffset map[int]int64

func (s *Subscriber) PartitionOffset(topic string) (PartitionOffset, error) {
	client := &kafka.Client{
		Addr:      kafka.TCP(s.config.Brokers...),
		Transport: s.config.Transport,
	}

	partitions, err := kafka.LookupPartitions(context.Background(), "tcp", s.config.Brokers[0], topic)
	if err != nil {
		return nil, err
	}

	partitionOffset := make(PartitionOffset, len(partitions))
	for _, partition := range partitions {
		req := &kafka.OffsetFetchRequest{
			Addr:    kafka.TCP(s.config.Brokers...),
			GroupID: s.config.ConsumerGroup,
			Topics: map[string][]int{
				topic: {partition.ID},
			},
		}
		res, err := client.OffsetFetch(context.Background(), req)
		if err != nil {
			return nil, err
		}

		partitionOffset[partition.ID] = res.Topics[topic][0].CommittedOffset
	}

	return partitionOffset, nil
}
