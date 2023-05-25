package kafka

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	ktrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/segmentio/kafka.go.v0"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup
	dialer        *kafka.Dialer

	closed bool
}

// NewSubscriber creates a new Kafka Subscriber.
func (c *KafkaClient) NewSubscriber(
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
		dialer: c.credentials.Dialer(),

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler Unmarshaler

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

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: s.config.Brokers,
		Dialer:  s.dialer,
		Topic:   topic,
	})

	tracedReader := ktrace.WrapReader(kafkaReader,
		ktrace.WithServiceName("watermill-kafka"),
		ktrace.WithAnalytics(true))

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message)

	s.consumeMessages(ctx, tracedReader, topic, output, logFields)

	go func() {
		// blocking, until s.closing is closed
		s.handleReconnects(ctx, tracedReader, topic, output, logFields)
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) handleReconnects(
	ctx context.Context,
	reader *ktrace.Reader,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	for {
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
		s.consumeMessages(ctx, reader, topic, output, logFields)
	}
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	reader *ktrace.Reader,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	s.logger.Info("Starting consuming", logFields)

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

	go s.consume(ctx, reader, topic, output, logFields)
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

func (s *Subscriber) consume(
	ctx context.Context,
	reader *ktrace.Reader,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	messageHandler := s.createMessagesHandler(output)

	for {
		select {
		default:
			s.logger.Debug("Not closing", logFields)
		case <-s.closing:
			s.logger.Debug("Subscriber is closing, stopping group.Consume loop", logFields)
			return
		case <-ctx.Done():
			s.logger.Debug("Ctx was cancelled, stopping group.Consume loop", logFields)
			return
		}

		kafkaMessage, err := reader.FetchMessage(ctx)
		if errors.Is(err, context.Canceled) {
			return

		} else if err == io.EOF {
			// end of input (broker closed connection, etc)
			s.logger.Error("EOF received, stopping listener", err, logFields)
			return

		} else if err != nil {
			err, ok := err.(kafka.Error)
			if !ok {
				// unexpected (for now), bail
				s.logger.Error("unexpected Kafka error", err, logFields)
				return
			}

			// https://kafka.apache.org/protocol#protocol_error_codes
			s.logger.Error("failed to fetch message", err,
				logFields)

			reader.Close()

		} else {
			if err := messageHandler.processMessage(ctx, kafkaMessage, reader, logFields); err != nil {
				return
			}
		}

		// this is expected behaviour to run Consume again after it exited
		// see: https://github.com/ThreeDotsLabs/watermill/issues/210
		s.logger.Debug("Consume stopped without any error, running consume again", logFields)
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
	kafkaMsg kafka.Message,
	reader *ktrace.Reader,
	messageLogFields watermill.LogFields,
) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"kafka_partition_offset": kafkaMsg.Offset,
		"kafka_partition":        kafkaMsg.Partition,
	})

	h.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	ctx = setPartitionToCtx(ctx, int32(kafkaMsg.Partition))
	ctx = setPartitionOffsetToCtx(ctx, kafkaMsg.Offset)
	ctx = setMessageTimestampToCtx(ctx, kafkaMsg.Time)

	msg, err := h.unmarshaler.Unmarshal(&kafkaMsg)
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
			if reader != nil {
				reader.CommitMessages(ctx, kafkaMsg)
			}
			h.logger.Trace("Message Acked", receivedMsgLogFields)
			break ResendLoop
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

func (s *Subscriber) SubscribeInitialize() (err error) {
	if s.config.InitializeTopicDetails == nil {
		return errors.New("s.config.InitializeTopicDetails is empty, cannot SubscribeInitialize")
	}

	conn, err := s.dialer.Dial("tcp", s.config.Brokers[0])
	if err != nil {
		return errors.Wrap(err, "cannot create connect")
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(*s.config.InitializeTopicDetails)
	if err != nil {
		panic(err.Error())
	}

	s.logger.Info("Created Kafka topic", watermill.LogFields{"topic": s.config.InitializeTopicDetails.Topic})

	return nil
}
