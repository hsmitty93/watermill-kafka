package kafka

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/hamba/avro"
	"github.com/hamba/avro/registry"
	"golang.org/x/exp/slog"
)

type Client interface {
	FetchSchema(subject string) (avro.Schema, error)
	RegisterSchema(subject, content string) (avro.Schema, error)
	NewSubscriber(
		config SubscriberConfig,
		logger watermill.LoggerAdapter,
	) (*Subscriber, error)
}

type KafkaClient struct {
	appName     string
	brokers     []string
	credentials *KafkaCredentials
	log         *slog.Logger
	registry    registry.Registry
	schemas     *avro.SchemaCache
}

var _ Client = (*KafkaClient)(nil)

func NewClient(appName string, brokers []string, opts ...KafkaClientOption) (*KafkaClient, error) {
	if appName == "" {
		return nil, errors.New("app name cannot not be blank")
	}

	if len(brokers) == 0 {
		return nil, errors.New("must provide at least one broker address")
	}

	client := &KafkaClient{
		appName: appName,
		brokers: brokers,
		credentials: &KafkaCredentials{
			AppName: appName,
		},
		log:     slog.Default().WithGroup("pubsub"),
		schemas: avro.DefaultSchemaCache,
	}

	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}

	return client, nil
}

type KafkaClientOption func(*KafkaClient) error

// WithBrokerCredentials sets the SASL authentication credentials
// to use when connecting to brokers.
func WithBrokerCredentials(creds *KafkaCredentials) KafkaClientOption {
	return func(c *KafkaClient) error {
		c.credentials = creds
		return nil
	}
}

// WithSchemaRegistryCredentials sets the HTTP basic authentication credentials
// to use when making API calls to the schema registry.
func WithSchemaRegistry(url, username, password string) KafkaClientOption {
	return func(c *KafkaClient) error {
		opts := []registry.ClientFunc{}
		if username != "" || password != "" {
			opts = append(opts, registry.WithBasicAuth(username, password))
		}

		reg, err := registry.NewClient(url, opts...)
		if err != nil {
			return err
		}

		c.registry = reg
		return nil
	}
}

// WithLogger sets the logger to use.
func WithLogger(logger *slog.Logger) KafkaClientOption {
	return func(c *KafkaClient) error {
		c.log = logger
		return nil
	}
}

func (c *KafkaClient) FetchSchema(subject string) (avro.Schema, error) {
	if c.registry == nil {
		return nil, errors.New("no schema registry configured")
	}

	schema, err := c.registry.GetLatestSchema(subject)
	if err != nil {
		c.log.Error(err.Error())
		switch vErr := err.(type) {
		case registry.Error:
			if vErr.StatusCode == http.StatusNotFound {
				return nil, nil
			}
		default:
			return nil, fmt.Errorf("failed to look up schema in registry: %w", err)
		}
	}

	return schema, nil
}

func (c *KafkaClient) RegisterSchema(subject, content string) (avro.Schema, error) {
	if c.registry == nil {
		return nil, errors.New("no schema registry configured")
	}

	_, schema, err := c.registry.CreateSchema(subject, content)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (c *KafkaClient) getSchema(subject string) (avro.Schema, error) {
	schema := c.schemas.Get(subject)
	if schema != nil {
		return schema, nil
	}

	if c.registry == nil {
		return nil, errors.New("no schema registry configured")
	}

	schema, err := c.registry.GetLatestSchema(subject)
	if err != nil {
		c.log.Error("fetching schema", "subject", subject, "error", err)
		return nil, fmt.Errorf("fetching schema %q: %w", subject, err)
	}

	c.schemas.Add(subject, schema)
	return schema, nil
}
