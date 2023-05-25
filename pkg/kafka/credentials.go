package kafka

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	DefaultDialerTimeout = 30 * time.Second
)

// KafkaCredentials represents the username (API key) and password (API secret)
// used for authenticating with Kafka broker(s) for reading and writing topics.
type KafkaCredentials struct {
	AppName  string
	Username string
	Password string
}

// Dialer returns a Kafka dialer suitable for connecting to a broker with the given credentials.
//
// If the credentials are nil, or if neither a username nor password is present,
// then a default, unauthenticated dialer is returned.
func (c *KafkaCredentials) Dialer() *kafka.Dialer {
	clientID := strings.ReplaceAll(uuid.NewString(), "-", "")
	if c != nil {
		clientID = fmt.Sprintf("%s-%s", c.AppName, clientID)
	}

	dialer := &kafka.Dialer{
		ClientID:  clientID,
		DualStack: true,
		Timeout:   DefaultDialerTimeout,
	}

	if c != nil && (c.Username != "" || c.Password != "") {
		dialer.SASLMechanism = plain.Mechanism{
			Username: c.Username,
			Password: c.Password,
		}
		dialer.TLS = &tls.Config{}
	}

	return dialer
}

// Transport returns a Kafka transport suitable for connecting to a broker with the given credentials.
//
// If the credentials are nil, or if neither a username nor password is present,
// then a transport utilizing a default, unauthenticated dialer is returned.
func (c *KafkaCredentials) Transport() kafka.RoundTripper {
	return &kafka.Transport{
		Dial: c.Dialer().DialFunc,
	}
}
