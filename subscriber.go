package danfo

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

// Subscriber listens for messages on a RabbitMQ Queue
type Subscriber struct {
	Connection          *amqp.Connection
	Channel             *amqp.Channel
	NotifyCloseListener chan *amqp.Error
	IsConnected         bool
	ReconnectionDelay   time.Duration
}

// SubscribeConfig describes the options for subscribing for messages
type SubscribeConfig struct {
	// Describes the queue configuration
	queueConfig AMQPQueueConfig

	// Describes the exchange configuration
	exchangeConfig AMQPExchangeConfig

	// The consumer is identified by a string that is unique and scoped for all
	// consumers on this channel. An empty string will cause
	// the library to generate a unique identity.
	consumer string

	// When autoAck (also known as noAck) is true, the server will acknowledge
	// deliveries to this consumer prior to writing the delivery to the network. When
	// autoAck is true, the consumer should not call Delivery.Ack. Automatically
	// acknowledging deliveries means that some deliveries may get lost if the
	// consumer is unable to process them after the server delivers them.
	// See http://www.rabbitmq.com/confirms.html for more details.
	autoAck bool

	// When exclusive is true, the server will ensure that this is the sole consumer
	// from this queue. When exclusive is false, the server will fairly distribute
	// deliveries across multiple consumers.
	exclusive bool

	// When noWait is true, do not wait for the server to confirm the request and
	// immediately begin deliveries.  If it is not possible to consume, a channel
	// exception will be raised and the channel will be closed.
	noWait bool

	// Optional arguments can be provided that have specific semantics for the queue
	// or server.
	args amqp.Table
}

// SubscribeConfigSetter sets fields in a subscribe config
type SubscribeConfigSetter func(sConfig *SubscribeConfig)

// NonDurableSubscribeQueue declares a non durable queue on a subscribe config
// A non-durable queue will not survive server restarts
func NonDurableSubscribeQueue(sConfig *SubscribeConfig) {
	sConfig.queueConfig.durable = false
}

// AutoDeletedSubscribeQueue declares an auto-deleted queue on a subscribe config.
// An auto-deleted queue will be deleted when there are no remaining consumers or binding
func AutoDeletedSubscribeQueue(sConfig *SubscribeConfig) {
	sConfig.queueConfig.autoDelete = true
}

// ExclusiveSubscribeQueue declares an Exclusive queue on a subscribe config
// An exclusive queue cannot be accessed by other connections asides
// from the connection that declares them
func ExclusiveSubscribeQueue(sConfig *SubscribeConfig) {
	sConfig.queueConfig.exclusive = true
}

// NoWaitSubscribeQueue declares sets the `noWait` option to true. When set
// the server will not respond to the declare queue call.
// A channel exception will arrive if the conditions are met
// for existing queues or attempting to modify an existing queue from
// a different connection.
func NoWaitSubscribeQueue(sConfig *SubscribeConfig) {
	sConfig.queueConfig.noWait = true
}

// SubscribeQueueArguments sets the set of arguments for the queue declaration
func SubscribeQueueArguments(args amqp.Table) SubscribeConfigSetter {
	return func(sConfig *SubscribeConfig) {
		sConfig.queueConfig.args = args
	}
}

// AutoAck sets the auto-ack option on the consumer to true.
// When autoAck (also known as noAck) is true, the server will acknowledge
// deliveries to this consumer prior to writing the delivery to the network. When
// autoAck is true, the consumer should not call Delivery.Ack. Automatically
// acknowledging deliveries means that some deliveries may get lost if the
// consumer is unable to process them after the server delivers them.
// See http://www.rabbitmq.com/confirms.html for more details.
func AutoAck(sConfig *SubscribeConfig) {
	sConfig.autoAck = true
}

// ExclusiveSubscribe declares an exclusive consumer/subscriber.
// When exclusive is true, the server will ensure that this is the sole consumer
// from the declared queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func ExclusiveSubscribe(sConfig *SubscribeConfig) {
	sConfig.exclusive = true
}

// NoWaitSubscribe sets the noWait option on the consumer/subscriber
// When noWait is true, do not wait for the server to confirm the request and
// immediately begin deliveries.  If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func NoWaitSubscribe(sConfig *SubscribeConfig) {
	sConfig.noWait = true
}

// ConsumerTag sets the consumer tag string
// The consumer is identified by a string that is unique and scoped for all
// consumers on this channel. An empty string will cause
// the library to generate a unique identity.
func ConsumerTag(consumer string) SubscribeConfigSetter {
	return func(sConfig *SubscribeConfig) {
		sConfig.consumer = consumer
	}
}

// ConsumerArguments sets the optional arguments can be provided that have specific
// semantics for the queue or server.
func ConsumerArguments(args amqp.Table) SubscribeConfigSetter {
	return func(sConfig *SubscribeConfig) {
		sConfig.args = args
	}
}

// handleSubscriberReconnection listens for connection errors on a subscriber and handles the reconnection logic
func handleSubscriberReconnection(s *Subscriber, url string) {
	<-s.NotifyCloseListener

	s.IsConnected = false

	// Loop infinitely until we reconnect
	for {
		connection, err := amqp.Dial(url)

		if err != nil {
			log.Printf("danfo: reconnection failed: %v", err)
			log.Printf("Sleeping for %v seconds, before retrying", s.ReconnectionDelay.String())
			time.Sleep(s.ReconnectionDelay)
			continue
		}

		channel, err := connection.Channel()

		if err != nil {
			log.Printf("danfo: reconnection failed: %v", err)
			log.Printf("Sleeping for %v seconds, before retrying", s.ReconnectionDelay.String())
			time.Sleep(s.ReconnectionDelay)
			continue
		}

		// Update channel, connection and listener
		s.Connection = connection
		s.Channel = channel
		s.NotifyCloseListener = make(chan *amqp.Error)
		s.Channel.NotifyClose(s.NotifyCloseListener)
		s.IsConnected = true
		break
	}

	// Schedule a new reconnection goroutine on the new connection/channel after a successful reconnection
	go handleSubscriberReconnection(s, url)
}

// NewSubscriber creates a new subscriber which listens for messages on queues
func NewSubscriber(url string) (*Subscriber, error) {
	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()

	if err != nil {
		return nil, err
	}

	subscriber := &Subscriber{
		Connection:          connection,
		Channel:             channel,
		NotifyCloseListener: make(chan *amqp.Error),
		IsConnected:         true,
		ReconnectionDelay:   time.Second * 10,
	}

	_ = channel.NotifyClose(subscriber.NotifyCloseListener)

	go handleSubscriberReconnection(subscriber, url)

	return subscriber, nil
}

// Consume listens for messages on a queue
// We listen for messages using the default exchange, on which every queue is automatically bound to,
// with the queue's name as the binding key
func (s *Subscriber) Consume(
	queueName string,
	configSetters ...SubscribeConfigSetter,
) (<-chan amqp.Delivery, error) {
	queueConfig := AMQPQueueConfig{
		name:       queueName,
		durable:    true,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}

	config := &SubscribeConfig{
		queueConfig: queueConfig,
		consumer:    "",
		autoAck:     false,
		exclusive:   false,
		noWait:      false,
		args:        nil,
	}

	for _, configSetter := range configSetters {
		configSetter(config)
	}

	_, err := s.Channel.QueueDeclare(
		config.queueConfig.name,
		config.queueConfig.durable,
		config.queueConfig.autoDelete,
		config.queueConfig.exclusive,
		config.queueConfig.noWait,
		config.queueConfig.args,
	)

	if err != nil {
		return nil, err
	}

	messages, err := s.Channel.Consume(
		config.queueConfig.name,
		config.consumer,
		config.autoAck,
		config.exclusive,
		false,
		config.noWait,
		config.args,
	)

	if err != nil {
		return nil, err
	}

	return messages, nil
}

// On listens for broadcast messages sent to a queue, routed via a specified exchange
// It allows us to listen for messages based on a provided pattern (binding key)
// Internally, it is bound to a `topic` exchange via the specified binding key
// Reference: https://www.rabbitmq.com/tutorials/tutorial-five-go.html
func (s *Subscriber) On(
	exchangeName string,
	bindingKey string,
	queueName string,
	configSetters ...SubscribeConfigSetter,
) (<-chan amqp.Delivery, error) {
	queueConfig := AMQPQueueConfig{
		name:       queueName,
		durable:    true,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}

	exchangeConfig := AMQPExchangeConfig{
		name:         exchangeName,
		exchangeType: "topic",
		durable:      true,
		autoDelete:   false,
		internal:     false,
		noWait:       false,
		args:         nil,
	}

	config := &SubscribeConfig{
		queueConfig:    queueConfig,
		exchangeConfig: exchangeConfig,
		consumer:       "",
		autoAck:        false,
		exclusive:      false,
		noWait:         false,
		args:           nil,
	}

	for _, configSetter := range configSetters {
		configSetter(config)
	}

	_, err := s.Channel.QueueDeclare(
		config.queueConfig.name,
		config.queueConfig.durable,
		config.queueConfig.autoDelete,
		config.queueConfig.exclusive,
		config.queueConfig.noWait,
		config.queueConfig.args,
	)

	if err != nil {
		return nil, err
	}

	err = s.Channel.ExchangeDeclare(
		config.exchangeConfig.name,
		config.exchangeConfig.exchangeType,
		config.exchangeConfig.durable,
		config.exchangeConfig.autoDelete,
		config.exchangeConfig.internal,
		config.exchangeConfig.noWait,
		config.exchangeConfig.args,
	)

	if err != nil {
		return nil, err
	}

	// Bind the queue to the exchange using the binding key
	err = s.Channel.QueueBind(
		config.queueConfig.name,
		bindingKey,
		config.exchangeConfig.name,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	messages, err := s.Channel.Consume(
		config.queueConfig.name,
		config.consumer,
		config.autoAck,
		config.exclusive,
		false,
		config.noWait,
		config.args,
	)

	if err != nil {
		return nil, err
	}

	return messages, nil
}

// Qos controls how many messages or how many bytes the server will try to
// keep on the network for consumers before receiving delivery acks.
func (s *Subscriber) Qos(prefetchCount, prefetchSize int, global bool) error {
	return s.Channel.Qos(prefetchCount, prefetchSize, global)
}