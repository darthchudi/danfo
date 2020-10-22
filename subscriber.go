package danfo

import (
	"github.com/streadway/amqp"
)

// Subscriber listens for messages on a RabbitMQ Queue
type Subscriber struct {
	connection *amqp.Connection
	channel    *amqp.Channel
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

// Ingester is a function/callback that receives RabbitMQ deliveries and handles them
type Ingester func(delivery amqp.Delivery)

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
		connection: connection,
		channel:    channel,
	}

	return subscriber, nil
}

// Consume listens for messages on a work queue
// The assumption behind a work queue is that each task is delivered to exactly one queue
// We listen for messages using the default exchange, on which every queue is automatically bound to,
// with the queue's name as the binding key
func (s *Subscriber) Consume(
	queueName string,
	ingester Ingester,
	configSetters ...SubscribeConfigSetter,
) error {
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

	_, err := s.channel.QueueDeclare(
		config.queueConfig.name,
		config.queueConfig.durable,
		config.queueConfig.autoDelete,
		config.queueConfig.exclusive,
		config.queueConfig.noWait,
		config.queueConfig.args,
	)

	if err != nil {
		return err
	}

	messages, err := s.channel.Consume(
		config.queueConfig.name,
		config.consumer,
		config.autoAck,
		config.exclusive,
		false,
		config.noWait,
		config.args,
	)

	if err != nil {
		return err
	}

	// Spin up a dedicated go routine for reading the messages until the channel closes
	go func() {
		for message := range messages {
			// invoke the ingester in a separate goroutine so that
			// each ingester runs in isolation and doesn't block new
			// messages from being read
			go ingester(message)
		}
	}()

	return nil
}

// On listens for broadcast messages sent to a queue, routed via a specified exchange
// It allows us to listen for messages based on a provided pattern (binding key)
// Internally, it is bound to a `topic` exchange via the specified binding key
// Reference: https://www.rabbitmq.com/tutorials/tutorial-five-go.html
func (s *Subscriber) On(
	exchangeName string,
	bindingKey string,
	queueName string,
	ingester Ingester,
	configSetters ...SubscribeConfigSetter,
) error {
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

	_, err := s.channel.QueueDeclare(
		config.queueConfig.name,
		config.queueConfig.durable,
		config.queueConfig.autoDelete,
		config.queueConfig.exclusive,
		config.queueConfig.noWait,
		config.queueConfig.args,
	)

	if err != nil {
		return err
	}

	err = s.channel.ExchangeDeclare(
		config.exchangeConfig.name,
		config.exchangeConfig.exchangeType,
		config.exchangeConfig.durable,
		config.exchangeConfig.autoDelete,
		config.exchangeConfig.internal,
		config.exchangeConfig.noWait,
		config.exchangeConfig.args,
	)

	if err != nil {
		return err
	}

	// Bind the queue to the exchange using the binding key
	err = s.channel.QueueBind(
		config.queueConfig.name,
		bindingKey,
		config.exchangeConfig.name,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	messages, err := s.channel.Consume(
		config.queueConfig.name,
		config.consumer,
		config.autoAck,
		config.exclusive,
		false,
		config.noWait,
		config.args,
	)

	if err != nil {
		return err
	}

	// Spin up a dedicated go routine for iterating through the messages channel, until the channel closes
	go func() {
		for message := range messages {
			// invoke the ingester in a separate goroutine so that
			// each ingester runs in isolation and doesn't block new
			// messages from being read
			go ingester(message)
		}
	}()

	return nil
}
