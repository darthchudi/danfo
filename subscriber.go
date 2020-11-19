package danfo

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

// Subscription describes a call to consume messages from a queue, either directly via `.Consume()`
// or through an exchange with `.On()`.
// It is an abstraction that allows us to continue streaming messages from a queue after we disconnect from the
// RabbitMQ broker (because of an error) and successfully reconnect.
//
// It holds the config options used to originally subscribe to messages on the queue and the channel returned to the caller
// of a subscription (`.Consume()`, `.On()`)
type Subscription struct {
	channel         *amqp.Channel
	messages        chan amqp.Delivery
	config          *SubscribeConfig
	declareExchange bool
}

// stream reads messages from a source channel into an internal channel until the source
// is closed, typically due to an exception/error on the connection
func (s Subscription) stream(src <-chan amqp.Delivery) {
	for message := range src {
		s.messages <- message
	}
}

// Subscriber listens for messages on a RabbitMQ Queue
type Subscriber struct {
	Connection          *amqp.Connection
	NotifyCloseListener chan *amqp.Error
	IsConnected         bool
	ReconnectionDelay   time.Duration
	subscriptions       []*Subscription
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

	// TODO: write description
	bindingKey string
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

// restartSubscriptions continues streaming messages on all subscriptions on the subscriber after
// a successful reconnection
func (s *Subscriber) restartSubscriptions() error {
	for _, subscription := range s.subscriptions {
		// Create a new channel scoped to the subscription
		channel, err := s.Connection.Channel()

		if err != nil{
			return err
		}

		_, err = channel.QueueDeclare(
			subscription.config.queueConfig.name,
			subscription.config.queueConfig.durable,
			subscription.config.queueConfig.autoDelete,
			subscription.config.queueConfig.exclusive,
			subscription.config.queueConfig.noWait,
			subscription.config.queueConfig.args,
		)

		if err != nil {
			return err
		}

		// If the subscription consumes messages from the queue via a named exchange i.e
		// with a call to `On()`, we will need to redeclare the exchange and bind the queue to
		// the exchange
		if subscription.declareExchange {
			err = channel.ExchangeDeclare(
				subscription.config.exchangeConfig.name,
				subscription.config.exchangeConfig.exchangeType,
				subscription.config.exchangeConfig.durable,
				subscription.config.exchangeConfig.autoDelete,
				subscription.config.exchangeConfig.internal,
				subscription.config.exchangeConfig.noWait,
				subscription.config.exchangeConfig.args,
			)

			if err != nil {
				return err
			}

			// Bind the queue to the exchange using the binding key
			err = channel.QueueBind(
				subscription.config.queueConfig.name,
				subscription.config.bindingKey,
				subscription.config.exchangeConfig.name,
				false,
				nil,
			)

			if err != nil {
				return err
			}
		}

		// Consume from the queue
		src, err := channel.Consume(
			subscription.config.queueConfig.name,
			subscription.config.consumer,
			subscription.config.autoAck,
			subscription.config.exclusive,
			false,
			subscription.config.noWait,
			subscription.config.args,
		)

		if err != nil {
			return err
		}

		// Update the subscription's channel
		subscription.channel = channel

		// Resume streaming messages from a source messages channel
		go subscription.stream(src)
	}

	return nil
}

// NewSubscriber creates a new subscriber which listens for messages on queues
func NewSubscriber(url string) (*Subscriber, error) {
	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}

	subscriber := &Subscriber{
		Connection:          connection,
		NotifyCloseListener: make(chan *amqp.Error),
		IsConnected:         true,
		ReconnectionDelay:   time.Second * 10,
	}

	// Register a listener for exceptions thrown on the connection by the server
	_ = subscriber.Connection.NotifyClose(subscriber.NotifyCloseListener)

	go subscriber.reconnectOnConnectionError(url)

	return subscriber, nil
}

// reconnectOnConnectionError listens for connection errors/exceptions from the server and
// attempts to reconnect to the server
func (s *Subscriber) reconnectOnConnectionError(url string) {
	<-s.NotifyCloseListener

	s.IsConnected = false

	log.Printf("\n🌻Connection Exception: %+v\n", s)

	// Loop infinitely until we reconnect
	for {
		connection, err := amqp.Dial(url)

		if err != nil {
			log.Printf("danfo: reconnection failed: %v", err)
			log.Printf("Sleeping for %v seconds, before retrying", s.ReconnectionDelay.String())
			time.Sleep(s.ReconnectionDelay)
			continue
		}

		// Update connection and close listener
		s.Connection = connection
		s.NotifyCloseListener = make(chan *amqp.Error)
		s.Connection.NotifyClose(s.NotifyCloseListener)
		s.IsConnected = true

		log.Print("Subscriber reconnected")

		// Restart subscriptions with the new connection
		err = s.restartSubscriptions()

		if err != nil {
			log.Printf("danfo: could not restart subscriptions: %v", err)
		}

		break
	}

	// Schedule a new reconnection goroutine on the new connection after a successful reconnection
	go s.reconnectOnConnectionError(url)
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


	// Create a new channel scoped to the subscription/consumer
	channel, err := s.Connection.Channel()

	if err != nil {
		return nil, err
	}

	_, err = channel.QueueDeclare(
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

	src, err := channel.Consume(
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

	// Create a new subscription registered on the subscriber
	subscription := &Subscription{
		messages:        make(chan amqp.Delivery),
		channel: channel,
		config:          config,
		declareExchange: false,
	}
	s.subscriptions = append(s.subscriptions, subscription)

	go subscription.stream(src)

	return subscription.messages, nil
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
		bindingKey:     bindingKey,
		consumer:       "",
		autoAck:        false,
		exclusive:      false,
		noWait:         false,
		args:           nil,
	}

	for _, configSetter := range configSetters {
		configSetter(config)
	}

	// Create a new channel scoped to the subscription/consumer
	channel, err := s.Connection.Channel()

	if err != nil {
		return nil, err
	}

	_, err = channel.QueueDeclare(
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

	err = channel.ExchangeDeclare(
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
	err = channel.QueueBind(
		config.queueConfig.name,
		config.bindingKey,
		config.exchangeConfig.name,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	src, err := channel.Consume(
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

	// Create a new subscription registered on the subscriber
	subscription := &Subscription{
		messages:        make(chan amqp.Delivery),
		channel:         channel,
		config:          config,
		declareExchange: true,
	}
	s.subscriptions = append(s.subscriptions, subscription)

	go subscription.stream(src)

	return subscription.messages, nil
}
