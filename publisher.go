package danfo

import (
	"github.com/streadway/amqp"
)

const (
	DEFAULT_AMQP_EXCHANGE = ""
)

// Publisher publishes messages to RabbitMQ
type Publisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// PublishConfig describes the options for publishing a message
type PublishConfig struct {
	// Describes the queue configuration
	queueConfig AMQPQueueConfig

	// Describes the exchange configuration
	exchangeConfig AMQPExchangeConfig

	// The tell the exchange where to route the message to
	routingKey string

	// This flag tells the server how to react if the message
	// cannot be routed to a queue. If this flag is set, the
	// server will return an unroutable message with a Return method.
	// If this flag is false, the server silently drops the message.
	mandatory bool

	//  This flag tells the server how to react if the message cannot be
	// routed to a queue consumer immediately. If this flag is set, the server
	// will return an undeliverable message with a Return method. If this flag
	// is zero, the server will queue the message, but with no guarantee that it will ever be consumed.
	immediate bool
}

// PublishConfigSetter sets fields in a publish config
type PublishConfigSetter func(config *PublishConfig)

// NonDurablePublishQueue declares a non durable queue on a publish config.
// A non-durable queue will not survive server restarts
func NonDurablePublishQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.durable = false
}

// AutoDeletedPublishQueue declares an auto-deleted queue on a publish config.
// An auto-deleted queue will be deleted when there are no remaining consumers or binding
func AutoDeletedPublishQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.autoDelete = true
}

// ExclusivePublishQueue declares an Exclusive queue on a publish config
// An exclusive queue cannot be accessed by other connections asides
// from the connection that declares them
func ExclusivePublishQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.exclusive = true
}

// NoWaitPublishQueue declares sets the `noWait` option to true. When set
// the server will not respond to the declare queue call.
// A channel exception will arrive if the conditions are met
// for existing queues or attempting to modify an existing queue from
// a different connection.
func NoWaitPublishQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.noWait = true
}

// PublishQueueArguments sets the set of arguments for the queue declaration
func PublishQueueArguments(args amqp.Table) PublishConfigSetter {
	return func(pConfig *PublishConfig) {
		pConfig.queueConfig.args = args
	}
}

// NonDurableExchange declares a non durable exchange on a publish config
// A non-durable exchange will not survive server restarts
func NonDurableExchange(pConfig *PublishConfig) {
	pConfig.exchangeConfig.durable = false
}

// AutoDeletedExchange declares an auto-deleted exchange on a publish config.
// An auto-deleted exchange will be deleted when there are no remaining consumers or binding
func AutoDeletedExchange(pConfig *PublishConfig) {
	pConfig.exchangeConfig.autoDelete = true
}

// InternalExchange declares an internal exchange on a publish config
// Internal exchanges do not accept accept publishers. Internal exchanges
// are useful when you wish to implement inter-exchange topologies
// that should not be exposed to users of the broker.
func InternalExchange(pConfig *PublishConfig) {
	pConfig.exchangeConfig.internal = true
}

// NoWaitExchange sets the `noWait` option to true. When set
// the server will not respond to the declare exchange call.
// A channel exception will arrive if the conditions are met
// for existing exchanges or attempting to modify an existing
// exchange from a different connection.
func NoWaitExchange(pConfig *PublishConfig) {
	pConfig.exchangeConfig.noWait = true
}

// ExchangeArguments sets the arguments that are sent for exchange types
// that require extra arguments. These arguments are specific to the server's
// implementation of the exchange
func ExchangeArguments(args amqp.Table) PublishConfigSetter {
	return func(pConfig *PublishConfig) {
		pConfig.exchangeConfig.args = args
	}
}

// MandatoryPublish tells the server how to react if the message cannot be
// routed to a queue. If this flag is set, the server will return an unroutable
// message with a Return method. If this flag is zero, the server silently drops the message.
func MandatoryPublish(pConfig *PublishConfig) {
	pConfig.mandatory = true
}

// ImmediatePublish tells the server how to react if the message cannot be routed to a queue consumer
// immediately. If this flag is set, the server will return an undeliverable message with a Return method.
// If this flag is zero, the server will queue the message, but with no guarantee that it will ever be consumed.
func ImmediatePublish(pConfig *PublishConfig) {
	pConfig.immediate = true
}

// NewPublisher creates a new publisher and initializes its AMQP connection and channel
func NewPublisher(url string) (*Publisher, error) {

	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()

	if err != nil {
		return nil, err
	}

	publisher := &Publisher{
		connection: connection,
		channel:    channel,
	}

	return publisher, nil
}

// Queues places a message on a RabbitMQ Queue, which may have 1 or more consumers listening.
// It declares the queue before publishing messages
// Internally it uses the default exchange for sending messages.  The default exchange is a
// `direct` exchange with no name (empty string) pre-declared by the broker. It has one special
// property that makes it very useful for simple applications: every queue that is created is
// automatically bound to it with a `routing key` which is the same as the queue name).
// Note: PublishConfigSetters that modify the exchange are not relevant to this method
// Reference: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-default
func (p *Publisher) Queue(
	queueName string,
	message []byte,
	configSetters ...PublishConfigSetter,
) error {
	queueConfig := AMQPQueueConfig{
		name:       queueName,
		durable:    true,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}

	// The only relevant exchange config field here is the exchange name
	// because we don't need to declare the exchange; the default exchange
	// is pre-declared by the broker. The extra fields are added for correctness
	// and to avoid confusion when reading.
	exchangeConfig := AMQPExchangeConfig{
		name:         DEFAULT_AMQP_EXCHANGE,
		exchangeType: "direct",
		durable:      true,
		autoDelete:   false,
		internal:     false,
		noWait:       false,
		args:         nil,
	}

	config := &PublishConfig{
		queueConfig:    queueConfig,
		exchangeConfig: exchangeConfig,
		routingKey:     queueName, // Queues are bound to the default exchange with their queue name
		mandatory:      false,
		immediate:      false,
	}

	for _, configSetter := range configSetters {
		configSetter(config)
	}

	_, err := p.channel.QueueDeclare(
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

	payload := amqp.Publishing{Body: message}

	err = p.channel.Publish(
		config.exchangeConfig.name,
		config.routingKey,
		config.mandatory,
		config.immediate,
		payload,
	)

	if err != nil {
		return err
	}

	return nil
}

// Emit broadcasts a message to multiple queues,  which may have 1 or more consumers listening.
// It declares the exchange before publishing messages
// It publishes messages based on a provided pattern (routing key).
// Internally, it uses a topic exchange for sending messages.
// The routing key allows us scope messages only to queues that are bound with a matching "binding key".
// Reference: https://www.rabbitmq.com/tutorials/tutorial-five-go.html
func (p *Publisher) Emit(
	exchangeName string,
	routingKey string,
	message []byte,
	configSetters ...PublishConfigSetter,
) error {
	exchangeConfig := AMQPExchangeConfig{
		name:         exchangeName,
		exchangeType: "topic",
		durable:      true,
		autoDelete:   false,
		internal:     false,
		noWait:       false,
		args:         nil,
	}

	config := &PublishConfig{
		exchangeConfig: exchangeConfig,
		routingKey:     routingKey,
		mandatory:      false,
		immediate:      false,
	}

	for _, configSetter := range configSetters {
		configSetter(config)
	}

	err := p.channel.ExchangeDeclare(
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

	payload := amqp.Publishing{Body: message}

	err = p.channel.Publish(
		config.exchangeConfig.name,
		config.routingKey,
		config.mandatory,
		config.immediate,
		payload,
	)

	if err != nil {
		return err
	}

	return nil
}
