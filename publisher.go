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

// AMQPQueueConfig describes the options for declaring an AMQP queue
type AMQPQueueConfig struct {
	// The queue name may be empty, in which case the server will generate a unique name
	// which will be returned in the Name field of Queue struct.
	name string

	// Durable queues will survive server restarts
	durable bool

	// Auto-deleted queues will be deleted when there are no remaining bindings.
	autoDelete bool

	// Exclusive queues are only accessible by the connection that declares them. Exclusive
	// non-durable queues will be deleted when the connection closes. Channels on other
	// connections will receive an error when attempting  to declare, bind, consume, purge
	// or delete a queue with the same name.
	exclusive bool

	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	noWait bool

	// Optional amqp.Table of arguments that are specific to the server's implementation of
	// the queue can be sent for queues that require extra parameters.
	args amqp.Table
}

// AMQPExchangeConfig describes the options for declaring an AMQP exchange
type AMQPExchangeConfig struct {
	// Exchange names starting with "amq." are reserved for pre-declared and
	// standardized exchanges. The client MAY declare an exchange starting with
	// "amq." if the passive option is set, or the exchange already exists.  Names can
	// consist of a non-empty sequence of letters, digits, hyphen, underscore,
	// period, or colon.
	name string

	// Each exchange belongs to one of a set of exchange kinds/types implemented by
	// the server. The exchange types define the functionality of the exchange - i.e.
	// how messages are routed through it. Once an exchange is declared, its type
	// cannot be changed.  The common types are "direct", "fanout", "topic" and
	// "headers".
	exchangeType string

	// Durable exchanges will survive server restarts
	durable bool

	// Auto-deleted exchanges will be deleted when there are no remaining bindings.
	autoDelete bool

	// Exchanges declared as `internal` do not accept accept publishes. Internal
	// exchanges are useful when you wish to implement inter-exchange topologies
	// that should not be exposed to users of the broker.
	internal bool

	// When noWait is true, declare without waiting for a confirmation from the server.
	// The channel may be closed as a result of an error
	noWait bool

	// Optional amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	args amqp.Table
}

// PublishConfig describes the options for publishing a message
type PublishConfig struct {
	// Describes the queue configuration
	queueConfig *AMQPQueueConfig

	// Describes the exchange configuration
	exchangeConfig *AMQPExchangeConfig

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

// Sets fields in a publish config
type PublishConfigSetter func(config *PublishConfig)

// NonDurableQueue declares a non durable queue on a publish config.
// A non-durable queue will not survive server restarts
func NonDurableQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.durable = false
}

// AutoDeletedQueue declares an auto-deleted queue on a publish config.
// An auto-deleted queue will be deleted when there are no remaining consumers or binding
func AutoDeletedQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.autoDelete = true
}

// ExclusiveQueue declares an Exclusive queue on a publish config
// An exclusive queue cannot be accessed by other connections asides
// from the connection that declares them
func ExclusiveQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.exclusive = true
}

// NoWaitQueue declares sets the `noWait` option to true. When set
// the server will not respond to the declare queue call.
// A channel exception will arrive if the conditions are met
// for existing queues or attempting to modify an existing queue from
// a different connection.
func NoWaitQueue(pConfig *PublishConfig) {
	pConfig.queueConfig.noWait = true
}

// QueueArguments sets the set of arguments for the queue declaration
func QueueArguments(args amqp.Table) PublishConfigSetter {
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
	publisher := &Publisher{}

	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()

	if err != nil {
		return nil, err
	}

	publisher.connection = connection
	publisher.channel = channel

	return publisher, nil
}

// Queues places a message on a RabbitMQ Queue, which may have 1 or more consumers listening.
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
	queueConfig := &AMQPQueueConfig{
		name:       queueName,
		durable:    true,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}

	// We don't need to specify the other exchange properties because the only
	// relevant exchange property in this context is the exchange name
	exchangeConfig := &AMQPExchangeConfig{
		name: DEFAULT_AMQP_EXCHANGE,
	}

	config := &PublishConfig{
		queueConfig:    queueConfig,
		exchangeConfig: exchangeConfig,
		mandatory:      false,
		immediate:      false,
	}

	for _, configSetter := range configSetters {
		configSetter(config)
	}

	queue, err := p.channel.QueueDeclare(
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

	// Set the routing key after the queue has been declared
	config.routingKey = queue.Name

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
	exchangeConfig := &AMQPExchangeConfig{
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
