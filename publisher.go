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

	// Will remain declared when there are no remaining bindings.
	autoDelete bool

	// Exclusive queues are only accessible by the connection that declares them and
	//will be deleted when the connection closes.  Channels on other connections
	//will receive an error when attempting  to declare, bind, consume, purge or
	//delete a queue with the same name.
	exclusive bool

	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	noWait bool

	args amqp.Table
}

// PublishConfig describes the options for publishing a message
type PublishConfig struct {
	// Describes the queue configuration
	queueConfig *AMQPQueueConfig

	// Exchange used for sending messages
	exchange string

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
// A durable queue will survive server restarts
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
// automatically bound to it with a `routing key` which is the same as the queue name)
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

	config := &PublishConfig{
		queueConfig: queueConfig,
		exchange: DEFAULT_AMQP_EXCHANGE,
		mandatory: false,
		immediate: false,
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
		config.exchange,
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
