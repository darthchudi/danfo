package danfo

import "github.com/streadway/amqp"

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