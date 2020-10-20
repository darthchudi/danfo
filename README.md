# Danfo

![Danfo Logo](https://res.cloudinary.com/chudi/image/upload/v1602901743/Layer_1_2x.png)

Danfo is an opinionated wrapper built on-top of the [Go AMQP library](http://github.com/streadway/amqp). It provides a declarative API for working with RabbitMQ in Go. 

## Goals

- 🧹 Reduce boilerplate code required for sending and receiving messages
- 🥢 Provide a friendlier interface on a low(er) level interface
- 👀 Mirror the [Node.js](https://github.com/random-guys/eventbus) EventBus API as closely as possible
- 🙃 Provide good defaults

## Sending Messages

There are two ways in which you can send a message with Danfo

### 1. Queue a message
![](https://www.rabbitmq.com/img/tutorials/python-two.png)


This places a message on a Queue, which may have 1 or more consumers listening on it.

Internally it uses the default exchange for sending messages.  The default exchange is a `direct` exchange with no name (empty string) pre-declared by the broker. It has one special property that makes it very useful for simple applications: every queue that is created is automatically bound to it with a `routing key` which is the same as the queue name).

The default exchange makes it seem like it is possible to deliver messages directly to queues, even though that is not technically what is happening.

When you use default exchange, your message is delivered to the queue 
with a name equal to the routing key of the message. Every queue is 
automatically bound to the default exchange with a routing key which is 
the same as the queue name.

Reference: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-default


#### Usage

```go
package main

import (
"encoding/json"
"github.com/darthchudi/danfo"
"log"
"time"
)
type Transaction struct {
	Amount    int       `json:"amount"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	publisher, err := danfo.NewPublisher("amqp://localhost")

	if err != nil {
		log.Fatalf("An error occured while connecting publisher: %v", err)
	}

	transaction := Transaction{
		Amount:    100,
		Type:      "Debit",
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(transaction)

	if err != nil {
		log.Fatalf("JSON Marshall error: %v", err)
	}

	err = publisher.Queue("NOTIFICATION_QUEUE", payload)

	if err != nil {
		log.Fatalf("Queue error: %v", err)
	}

	log.Print("Published message!")
}
``` 


#### Defaults 

##### Declaring the queue

Internally, Danfo uses the following defaults for declaring queues:
- Each queue is durable i.e will survive server restarts
    - `danfo.NonDurableQueue` can be used for reverting this
-  Each queue is non-auto deleted i.e will remain when there are no remaining consumers or binding
    - `danfo.AutoDeletedQueue` can be used for reverting this
- Each queue is non-exclusive i.e they can be accessed by other connections asides from the connection that declares them
    - `danfo.ExclusiveQueue` can be used for reverting this
- `noWait` is set to false. When `noWait` is set to true, the server will not respond to the declare queue call. A channel exception will arrive if the conditions are met for existing queues or attempting to modify an existing queue from a different connection.
    - `danfo.NoQueueWait` functional option can be used for reverting this
- No arguments are provided for the queue declaration
    - `danfo.QueueArguments()` can be used for reverting this


##### Publishing the message to the queue

Internally, Danfo uses the following defaults for publishing messages:
- The default exchange `""` is used
- The name of the queue is used as the `routing key` for publishing. 
- The `mandatory`  flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue. If this flag is set, the server will return an unroutable message with a Return method. If this flag is zero, the server silently drops the message.
    - `danfo.MandatoryPublish` functional option can be used for reverting this
- The `immediate` flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue consumer immediately. If this flag is set, the server will return an undeliverable message with a Return method. If this flag is zero, the server will queue the message, but with no guarantee that it will ever be consumed.
    - `danfo.ImmediatePublish` functional option can be used for reverting this


### 2. Emit a message
![](https://www.rabbitmq.com/img/tutorials/python-five.png)

This broadcasts a message to multiple queues,  which may have 1 or more consumers listening. It allows us to publish messages based on a pattern. 

Here we explicitly reference the exchange used for routing messages.

Internally, it uses a `topic` exchange for sending messages; this gives us the flexibility to scope messages to queues using arbitrary patterns. A `topic` exchange is also capable of behaving like other exchanges e.g
 
 - When a queue is bound with `"#"` (hash) `binding key` - it will receive all the messages, regardless of the routing key - like in `fanout` exchange.
 - When special characters `"*"` (star) and `"#"` (hash) aren't used in bindings, the `topic` exchange will behave just like a `direct` one.
 
 
 Reference: https://www.rabbitmq.com/tutorials/tutorial-five-go.html


#### Usage

```go
package main

import (
"github.com/darthchudi/danfo"
"encoding/json"
"log"
"time"
)
type Cart struct {
	User      string    `json:"user"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	publisher, err := danfo.NewPublisher("amqp://localhost")

	if err != nil {
		log.Fatalf("Publisher error: %v", err)
	}

	cart := Cart{
		User:      "6ee6d392-227e-45c0-bad5-fb92d0a32def",
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(cart)

	if err != nil {
		log.Fatalf("JSON Marshal error: %v", err)
	}

	err = publisher.Emit("events", "cart.click", payload)

	if err != nil {
		log.Fatalf("Publish error: %v", err)
	}
}
``` 


#### Defaults 

##### Declaring the exchange

- Each exchange is a topic exchange 
- Each exchange is durable i.e will survive server restarts
    - `danfo.NonDurableExchange` functional option can be used for reverting this 
-   Each exchange is non-auto deleted i.e will remain when there are no remaining consumers or binding
    - `danfo.AutoDeletedExchange` functional option can be used for reverting this
- Each exchange is non-internal. Internal exchanges do not accept publishes
    - `danfo.InternalExchange` can be used for reverting this
- `noWait` is set to false. When `noWait` is set to true, the server will not respond to the declare exchange call. A channel exception will arrive if the conditions are met for existing exchange or attempting to modify an existing exchange from a different connection.
    - `danfo.NoExchangeWait` functional option can be used for reverting this
- No arguments are provided for the exchange declaration
    - `danfo.ExchangeArguments()` can be used for reverting this


##### Emitting message to queues

Internally, Danfo uses the following defaults for publishing emitted messages to queues:
- The provided exchange name of type `topic` is used
- The provided `routing key` is used for publishing messages to bound queues  
- The `mandatory`  flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue. If this flag is set, the server will return an unroutable message with a Return method. If this flag is zero, the server silently drops the message.
    - `danfo.MandatoryPublish` functional option can be used for reverting this
- The `immediate` flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue consumer immediately. If this flag is set, the server will return an undeliverable message with a Return method. If this flag is zero, the server will queue the message, but with no guarantee that it will ever be consumed.
    - `danfo.ImmediatePublish` functional option can be used for reverting this