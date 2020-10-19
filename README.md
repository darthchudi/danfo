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

type Payload struct {
	Amount    int       `json:"amount"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	publisher, err := danfo.NewPublisher("amqp://localhost")

	if err != nil {
		log.Fatalf("An error occured while connecting publisher: %v", err)
	}

	rawPayload := Payload{
		Amount:    100,
		Type:      "Debit",
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(rawPayload)

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

Internally, danfo uses the following defaults for declaring queues:
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

Internally, danfo uses the following defaults for publishing messages:
- The default exchange `""` is used
- The name of the queue is used as the `routing key` for publishing. 
- The `mandatory`  flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue. If this flag is set, the server will return an unroutable message with a Return method. If this flag is zero, the server silently drops the message.
    - `danfo.MandatoryPublish` functional option can be used for reverting this
- The `immediate` flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue consumer immediately. If this flag is set, the server will return an undeliverable message with a Return method. If this flag is zero, the server will queue the message, but with no guarantee that it will ever be consumed.
    - `danfo.ImmediatePublish` functional option can be used for reverting this


