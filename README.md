# Danfo

![Danfo Logo](https://res.cloudinary.com/chudi/image/upload/v1602901743/Layer_1_2x.png)

Danfo is a wrapper around the [Go AMQP client](http://github.com/streadway/amqp).

## Goals

- 🧹 Reduce boilerplate code 
- 🌿 Provide good defaults I'd likely use in real-life projects
- 🤠 Auto-reconnect to the broker on connnection/channel error

## Sending Messages

There are two ways in which you can send a message with Danfo

### 1. Queue a message (1 to 1)
![](https://www.rabbitmq.com/img/tutorials/python-two.png)


Sends a message to a single Queue.

The default exchange `"""` is used internally by RabbitMQ when `Queue` is called. Every queue is automatically bound to the default exchange with a routing key which is 
the same as the queue name.

Reference: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-default


#### Usage

```go
package main

import (
	"encoding/json"
	"github.com/darthchudi/danfo"
	"log"
	"os"
	"time"
)

type Transaction struct {
	Amount    int       `json:"amount"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}
func NewPayload() ([]byte, error){
    transaction := Transaction{
    		Amount:    100,
    		Type:      "Debit",
    		Timestamp: time.Now(),
    	}
    
    	return json.Marshal(transaction)
}


func main() {
	url := os.Getenv("AMQP_URL")
	publisher, err := danfo.NewPublisher(url)

	if err != nil {
		log.Fatalf("An error occured while connecting publisher: %v", err)
	}

	payload, err := NewPayload()

	if err != nil {
		log.Fatalf("payload error: %v", err)
	}

	err = publisher.Queue("NOTIFICATION_QUEUE", payload)

	if err != nil {
		log.Fatalf("Publisher error: %v", err)
	}

	log.Print("Published message!")
}
``` 

#### Defaults 

##### Declaring the queue

Danfo sets the following [properties](https://www.rabbitmq.com/queues.html#properties) on Queues by default:
- Each queue is durable i.e will survive server restarts
    - `danfo.NonDurableQueue`
-  Each queue is non-auto deleted i.e will remain when there are no remaining consumers or binding
    - `danfo.AutoDeletedQueue` 
- Each queue is non-exclusive i.e they can be accessed by other connections asides from the connection that declares them
    - `danfo.ExclusiveQueue` 
- `noWait` is set to false. When `noWait` is set to true, the server will not respond to the declare queue call. A channel exception will arrive if the conditions are met for existing queues or attempting to modify an existing queue from a different connection.
    - `danfo.NoQueueWait` 
- No arguments are provided for the queue declaration
    - `danfo.QueueArguments` 


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

Sends a message to multiple queues. Messages are sent using a pattern. 


Danfo uses a `topic` exchange for sending messages when `Emit` is called; this gives us the flexibility to scope messages to only certain queues in our topology using arbitrary patterns e.g `user.login`, `playlist.created`.
 
Reference: https://www.rabbitmq.com/tutorials/tutorial-five-go.html


#### Usage

```go
package main

import (
	"encoding/json"
	"github.com/darthchudi/danfo"
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
    - `danfo.NonDurableExchange` 
-   Each exchange is non-auto deleted i.e will remain when there are no remaining consumers or binding
    - `danfo.AutoDeletedExchange` 
- Each exchange is non-internal. Internal exchanges do not accept publishes
    - `danfo.InternalExchange` 
- `noWait` is set to false. When `noWait` is set to true, the server will not respond to the declare exchange call. A channel exception will arrive if the conditions are met for existing exchange or attempting to modify an existing exchange from a different connection.
    - `danfo.NoExchangeWait` 
- No arguments are provided for the exchange declaration
    - `danfo.ExchangeArguments()` 


##### Emitting message to queues

Internally, Danfo uses the following defaults for publishing emitted messages to queues:
- The provided exchange name of type `topic` is used
- The provided `routing key` is used for publishing messages to bound queues  
- The `mandatory`  flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue. If this flag is set, the server will return an unroutable message with a Return method. If this flag is zero, the server silently drops the message.
    - `danfo.MandatoryPublish` functional option can be used for reverting this
- The `immediate` flag is set to false. This flag tells the server how to react if the message cannot be routed to a queue consumer immediately. If this flag is set, the server will return an undeliverable message with a Return method. If this flag is zero, the server will queue the message, but with no guarantee that it will ever be consumed.
    - `danfo.ImmediatePublish` functional option can be used for reverting this