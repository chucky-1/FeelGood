package producer

import (
	"github.com/streadway/amqp"

	"fmt"
	"strconv"
)

// Rabbit is struct of rabbit
type Rabbit struct {
	channel *amqp.Channel
	queue   *amqp.Queue
}

// NewRabbit is constructor
func NewRabbit(channel *amqp.Channel, queue *amqp.Queue) *Rabbit {
	return &Rabbit{channel: channel, queue: queue}
}

// Write sends messages into broker
func (r *Rabbit) Write(ch chan int, amountOfMessages int, message string) error {
	for i := 0; i < amountOfMessages; i++ {
		num := strconv.Itoa(i)
		err := r.channel.Publish("", r.queue.Name, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message + num),
			})
		if err != nil {
			return fmt.Errorf("%s: %s", "Failed to publish a message", err)
		}
		ch <- i
	}
	return nil
}
