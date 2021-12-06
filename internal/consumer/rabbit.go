package consumer

import (
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"fmt"
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

// Read func reads messages from broker and sends it in batch
func (r *Rabbit) Read(ch chan pgx.Batch, batch pgx.Batch, amountOfBatches, amountOfMessages int) error {
	msgs, err := r.channel.Consume(r.queue.Name, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("%s: %s", "Failed to register a consumer", err)
	}

	for x := 0; x < amountOfBatches; x++ {
		newBatch := batch
		for i := 0; i < amountOfMessages; i++ {
			m, ok := <-msgs
			if !ok {
				break
			}
			log.Infof("message %d, %d: %s given", x, i, string(m.Body))
			newBatch.Queue("INSERT INTO message VALUES ($1)", string(m.Body))
		}
		ch <- newBatch
	}
	close(ch)
	return nil
}

// Close func closes channel
func (r *Rabbit) Close() error {
	err := r.channel.Close()
	if err != nil {
		return err
	}
	return nil
}
