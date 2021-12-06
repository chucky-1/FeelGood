package service

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

// Consumer have methods of brokers of messages
type Consumer interface {
	Read(ch chan pgx.Batch, batch pgx.Batch, amountOfBatches, amountOfMessages int) error
	Close() error
}

// KafkaConsumer is struct of kafka
type KafkaConsumer struct {
	reader *kafka.Reader
}

// NewKafkaConsumer is constructor
func NewKafkaConsumer(reader *kafka.Reader) *KafkaConsumer {
	return &KafkaConsumer{reader: reader}
}

// RabbitConsumer is struct of rabbit
type RabbitConsumer struct {
	channel *amqp.Channel
	queue   *amqp.Queue
}

// NewRabbitConsumer is constructor
func NewRabbitConsumer(channel *amqp.Channel, queue *amqp.Queue) *RabbitConsumer {
	return &RabbitConsumer{channel: channel, queue: queue}
}

// Read func reads messages from broker and sends it in batch
func (k *KafkaConsumer) Read(ch chan pgx.Batch, batch pgx.Batch, amountOfBatches, amountOfMessages int) error {
	for x := 0; x < amountOfBatches; x++ {
		newBatch := batch
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(amountOfMessages/2))
		defer cancel()
		for i := 0; i < amountOfMessages; i++ {
			m, err := k.reader.ReadMessage(ctx)
			if err != nil {
				log.Error(err)
				close(ch)
				return err
			}
			log.Infof("message %d, %d: %s given", x, i, string(m.Value))
			newBatch.Queue("INSERT INTO message VALUES ($1)", string(m.Value))
		}
		ch <- newBatch
	}
	close(ch)
	return nil
}

// Close func closes channel
func (k *KafkaConsumer) Close() error {
	err := k.reader.Close()
	if err != nil {
		return fmt.Errorf("failed to close reader: %s", err)
	}
	return nil
}

// Read func reads messages from broker and sends it in batch
func (r *RabbitConsumer) Read(ch chan pgx.Batch, batch pgx.Batch, amountOfBatches, amountOfMessages int) error {
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
func (r *RabbitConsumer) Close() error {
	err := r.channel.Close()
	if err != nil {
		return err
	}
	return nil
}
