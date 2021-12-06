// Package service have business logic
package service

import (
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"fmt"
	"strconv"
)

// Producer have methods of brokers of messages
type Producer interface {
	Write(ch chan int, amountOfMessages int, message string) error
}

// KafkaProducer is struct of kafka
type KafkaProducer struct {
	conn *kafka.Conn
}

// NewKafkaProducer is constructor
func NewKafkaProducer(conn *kafka.Conn) *KafkaProducer {
	return &KafkaProducer{conn: conn}
}

// RabbitProducer is struct of rabbit
type RabbitProducer struct {
	channel *amqp.Channel
	queue   *amqp.Queue
}

// NewRabbitProducer is constructor
func NewRabbitProducer(channel *amqp.Channel, queue *amqp.Queue) *RabbitProducer {
	return &RabbitProducer{channel: channel, queue: queue}
}

// Write sends messages into broker
func (k *KafkaProducer) Write(ch chan int, amountOfMessages int, message string) error {
	for i := 0; i < amountOfMessages; i++ {
		num := strconv.Itoa(i)
		_, err := k.conn.WriteMessages(
			kafka.Message{Value: []byte(message + num)},
		)
		if err != nil {
			log.Errorf("failed to write messages: %s", err)
		}
		ch <- i
	}
	return nil
}

// Write sends messages into broker
func (r *RabbitProducer) Write(ch chan int, amountOfMessages int, message string) error {
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
