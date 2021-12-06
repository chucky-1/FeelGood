package producer

import (
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"strconv"
)

// Kafka is struct of kafka
type Kafka struct {
	conn *kafka.Conn
}

// NewKafka is constructor
func NewKafka(conn *kafka.Conn) *Kafka {
	return &Kafka{conn: conn}
}

// Write sends messages into broker
func (k *Kafka) Write(ch chan int, amountOfMessages int, message string) error {
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
