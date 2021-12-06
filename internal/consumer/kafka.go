package consumer

import (
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"context"
	"fmt"
	"time"
)

// Kafka is struct of kafka
type Kafka struct {
	reader *kafka.Reader
}

// NewKafka is constructor
func NewKafka(reader *kafka.Reader) *Kafka {
	return &Kafka{reader: reader}
}

func readMessages(k *Kafka, ch chan pgx.Batch, batch pgx.Batch, amountOfMessages, x int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(amountOfMessages/2))
	defer cancel()
	for i := 0; i < amountOfMessages; i++ {
		m, err := k.reader.ReadMessage(ctx)
		if err != nil {
			close(ch)
			return err
		}
		log.Infof("message %d, %d: %s given", x, i, string(m.Value))
		batch.Queue("INSERT INTO message VALUES ($1)", string(m.Value))
	}
	ch <- batch
	return nil
}

// Read func reads messages from broker and sends it in batch
func (k *Kafka) Read(ch chan pgx.Batch, batch pgx.Batch, amountOfBatches, amountOfMessages int) error {
	for x := 0; x < amountOfBatches; x++ {
		newBatch := batch
		err := readMessages(k, ch, newBatch, amountOfMessages, x)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	close(ch)
	return nil
}

// Close func closes channel
func (k *Kafka) Close() error {
	err := k.reader.Close()
	if err != nil {
		return fmt.Errorf("failed to close reader: %s", err)
	}
	return nil
}
