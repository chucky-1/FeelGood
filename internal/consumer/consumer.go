// Package consumer gives messages from broker
package consumer

import "github.com/jackc/pgx/v4"

// Consumer have methods of brokers of messages
type Consumer interface {
	Read(ch chan pgx.Batch, batch pgx.Batch, amountOfBatches, amountOfMessages int) error
	Close() error
}
