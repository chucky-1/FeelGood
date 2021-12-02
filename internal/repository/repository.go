package repository

import (
	"context"
	"github.com/jackc/pgx/v4"
)

// Repository provides a connection with postgres
type Repository struct {
	conn  *pgx.Conn
	batch *pgx.Batch
}

// NewRepository is constructor
func NewRepository(conn *pgx.Conn) *Repository {
	return &Repository{conn: conn, batch: new(pgx.Batch)}
}

func (c *Repository) Batch(message string) {
	c.batch.Queue("INSERT INTO message VALUES ($1)", message)
}

func (c *Repository) SendBatch(ctx context.Context) error {
	result := c.conn.SendBatch(ctx, c.batch)
	err := result.Close()
	if err != nil {
		return err
	}
	return nil
}
