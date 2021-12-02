// Package repository works with database
package repository

import (
	"github.com/jackc/pgx/v4"

	"context"
)

// Repository is struct
type Repository struct {
	conn *pgx.Conn
}

// NewRepository is constructor
func NewRepository(conn *pgx.Conn) *Repository {
	return &Repository{conn: conn}
}

// SendBatch dose sendBatch
func (r *Repository) SendBatch(ctx context.Context, batch *pgx.Batch) error {
	var batchRes = r.conn.SendBatch(ctx, batch)
	err := batchRes.Close()
	if err != nil {
		return err
	}
	return nil
}
