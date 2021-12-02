package main

import (
	"github.com/caarlos0/env/v6"
	"github.com/chucky-1/FeelGood/internal/configs"
	"github.com/chucky-1/FeelGood/internal/repository"
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"context"
	"fmt"
	"time"
)

const (
	minBytes         = 10e3 // 10KB
	maxBytes         = 10e6 // 10MB
	secondForConnect = 10
)

func main() {
	start := time.Now()

	// Configuration
	cfg := &configs.Config{}
	opts := &env.Options{}
	if err := env.Parse(cfg, *opts); err != nil {
		log.Fatal(err)
	}

	// Kafka connect
	hostAndPort := cfg.Host + ":" + cfg.Port
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{hostAndPort},
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		CommitInterval: time.Second,
	})

	// Postgres connect
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*secondForConnect)
	defer cancel()
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.PsUser, cfg.PsPassword, cfg.PsHost, cfg.PsPort, cfg.PsDBName)
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		log.Errorf("Unable to connect to database: %s\n", err)
		return
	}
	defer func(conn *pgx.Conn, ctx context.Context) {
		err = conn.Close(ctx)
		if err != nil {
			log.Error(err)
			return
		}
	}(conn, context.Background())
	rep := repository.NewRepository(conn)

	batch := new(pgx.Batch)

	// Business logic
	var i float32 = 1
	// 10 seconds for connect
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*secondForConnect)
	defer cancel()
	for {
		m, err2 := r.ReadMessage(ctx)
		if err2 != nil {
			log.Error(err2)
		}
		log.Infof("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		batch.Queue("INSERT INTO message VALUES ($1)", "message")
		if i/2000 == 1 {
			err = rep.SendBatch(ctx, batch)
			if err != nil {
				log.Error(err)
				break
			}
			log.Info("2000 messages are read in ", time.Since(start).Milliseconds(), " milliseconds")
			start = time.Now()
			i = 1
			cancel()
			// 1 second for 2000 messages
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		}
		i++
	}
	if err = r.Close(); err != nil {
		log.Error("failed to close reader:", err)
		return
	}
}
