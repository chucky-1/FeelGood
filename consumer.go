package main

import (
	"context"
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/chucky-1/FeelGood/internal/configs"
	"github.com/chucky-1/FeelGood/internal/repository"
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
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
		Brokers:   []string{hostAndPort},
		GroupID:   cfg.GroupID,
		Topic:     cfg.Topic,
		//Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		CommitInterval: time.Second,
	})

	// Postgres connect
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.PsUser, cfg.PsPassword, cfg.PsHost, cfg.PsPort, cfg.PsDbName)
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())
	rep := repository.NewRepository(conn)

	// Business logic
	var i float32 = 1
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Info("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		rep.Batch(string(m.Value))
		if i / 2000 == 1 {
			err := rep.SendBatch(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			log.Info("2000 messages are read in ", time.Since(start).Milliseconds(), " milliseconds")
			start = time.Now()
			i = 1
		}
		i++
	}

	if err = r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}