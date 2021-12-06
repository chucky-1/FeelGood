package main

import (
	"github.com/caarlos0/env/v6"
	"github.com/chucky-1/FeelGood/internal/configs"
	"github.com/chucky-1/FeelGood/internal/repository"
	"github.com/chucky-1/FeelGood/internal/service"
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"context"
	"fmt"
	"time"
)

const (
	minBytes           = 10e3 // 10KB
	maxBytes           = 10e6 // 10MB
	secondForConnect   = 10
	secondForSendBatch = 60
	broker             = "rabbit" // kafka or rabbit
	amountOfMessages   = 10000
	amountOfBatches    = 10
)

func sendBatch(rep *repository.Repository, batch pgx.Batch) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*secondForSendBatch)
	defer cancel()
	err := rep.SendBatch(ctx, &batch)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	start, allTime := time.Now(), time.Now()

	// Configuration
	cfg := &configs.Config{}
	opts := &env.Options{}
	if err := env.Parse(cfg, *opts); err != nil {
		log.Error(err)
		return
	}

	var srv service.Consumer

	switch {
	case broker == "kafka":
		// Kafka connect
		hostAndPort := cfg.Host + ":" + cfg.Port
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{hostAndPort},
			GroupID:        cfg.GroupID,
			Topic:          cfg.Topic,
			MinBytes:       minBytes,
			MaxBytes:       maxBytes,
			CommitInterval: time.Second,
		})
		srv = service.NewKafkaConsumer(reader)
	case broker == "rabbit":
		// Rabbit connect
		url := fmt.Sprintf("amqp://%s:%s@%s:%s", cfg.RbUser, cfg.RbPassword, cfg.RbHost, cfg.RbPort)
		conn, err := amqp.Dial(url)
		if err != nil {
			log.Errorf("%s: %s", "Failed to connect to RabbitMQ", err)
			return
		}
		defer func(conn *amqp.Connection) {
			err = conn.Close()
			if err != nil {
				log.Error(err)
				return
			}
		}(conn)
		ch, err := conn.Channel()
		if err != nil {
			log.Errorf("%s: %s", "Failed to open a channel", err)
			return
		}
		defer func(ch *amqp.Channel) {
			err = ch.Close()
			if err != nil {
				log.Error(err)
				return
			}
		}(ch)
		queue, err := ch.QueueDeclare("hello", true, false, false, false, nil)
		if err != nil {
			log.Errorf("%s: %s", "Failed to declare a queue", err)
			return
		}
		srv = service.NewRabbitConsumer(ch, &queue)
	}

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

	// Business logic
	ch := make(chan pgx.Batch)
	go func() {
		err = srv.Read(ch, pgx.Batch{}, amountOfBatches, amountOfMessages)
		if err != nil {
			log.Error(err)
			return
		}
	}()
	for c := range ch {
		err = sendBatch(rep, c)
		if err != nil {
			log.Error(err)
		}
		log.Infof("%d messages are read in %d %s", amountOfMessages, time.Since(start).Milliseconds(), " milliseconds")
		start = time.Now()
	}
	log.Infof("%d read and added into database in %e", amountOfBatches*amountOfMessages, time.Since(allTime).Seconds())
	err = srv.Close()
	if err != nil {
		log.Error(err)
		return
	}
}
