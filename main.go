package main

import (
	"github.com/caarlos0/env/v6"
	"github.com/chucky-1/FeelGood/internal/configs"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"context"
	"fmt"
	"time"
)

const (
	secondForConnect  = 10
	amountOfMessages  = 2000
	amountOfGoroutine = 4
)

func write(conn *kafka.Conn, ch chan int) error {
	for i := 0; i < amountOfMessages; i++ {
		_, err := conn.WriteMessages(
			kafka.Message{Value: []byte("message")},
		)
		if err != nil {
			log.Errorf("failed to write messages: %s", err)
			return err
		}
		ch <- i
	}
	return nil
}

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
	conn, err := kafka.DialLeader(context.Background(), "tcp", hostAndPort, cfg.Topic, 0)
	if err != nil {
		log.Fatalf("failed to dial leader: %s", err)
	}
	err = conn.SetWriteDeadline(time.Now().Add(time.Second * secondForConnect))
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn *kafka.Conn) {
		err = conn.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(conn)

	// Business logic
	ch := make(chan int)
	for i := 0; i < amountOfGoroutine; i++ {
		go func() {
			err = write(conn, ch)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
	i := 0
	totalMessages := amountOfGoroutine * amountOfMessages
	for message := range ch {
		fmt.Println(i, message)
		i++
		if i == totalMessages {
			close(ch)
		}
	}
	log.Info("Program execution time ", time.Since(start).Milliseconds(), " milliseconds")
}
