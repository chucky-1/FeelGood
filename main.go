package main

import (
	"context"
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/chucky-1/FeelGood/internal/configs"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func write(conn *kafka.Conn, ch chan int) error {
	for i:=0; i<2000; i++ {
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

func main()  {
	start := time.Now()

	// Configuration
	cfg := &configs.Config{}
	opts := &env.Options{}
	if err := env.Parse(cfg, *opts); err != nil {
		log.Fatal(err)
	}

	hostAndPort := cfg.Host + ":" + cfg.Port
	conn, err := kafka.DialLeader(context.Background(), "tcp", hostAndPort, cfg.Topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader: %s", err)
	}

	err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Fatal(err)
	}

	ch := make(chan int)
	for i:=0; i<4; i++ {
		go func() {
			err = write(conn, ch)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	i:=0
	for message := range ch {
		fmt.Println(i, message)
		i++
		if i == 8000 {
			close(ch)
		}
	}
	if err = conn.Close(); err != nil {
		log.Fatal("failed to close writer: %s", err)
	}
	log.Info("Program execution time ", time.Since(start).Milliseconds(), " milliseconds")
}
