package main

import (
	"github.com/caarlos0/env/v6"
	"github.com/chucky-1/FeelGood/internal/configs"
	"github.com/chucky-1/FeelGood/internal/service"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"context"
	"fmt"
	"time"
)

const (
	secondForConnect  = 10
	broker            = "rabbit" // kafka or rabbit
	amountOfMessages  = 10000
	amountOfGoroutine = 10
)

func main() {
	start := time.Now()

	// Configuration
	cfg := &configs.Config{}
	opts := &env.Options{}
	if err := env.Parse(cfg, *opts); err != nil {
		log.Fatal(err)
	}

	var srv service.Producer

	switch {
	case broker == "kafka":
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
		srv = service.NewKafkaProducer(conn)
	case broker == "rabbit":
		// Rabbit connect
		url := fmt.Sprintf("amqp://%s:%s@%s:%s", cfg.RbUser, cfg.RbPassword, cfg.RbHost, cfg.RbPort)
		conn, err := amqp.Dial(url)
		if err != nil {
			log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
		}
		defer func(conn *amqp.Connection) {
			err = conn.Close()
			if err != nil {
				log.Fatal(err)
			}
		}(conn)
		channel, err := conn.Channel()
		if err != nil {
			log.Fatal(err)
		}
		defer func(channel *amqp.Channel) {
			err = channel.Close()
			if err != nil {
				log.Fatal(err)
			}
		}(channel)
		queue, err := channel.QueueDeclare("hello", true, false, false, false, nil)
		if err != nil {
			log.Fatalf("%s: %s", "Failed to declare a queue", err)
		}
		srv = service.NewRabbitProducer(channel, &queue)
	}

	// Business logic
	ch := make(chan int)
	for i := 0; i < amountOfGoroutine; i++ {
		go func() {
			err := srv.Write(ch, amountOfMessages, "message")
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
