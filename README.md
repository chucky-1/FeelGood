This app uses kafka. The producer sends 8000 messages in about 4 seconds. The consumer receives and writes messages to Postgres almost instantly.

The producer achieves this speed due to goroutines.

The consumer achieves this speed due to asynchronous commits in Kafka and Batch in Postgres.

To run the application from the root folder, run the following commands:
- docker-compose up
- go run producer.go
- go run consumer.go