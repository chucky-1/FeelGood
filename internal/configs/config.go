package configs

type Config struct {
	Host    string `env:"HOST"     envDefault:"localhost"`
	Port    string `env:"PORT"     envDefault:"9092"`
	Topic   string `env:"TOPIC"    envDefault:"my-topic"`
	GroupID string `env:"GROUP-ID" envDefault:"consumer-group-id"`
}
