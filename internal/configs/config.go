package configs

type Config struct {
	Host       string `env:"HOST"              envDefault:"localhost"`
	Port       string `env:"PORT"              envDefault:"9092"`
	Topic      string `env:"TOPIC"             envDefault:"my-topic"`
	GroupID    string `env:"GROUP_ID"          envDefault:"consumer-group-id"`
	PsUser     string `env:"POSTGRES_USER"     envDefault:"user"`
	PsPassword string `env:"POSTGRES_PASSWORD" envDefault:"testpassword"`
	PsHost     string `env:"POSTGRES_HOST"     envDefault:"localhost"`
	PsPort     string `env:"POSTGRES_PORT"     envDefault:"5432"`
	PsDbName   string `env:"POSTGRES_DB_NAME"  envDefault:"postgres"`
}
