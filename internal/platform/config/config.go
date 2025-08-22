package config

import (
	"github.com/caarlos0/env/v10"
	"time"
)

type Config struct {
	HTTPAddr        string        `env:"HTTP_ADDR"         envDefault:":8080"`
	KafkaBrokers    []string      `env:"KAFKA_BROKERS"     envDefault:"localhost:9092" envSeparator:","`
	KafkaTopic      string        `env:"KAFKA_TOPIC"       envDefault:"events"`
	WriteBatchBytes int           `env:"WRITE_BATCH_BYTES" envDefault:"1048576"`
	WriterBatchTime time.Duration `env:"WRITER_BATCH_TIME" envDefault:"50ms"`
	LogLevel        string        `env:"LOG_LEVEL"         envDefault:"info"`
}

func FromEnv() (Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return Config{}, err
	}
	return c, nil
}
