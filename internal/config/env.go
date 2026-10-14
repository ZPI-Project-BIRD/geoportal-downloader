package config

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

const DefaultEnvFile = ".env"

func Load(specification any) error {
	path, _ := os.LookupEnv("ENV_FILE")
	if path == "" {
		path = DefaultEnvFile
	}

	_ = godotenv.Load(path)
	return envconfig.Process("", specification)
}
