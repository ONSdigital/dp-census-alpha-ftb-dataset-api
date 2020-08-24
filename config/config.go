package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr                string        `envconfig:"BIND_ADDR"`
	CodeListAPIURL          string        `envconfig:"CODE_LIST_API_URL"`
	FTBDatasetAPIURL        string        `envconfig:"FTBDATASET_API_URL"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	WebsiteURL              string        `envconfig:"WEBSITE_URL"`
	MongoConfig             MongoConfig
}

// MongoConfig contains the config required to connect to MongoDB.
type MongoConfig struct {
	BindAddr   string `envconfig:"MONGODB_BIND_ADDR"   json:"-"`
	Collection string `envconfig:"MONGODB_COLLECTION"`
	Database   string `envconfig:"MONGODB_DATABASE"`
}

var cfg *Configuration

// Get the application and returns the configuration structure
func Get() (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		BindAddr:                ":10400",
		CodeListAPIURL:          "http://localhost:22400",
		FTBDatasetAPIURL:        "http://localhost:10400",
		GracefulShutdownTimeout: 5 * time.Second,
		WebsiteURL:              "http://localhost:20000",
		MongoConfig: MongoConfig{
			BindAddr:   "localhost:27017",
			Collection: "datasets",
			Database:   "datasets",
		},
	}

	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Configuration) String() string {
	json, _ := json.Marshal(config)
	return string(json)
}
