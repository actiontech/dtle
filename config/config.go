package config

import (
	"fmt"
	"path/filepath"
)

// Config is the configuration for the Udup agent.
type Config struct {
	LogLevel  string `mapstructure:"log_level"`
	LogFile   string `mapstructure:"log_file"`
	LogRotate string `mapstructure:"log_rotate"`

	// config file that have been loaded (in order)
	File string `mapstructure:"-"`
}

// DefaultConfig is a the baseline configuration for Udup
func DefaultConfig() *Config {
	return &Config{
		LogLevel: "INFO",
		File:     "udup.conf",
	}
}

// LoadConfig loads the configuration at the given path, regardless if
// its a file or directory.
func LoadConfig(path string) (*Config, error) {
	cleaned := filepath.Clean(path)
	config, err := ParseConfigFile(cleaned)
	if err != nil {
		return nil, fmt.Errorf("Error loading %s: %s", cleaned, err)
	}

	config.File = cleaned
	return config, nil
}
