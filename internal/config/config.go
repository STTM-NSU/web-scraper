package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	StartDateScrapping  time.Time `yaml:"startDateScrapping"`
	ProxyRecoverTimeOut int       `yaml:"proxyRecoverTimeOut"`
	RedisChanelName     string    `yaml:"redisChanelName"`
	PartitionsCount     int       `yaml:"partitionsCount"`
}

func LoadConfig(filename string) (Config, error) {
	var cfg Config
	input, err := os.ReadFile(filename)
	if err != nil {
		return cfg, fmt.Errorf("can't read file: %w", err)
	}

	if err := yaml.Unmarshal(input, &cfg); err != nil {
		return cfg, fmt.Errorf("can't unmarshal config: %w", err)
	}

	if cfg.StartDateScrapping.After(time.Now()) {
		return cfg, fmt.Errorf("StartDateScrapping=%s can't be greater than now=%s", cfg.StartDateScrapping, time.Now())
	}

	if cfg.ProxyRecoverTimeOut <= 0 {
		return cfg, fmt.Errorf("ProxyRecoverTimeOut=%d can't be <= 0", cfg.ProxyRecoverTimeOut)
	}

	if cfg.RedisChanelName == "" {
		return cfg, fmt.Errorf("RedisChanelName if empty")
	}

	if cfg.PartitionsCount <= 0 {
		return cfg, fmt.Errorf("PartitionsCount=%d can't be <= 0", cfg.PartitionsCount)
	}

	return cfg, nil
}
