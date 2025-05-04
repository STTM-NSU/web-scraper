package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/STTM-NSU/web-scrapper/internal/db/redis"
	"github.com/STTM-NSU/web-scrapper/internal/model"
	"github.com/STTM-NSU/web-scrapper/internal/ria"
)

func main() {
	log := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)

	if err := godotenv.Load(); err != nil {
		log.Error("error loading .env file: " + err.Error())
		return
	}

	cfgRedis := redis.Config{
		Host:     os.Getenv(model.EnvRedisHost),
		Port:     os.Getenv(model.EnvRedisPort),
		Password: os.Getenv(model.EnvRedisPassword),
	}
	if cfgRedis.Host == "" {
		log.Error("can't get host from env")
		return
	}
	if cfgRedis.Port == "" {
		log.Error("can't get port from env")
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	rdb, err := redis.Connect(ctx, cfgRedis)
	if err != nil {
		log.Error("can't connect to  redis: " + err.Error())
		return
	}

	log.Info(rdb.String())

	proxies := os.Getenv(model.EnvProxyUrls)
	if proxies == "" {
		log.Error("can't get proxies from env: " + err.Error())
		return
	}
	proxyUrls := strings.Split(os.Getenv(model.EnvProxyUrls), ",")

	riaScrapper := ria.NewScrapper(rdb, log, proxyUrls)
	fmt.Println(riaScrapper)

	if err := riaScrapper.Scrap(context.Background(), "20250305"); err != nil {
		log.Error("can't scrap ria " + err.Error())
	}

}
