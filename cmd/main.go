package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

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

	rdb := redis.Connect(cfgRedis)

	log.Info(rdb.String())

	proxyUrls := strings.Split(os.Getenv(model.EnvProxyUrls), ",")

	riaScrapper := ria.NewScrapper(rdb, log, proxyUrls)
	fmt.Println(riaScrapper)

	if err := riaScrapper.Scrap(context.Background(), "20250305"); err != nil {
		log.Error("can't scrap ria " + err.Error())
	}

}
