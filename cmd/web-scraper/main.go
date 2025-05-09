package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/STTM-NSU/web-scrapper/internal/config"
	"github.com/STTM-NSU/web-scrapper/internal/db/redis"
	"github.com/STTM-NSU/web-scrapper/internal/model"
	"github.com/STTM-NSU/web-scrapper/internal/proxy"
	"github.com/STTM-NSU/web-scrapper/internal/ria"
)

const _configFileName = "./configs/config.yaml"

func main() {
	log := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)

	if err := godotenv.Load(); err != nil {
		log.Error("error loading .env file: " + err.Error())
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

	cfg, err := config.LoadConfig(_configFileName)
	if err != nil {
		log.Error("can't load config: " + err.Error())
		return
	}

	fmt.Println(cfg)

	proxySwitcher, err := proxy.MyRoundRobinProxySwitcher(os.Getenv(model.EnvProxyUrls), log, cfg.ProxyRecoverTimeOut)
	if err != nil {
		log.Error("can't get proxy: " + err.Error())
		return
	}

	riaScrapper := ria.NewScrapper(rdb, log, proxySwitcher, cfg.RedisChanelName, cfg.PartitionsCount)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		proxySwitcher.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		proxySwitcher.RunForRecover(ctx)
	}()

	var done bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		data := cfg.StartDateScrapping
		for !done {

			if err := riaScrapper.Scrap(ctx, data.Format("20060102")); err != nil {
				log.Error("can't scrap ria " + err.Error())
			}
			if data.Format("20060102") == time.Now().Format("20060102") {
				time.Sleep(1 * time.Hour)
			} else {
				data = data.Add(24 * time.Hour)
			}

		}
	}()

	<-ctx.Done()
	log.Info("start graceful shutdown")
	done = true
	wg.Wait()
	log.Info("end graceful shutdown")
}
