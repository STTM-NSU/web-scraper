package ria

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/proxy"
	"github.com/redis/go-redis/v9"

	"github.com/STTM-NSU/web-scrapper/internal/model"
)

type Scrapper struct {
	rdb       *redis.Client
	logger    *slog.Logger
	proxyUrls []string
}

func NewScrapper(rdb *redis.Client, logger *slog.Logger, proxyUrls []string) *Scrapper {
	return &Scrapper{
		rdb:       rdb,
		logger:    logger,
		proxyUrls: proxyUrls,
	}
}

func (s *Scrapper) Scrap(ctx context.Context, day string) error {
	var articles sync.Map
	var articlesDate sync.Map
	var counter int
	var mutex sync.Mutex

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Recovered in ria.Scrap", slog.Any("panic", r))
		}
	}()

	c := colly.NewCollector(
		colly.URLFilters(
			regexp.MustCompile(`https://([a-z]+\.)?ria\.ru/`+day+`+[^?]`),
			regexp.MustCompile(`https://ria\.ru/services/`+day+`+`),
		),
		colly.Async(true),
	)

	err := c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 2,
	})
	if err != nil {
		return fmt.Errorf("can't set limit %w", err)
	}

	rp, err := proxy.RoundRobinProxySwitcher(s.proxyUrls...)
	if err != nil {
		log.Fatal(err)
	}
	c.SetProxyFunc(rp)

	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Request.AbsoluteURL(e.Attr("href"))
		if link != "" && !strings.Contains(link, "?") {
			err := e.Request.Visit(link)
			if err != nil {
				return
			}
		}
	})

	c.OnHTML("div.article__info-date", func(e *colly.HTMLElement) {
		date, err := time.Parse("15:04 02.01.2006", e.Text[:16])

		if err != nil {
			fmt.Println(err)
		}
		articlesDate.Store(e.Request.URL.String(), date)
		mutex.Lock()
		counter++
		mutex.Unlock()

	})

	c.OnHTML("div.article__title", func(e *colly.HTMLElement) {
		if text, ok := articles.Load(e.Request.URL.String()); ok {
			articles.Store(e.Request.URL.String(), append(text.([]string), e.Text))
		} else {
			articles.Store(e.Request.URL.String(), []string{e.Text})
		}
	})

	c.OnHTML("h1.article__title", func(e *colly.HTMLElement) {
		if text, ok := articles.Load(e.Request.URL.String()); ok {
			articles.Store(e.Request.URL.String(), append(text.([]string), e.Text))
		} else {
			articles.Store(e.Request.URL.String(), []string{e.Text})
		}
	})

	c.OnHTML("div.article__text", func(e *colly.HTMLElement) {
		if text, ok := articles.Load(e.Request.URL.String()); ok {
			articles.Store(e.Request.URL.String(), append(text.([]string), e.Text))
		} else {
			articles.Store(e.Request.URL.String(), []string{e.Text})
		}
	})
	c.OnHTML("div.recommend__place", func(e *colly.HTMLElement) {
		idStr := e.Request.URL.String()[len(e.Request.URL.String())-15:]
		id, err := strconv.Atoi(idStr[:10])
		if err != nil {
			s.logger.Error("can't get article id: " + err.Error())
			return
		}
		partition := id % model.PartitionsCount
		redisChanel := model.RedisChanelName + strconv.Itoa(partition)
		date, ok := articlesDate.Load(e.Request.URL.String())
		if !ok {
			s.logger.Error("no date", e.Request.URL.String())
			return
		}
		text, ok := articles.Load(e.Request.URL.String())
		if !ok {
			s.logger.Error("no text", e.Request.URL.String())
			return
		}
		redisMessage, err := json.Marshal(model.ScrapperPayload{
			Date: date.(time.Time).Format(time.RFC3339),
			Text: strings.Join(text.([]string), " "),
		})
		if err != nil {
			s.logger.Error("can't marshal message: " + err.Error())
		}
		err = s.rdb.Publish(ctx, redisChanel, redisMessage).Err()
		if err != nil {
			s.logger.Error("can't publish article: " + err.Error())
			var mes model.ScrapperPayload
			err := json.Unmarshal(redisMessage, &mes)
			if err != nil {
				s.logger.Error("can't unmarshal message: " + err.Error())
			}
			fmt.Println(redisChanel, mes)
			return
		}

	})

	c.OnHTML("div.list-more", func(e *colly.HTMLElement) {
		link := e.Request.AbsoluteURL(e.Attr("href"))
		err := e.Request.Visit(link[:len(link)-9] + e.Attr("data-url")[1:])
		if err != nil {
			return
		}

	})
	c.OnHTML("div.list-items-loaded", func(e *colly.HTMLElement) {
		link := e.Request.AbsoluteURL(e.Attr("href"))
		next := e.Attr("data-next-url")

		if next != "" {
			err := e.Request.Visit(link[:len(link)-9] + next[1:])
			if err != nil {
				return
			}
		}
	})
	c.OnRequest(func(request *colly.Request) {
		fmt.Println(request.ProxyURL, request.URL, counter)
	})

	if err := c.Visit("https://ria.ru/" + day + "/"); err != nil {
		return fmt.Errorf("can't start scrapping: %w", err)
	}

	c.Wait()

	date, _ := time.Parse("20060102", day)

	s.logger.Info("scraped", slog.String("date", date.Format("02.01.2006")), slog.Int("count", counter))

	//for k, v := range articles {
	//	fmt.Println(k + ": " + v)
	//}
	//fmt.Println(len(articles))
	//for k, v := range articles {
	//	err = s.rdb.Publish(ctx, "scrapper", articlesDate[k].Format(time.RFC3339)+" "+strings.Join(v, " ")).Err()
	//	if err != nil {
	//		return fmt.Errorf("cann't publish article: %w", err)
	//	}
	//}

	return nil
}
