package ria

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
	"github.com/vhlebnikov/colly/v2"

	"github.com/STTM-NSU/web-scrapper/internal/model"
	"github.com/STTM-NSU/web-scrapper/internal/proxy"
)

type Scrapper struct {
	rdb           *redis.Client
	logger        *slog.Logger
	proxySwitcher *proxy.MyRoundRobinSwitcher
	articles      sync.Map
	articlesDate  sync.Map

	redisChanelName string
	partitionsCount int
}

func NewScrapper(rdb *redis.Client,
	logger *slog.Logger,
	proxySwitcher *proxy.MyRoundRobinSwitcher,
	redisChanelName string,
	partitionsCount int) *Scrapper {
	return &Scrapper{
		rdb:             rdb,
		logger:          logger,
		proxySwitcher:   proxySwitcher,
		redisChanelName: redisChanelName,
		partitionsCount: partitionsCount,
	}
}

func (s *Scrapper) Scrap(ctx context.Context, day string) error {

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
	c.Context = ctx

	err := c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: runtime.GOMAXPROCS(-1),
		Delay:       100 * time.Millisecond,
		RandomDelay: 50 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("can't set limit %w", err)
	}

	c.SetProxyFunc(s.proxySwitcher.GetProxy)

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
		if _, ok := s.articlesDate.Load(e.Request.URL.String()); ok {
			s.logger.Error("duplicate url", slog.String("url", e.Request.URL.String()))
		}
		s.articlesDate.Store(e.Request.URL.String(), date)

	})

	c.OnHTML("div.article__title", func(e *colly.HTMLElement) {
		if text, ok := s.articles.Load(e.Request.URL.String()); ok {
			s.articles.Store(e.Request.URL.String(), append(text.([]string), e.Text))
		} else {
			s.articles.Store(e.Request.URL.String(), []string{e.Text})
		}
	})

	c.OnHTML("h1.article__title", func(e *colly.HTMLElement) {
		if text, ok := s.articles.Load(e.Request.URL.String()); ok {
			s.articles.Store(e.Request.URL.String(), append(text.([]string), e.Text))
		} else {
			s.articles.Store(e.Request.URL.String(), []string{e.Text})
		}
	})

	c.OnHTML("div.article__text", func(e *colly.HTMLElement) {
		if text, ok := s.articles.Load(e.Request.URL.String()); ok {
			s.articles.Store(e.Request.URL.String(), append(text.([]string), e.Text))
		} else {
			s.articles.Store(e.Request.URL.String(), []string{e.Text})
		}
	})
	c.OnHTML("div.recommend__place", func(e *colly.HTMLElement) {

		err := s.sendMessage(ctx, e.Request.URL.String())
		mutex.Lock()
		counter++
		mutex.Unlock()
		if err != nil {
			s.logger.Error("ria sendMessage: " + err.Error())
			return
		}
	})

	c.OnHTML("div.list-more", func(e *colly.HTMLElement) {
		link := e.Request.AbsoluteURL(e.Attr("href"))
		err := e.Request.Visit(link[:len(link)-9] + e.Attr("data-url")[1:])
		if err != nil {
			s.logger.Error("can't get more data: " + err.Error())
			return
		}

	})
	c.OnHTML("div.list-items-loaded", func(e *colly.HTMLElement) {
		link := e.Request.AbsoluteURL(e.Attr("href"))
		next := e.Attr("data-next-url")

		if next != "" {
			err := e.Request.Visit(link[:len(link)-9] + next[1:])
			if err != nil {
				s.logger.Error("can't visit article: " + err.Error())
				return
			}
		}
	})
	c.OnRequest(func(request *colly.Request) {
		// if !strings.Contains(request.URL.String(), "more.html") {
		//
		//	fmt.Println(request.ProxyURL, request.URL, counter)
		// }

	})
	c.OnError(func(response *colly.Response, err error) {
		if err == nil {
			return
		}
		s.logger.Error("can't visit article " + err.Error())
		// fmt.Println(response.Request.Ctx)

		if strings.Contains(err.Error(), "Too Many Requests") {
			time.Sleep(10 * time.Second)
			err = response.Request.Retry()
			if err != nil {
				s.logger.Error("can't retry: " + err.Error())
				return
			}
		} else if strings.Contains(err.Error(), "Bad Gateway") {
			pr, err := url.Parse(response.Request.ProxyURL)

			if err != nil {
				s.logger.Error("bad proxy: " + err.Error())
				return
			}
			s.proxySwitcher.GetCmdChan() <- proxy.CommandMessage{
				Cmd: proxy.Delete, Url: pr,
			}

		}
	})
	c.OnResponse(func(response *colly.Response) {
		if !strings.Contains(response.Request.URL.String(), "more.html") {

		}
		// fmt.Println(response.Request.ProxyURL)
	})

	date, err := time.Parse("20060102", day)
	if err != nil {
		s.logger.Error("bad date " + err.Error())
	}
	timeStart := time.Now()
	s.logger.Info("start scrapping day", slog.Time("date", date))
	if err := c.Visit("https://ria.ru/" + day + "/"); err != nil {
		return fmt.Errorf("can't start scrapping: %w", err)
	}

	c.Wait()

	duration := time.Now().Sub(timeStart).String()
	doneMessage, err := sonic.Marshal(model.DonePayload{
		Date:     date.Format("2006-01-02T15:00:00"),
		Count:    counter,
		Duration: duration,
	})

	if err != nil {
		return fmt.Errorf("can't marshal done message: %w", err)
	}
	s.rdb.Publish(ctx, s.redisChanelName+"_day_done", doneMessage)
	s.logger.Info("scraped",
		slog.String("date", date.Format("02.01.2006")),
		slog.Int("count", counter),
		slog.String("duration", duration))
	s.articlesDate.Clear()
	s.articles.Clear()

	return nil
}

func (s *Scrapper) sendMessage(ctx context.Context, url string) error {
	partition, err := getPartition(url, s.partitionsCount)
	if err != nil {
		return fmt.Errorf("can't get partition: %w", err)
	}
	redisChanel := s.redisChanelName + ":" + strconv.Itoa(partition)
	date, ok := s.articlesDate.Load(url)
	if !ok {
		return fmt.Errorf("no date " + url)
	}
	text, ok := s.articles.Load(url)
	if !ok {
		return fmt.Errorf("no text " + url)
	}
	redisMessage, err := sonic.Marshal(model.ScrapperPayload{
		Url:  url,
		Date: date.(time.Time).Format("2006-01-02T15:00:00"),
		Text: strings.Join(text.([]string), " "),
	})
	if err != nil {
		return fmt.Errorf("can't marshal message: %w", err)
	}
	err = s.rdb.Publish(ctx, redisChanel, redisMessage).Err()
	if err != nil {
		return fmt.Errorf("can't publish article: %w", err)
	}
	return nil
}

func getPartition(url string, partitionN int) (int, error) {
	h := fnv.New32()
	if _, err := h.Write([]byte(url)); err != nil {
		return 0, fmt.Errorf("%w: can't write url", err)
	}
	return int(h.Sum32()) % partitionN, nil
}
