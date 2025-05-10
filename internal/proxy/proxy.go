package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vhlebnikov/colly/v2"
)

type Command int

const (
	Add Command = iota + 1
	Delete
)

const RecoverTimeOut = 5 * time.Minute

type CommandMessage struct {
	Cmd Command
	Url *url.URL
}

type MyRoundRobinSwitcher struct {
	proxyURLs []*url.URL
	index     uint32
	mu        sync.RWMutex

	logger        *slog.Logger
	cmdChan       chan CommandMessage
	haveProxyChan chan struct{}
	recoverPool   []*url.URL

	proxyRecoverTimeOut time.Duration
}

func MyRoundRobinProxySwitcher(proxies string, log *slog.Logger, proxyRecoverTimeOutSeconds int) (*MyRoundRobinSwitcher, error) {

	if len(proxies) == 0 {
		return nil, fmt.Errorf("no proxy")
	}
	proxyUrls := strings.Split(proxies, ",")
	for i, proxy := range proxyUrls {
		proxyUrls[i] = "http://" + proxy
	}

	r := &MyRoundRobinSwitcher{
		proxyURLs:           make([]*url.URL, len(proxyUrls)),
		cmdChan:             make(chan CommandMessage, 10),
		haveProxyChan:       make(chan struct{}),
		recoverPool:         make([]*url.URL, 0, len(proxyUrls)),
		logger:              log,
		proxyRecoverTimeOut: time.Duration(proxyRecoverTimeOutSeconds) * time.Second,
	}

	for i, u := range proxyUrls {
		parsedU, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		r.proxyURLs[i] = parsedU
	}

	return r, nil
}

func (r *MyRoundRobinSwitcher) GetProxy(pr *http.Request) (*url.URL, error) {
	for {
		r.mu.RLock()
		proxyCount := len(r.proxyURLs)
		r.mu.RUnlock()

		if proxyCount > 0 {
			break
		}

		r.logger.Info("waiting for proxy")
		<-r.haveProxyChan
	}

	r.mu.RLock()
	index := atomic.AddUint32(&r.index, 1) - 1
	u := r.proxyURLs[index%uint32(len(r.proxyURLs))]
	r.mu.RUnlock()

	uStr := u.String()

	ctx := context.WithValue(pr.Context(), colly.ProxyURLKey, uStr)
	*pr = *pr.WithContext(ctx)
	pr.Header.Set(colly.ProxyUrlHeader, uStr)
	return u, nil
}

func (r *MyRoundRobinSwitcher) GetCmdChan() chan<- CommandMessage {
	return r.cmdChan
}

func (r *MyRoundRobinSwitcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-r.cmdChan:
			if !ok {
				return
			}
			switch v.Cmd {
			case Add:
				func() {
					r.mu.Lock()
					defer r.mu.Unlock()
					r.logger.Info("trying to add proxy", slog.String("url", v.Url.String()),
						slog.Int("proxy accessible", len(r.proxyURLs)),
						slog.Int("proxy inaccessible", len(r.recoverPool)))

					for i := range r.recoverPool {
						if r.proxyURLs[i].String() == v.Url.String() {
							r.proxyURLs = append(r.proxyURLs, r.recoverPool[i])
							if len(r.recoverPool) == 1 {
								r.recoverPool = []*url.URL{}
							} else {
								r.recoverPool = append(r.recoverPool[:i], r.recoverPool[i+1:]...)
							}

							r.logger.Info("Add proxy to proxyURLs", slog.String("url", v.Url.String()),
								slog.Int("proxy accessible", len(r.proxyURLs)),
								slog.Int("proxy inaccessible", len(r.recoverPool)))
							if len(r.proxyURLs) == 1 {
								r.haveProxyChan <- struct{}{}
							}
							break
						}

					}
				}()
			case Delete:
				func() {
					r.mu.Lock()
					defer r.mu.Unlock()
					r.logger.Info("trying to delete proxy",
						slog.String("proxy", v.Url.Host),
						slog.Int("proxy accessible", len(r.proxyURLs)),
						slog.Int("proxy inaccessible", len(r.recoverPool)))

					for i := range r.proxyURLs {
						if r.proxyURLs[i].String() == v.Url.String() {
							r.recoverPool = append(r.recoverPool, r.proxyURLs[i])
							if len(r.proxyURLs) == 1 {
								r.proxyURLs = []*url.URL{}
							} else {
								r.proxyURLs = append(r.proxyURLs[:i], r.proxyURLs[i+1:]...)
							}
							r.logger.Info("Delete proxy from proxyURLs", slog.String("proxy", v.Url.Host),
								slog.Int("proxy accessible", len(r.proxyURLs)),
								slog.Int("proxy inaccessible", len(r.recoverPool)))
							break
						}
					}
				}()
			}
		}
	}
}

func (r *MyRoundRobinSwitcher) RunForRecover(ctx context.Context) {
	ticker := time.NewTicker(RecoverTimeOut)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.RLock()
			poolCopy := make([]*url.URL, len(r.recoverPool))
			copy(poolCopy, r.recoverPool)
			r.mu.RUnlock()
			for _, proxy := range poolCopy {
				func(proxy *url.URL) {
					transport := &http.Transport{
						Proxy: http.ProxyURL(proxy),
					}

					client := &http.Client{
						Transport: transport,
						Timeout:   10 * time.Second,
					}

					resp, err := client.Get("https://ria.ru")
					if err != nil {
						r.logger.Info("proxy still unable", slog.String("proxy", proxy.Host), slog.String("error", err.Error()))
					}
					if err == nil && resp.StatusCode == http.StatusOK {
						r.cmdChan <- CommandMessage{
							Cmd: Add,
							Url: proxy,
						}
						resp.Body.Close()
					}
				}(proxy)
			}
		}
	}
}
