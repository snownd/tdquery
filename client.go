package tdquery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-resty/resty/v2"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const queryURL = "/rest/sqlt"
const maxUint64 uint64 = 18446744073709551615

type Client struct {
	h                   *resty.Client
	brokers             []string
	port                int
	healthCheckInterval time.Duration
	brokerStatus        []*brokerStatus
	done                chan struct{}
	lock                sync.RWMutex
	database            string
	useUrlDB            bool
}

type brokerStatus struct {
	ready    bool
	count    uint64
	endPoint *tdengineEndPoint
}

type tdengineEndPoint struct {
	ep   string
	host string
	// not restful port
	port string
}

type tdengineDnode struct {
	ID            int16
	EndPoint      string `mapstructure:"end_point"`
	VNodes        int16
	Cores         int16
	Status        string
	Role          string
	CreateTime    string `mapstructure:"create_time"`
	OfflineReason string `mapstructure:"offline_reason"`
}

func newTdengineEndPoint(ep string) *tdengineEndPoint {
	index := strings.Index(ep, ":")
	host := ep[:strings.Index(ep, ":")]
	port := ep[index+1:]
	return &tdengineEndPoint{ep, host, port}
}

func NewClient(opts ...Option) *Client {
	const (
		defaultHealthCheckInterval = 15 * time.Second
		defaultTimeout             = 30 * time.Second
		defaultMaxSockets          = 10
		defaultPort                = 6041
	)
	defaultBrokers := []string{"localhost"}
	client := &Client{
		h:                   resty.New().SetTimeout(defaultTimeout).SetDisableWarn(true).SetTransport(createTransport()),
		brokers:             defaultBrokers,
		port:                defaultPort,
		healthCheckInterval: defaultHealthCheckInterval,
		brokerStatus:        make([]*brokerStatus, 0),
		done:                make(chan struct{}),
	}
	for _, opt := range opts {
		opt(client)
	}
	return client
}

func (c *Client) Connect(ctx context.Context) error {
	for _, broker := range c.brokers {

		ret, err := c.request(ctx, broker, "show dnodes")
		if err != nil || ret.Code != 0 {
			fmt.Println("try connect to broker:", broker, "failed", err, ret.Code, ret.Message)
			continue
		}
		for _, node := range ret.Data {
			if node["role"].(string) == "arb" {
				continue
			}
			endPoint := newTdengineEndPoint(node["end_point"].(string))
			c.brokerStatus = append(c.brokerStatus, &brokerStatus{
				ready:    node["status"].(string) == "ready",
				count:    0,
				endPoint: endPoint,
			})
		}
		go c.check()
		return nil
	}
	return ErrorNoAvailableBroker
}

func (c *Client) Close(ctx context.Context) error {
	close(c.done)
	return nil
}

func (c *Client) Query(ctx context.Context, sql string, params ...interface{}) (*QueryResult, error) {
	broker, ok := c.pickAliveBroker()
	if !ok {
		return nil, ErrorNoAvailableBroker
	}
	fullSQL, err := interpolate(sql, params)
	if err != nil {
		return nil, err
	}
	return c.request(ctx, broker, fullSQL)
}

func (c *Client) NewSelectQueryBuilder() *SelectQueryBuilder {
	if c.useUrlDB {
		return &SelectQueryBuilder{
			QueryBuilder: QueryBuilder{
				c: c,
			},
		}
	}
	return &SelectQueryBuilder{
		QueryBuilder: QueryBuilder{
			c:        c,
			database: c.database,
		},
	}
}

func (c *Client) request(ctx context.Context, broker string, sql string) (*QueryResult, error) {
	res, err := c.h.
		R().
		SetContext(ctx).
		SetHeader("Content-Type", "text/plain").
		SetBody(sql).
		Post(c.newReqUrl(broker))
	if err != nil {
		return nil, err
	}
	rawRet := &rawQueryResult{}
	if err = json.Unmarshal(res.Body(), rawRet); err != nil {
		return nil, err
	}
	qr := NewQueryResult(rawRet, sql, res.Time())
	return qr, nil
}

func (c *Client) pickAliveBroker() (string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	min := maxUint64
	index := -1
	for i, s := range c.brokerStatus {
		if s.ready {
			if s.count <= min {
				min = s.count
				index = i
			}
		}
	}
	if index == -1 {
		return "", false
	}
	bs := c.brokerStatus[index]
	atomic.AddUint64(&bs.count, 1)
	return bs.endPoint.host, true
}

func (c *Client) newReqUrl(broker string) string {
	if c.useUrlDB {
		return fmt.Sprintf("http://%s:%d%s/%s", broker, c.port, queryURL, c.database)
	}
	return fmt.Sprintf("http://%s:%d%s", broker, c.port, queryURL)
}

func (c *Client) check() {
	ticker := time.NewTicker(c.healthCheckInterval)
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			r, err := c.Query(ctx, "show dnodes")
			cancel()
			if err != nil || r.Code != 0 {
				continue
			}
			c.lock.Lock()
			for _, node := range r.Data {
				if node["role"].(string) != "arb" && node["status"].(string) != "ready" {
					ep := node["end_point"].(string)
					for i, status := range c.brokerStatus {
						if status.endPoint.ep == ep {
							c.brokerStatus[i].ready = false
						}
					}
				}
			}
			c.lock.Unlock()

		case <-c.done:
			return
		}
	}
}
