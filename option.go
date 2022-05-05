package tdquery

import (
	"net"
	"net/http"
	"time"
)

type Option func(c *Client)

func WithBrokers(brokers []string) Option {
	return func(c *Client) {
		c.brokers = brokers
	}
}

func WithPort(port int) Option {
	return func(c *Client) {
		c.port = port
	}
}

func WithBasicAuth(username, password string) Option {
	return func(c *Client) {
		c.h.SetBasicAuth(username, password)
	}
}

func WithQueryTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.h.SetTimeout(timeout)
	}
}

func createTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: -1,
		DualStack: true,
	}
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         dialer.DialContext,
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     255,
		// todo use keepalive
		DisableKeepAlives: true,
	}
}

func WithDatabase(db string) Option {
	return func(c *Client) {
		c.database = db
	}
}

// WithUrlDatabase choose database in url like `/rest/sqlt/dbname`.
// Should only be used when TDengine version is greater than 2.2.0.0
func WithUrlDatabase() Option {
	return func(c *Client) {
		c.useUrlDB = true
	}
}
