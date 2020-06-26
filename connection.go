package impalathing

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/koblas/impalathing/services/beeswax"
	impala "github.com/koblas/impalathing/services/impalaservice"
)

type Option func(*Options)

func WithConnectionTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ConnectionTimeout = timeout
	}
}

func WithPollInterval(pollIntervalSeconds float64) Option {
	return func(o *Options) {
		o.PollIntervalSeconds = pollIntervalSeconds
	}
}

func WithBatchSize(bs int64) Option {
	return func(o *Options) {
		o.BatchSize = bs
	}
}

func WithPlainSaslTransport(username, password string) Option {
	return func(o *Options) {
		o.SaslTransportConfig = map[string]string{
			"mechanismName": "PLAIN",
			"username":      username,
			"password":      password,
		}
	}
}

func WithGSSAPISaslTransport() Option {
	return func(o *Options) {
		o.SaslTransportConfig = map[string]string{
			"mechanismName": "GSSAPI",
			"service":       "impala",
		}
	}
}

type Options struct {
	PollIntervalSeconds float64
	BatchSize           int64
	ConnectionTimeout   time.Duration
	SaslTransportConfig map[string]string
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 0.1, BatchSize: 10000, ConnectionTimeout: 10000 * time.Millisecond}
)

type Connection struct {
	client    *impala.ImpalaServiceClient
	handle    *beeswax.QueryHandle
	transport thrift.TTransport
	options   Options
}

func Connect(host string, port int, opts ...Option) (*Connection, error) {
	var options = DefaultOptions
	for _, opt := range opts {
		opt(&options)
	}

	socket, err := thrift.NewTSocketTimeout(fmt.Sprintf("%s:%d", host, port), options.ConnectionTimeout)

	if err != nil {
		return nil, err
	}

	var transport thrift.TTransport
	if options.SaslTransportConfig != nil {
		transport, err = NewTSaslTransport(socket, host, options.SaslTransportConfig)
		if err != nil {
			return nil, err
		}
	} else {
		transportFactory := thrift.NewTBufferedTransportFactory(24 * 1024 * 1024)
		transport, _ = transportFactory.GetTransport(socket)
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	if err := transport.Open(); err != nil {
		return nil, err
	}

	client := impala.NewImpalaServiceClientFactory(transport, protocolFactory)

	return &Connection{client, nil, transport, options}, nil
}

func (c *Connection) isOpen() bool {
	return c.client != nil
}

func (c *Connection) Close() error {
	if c.isOpen() {
		if c.handle != nil {
			_, err := c.client.Cancel(context.Background(), c.handle)
			if err != nil {
				return err
			}
			c.handle = nil
		}

		c.transport.Close()
		c.client = nil
	}
	return nil
}

func (c *Connection) query(ctx context.Context, query string) (RowSet, error) {
	bquery := beeswax.Query{}

	bquery.Query = query
	bquery.Configuration = []string{}

	handle, err := c.client.Query(ctx, &bquery)

	if err != nil {
		return nil, err
	}

	return newRowSet(c.client, handle, c.options), nil
}

func (c *Connection) Query(query string) (RowSet, error) {
	return c.query(context.Background(), query)
}

func (c *Connection) QueryWithContext(ctx context.Context, query string) (RowSet, error) {
	return c.query(ctx, query)
}
