package gudu

import (
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/MediaMath/gudu/services/beeswax"
	impala "github.com/MediaMath/gudu/services/impalaservice"
	"log"
)

type Options struct {
	PollIntervalSeconds float64
	BatchSize           int64
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 0.1, BatchSize: 10000}
)

type Connection struct {
	ctx       context.Context
	client    *impala.ImpalaServiceClient
	handle    *beeswax.QueryHandle
	transport thrift.TTransport
	options   Options
}

func Connect(ctx context.Context, host string, port int, options Options) (*Connection, error) {
	socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}

	transportFactory := thrift.NewTBufferedTransportFactory(24 * 1024 * 1024)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, _ := transportFactory.GetTransport(socket)

	if err := transport.Open(); err != nil {
		return nil, err
	}

	client := impala.NewImpalaServiceClientFactory(transport, protocolFactory)

	return &Connection{ctx, client, nil, transport, options}, nil
}

func (c *Connection) isOpen() bool {
	return c.client != nil
}

func (c *Connection) Close() error {
	if c.isOpen() {
		if c.handle != nil {
			status, err := c.client.Cancel(c.ctx, c.handle)
			if err != nil {
				return err
			} else {
				log.Println(status)
			}
			c.handle = nil
		}

		c.transport.Close()
		c.client = nil
	}
	return nil
}

func (c *Connection) Query(ctx context.Context, query string) (RowSet, error) {
	bquery := beeswax.Query{}

	bquery.Query = query
	bquery.Configuration = []string{}

	handle, err := c.client.Query(ctx, &bquery)

	if err != nil {
		return nil, err
	}

	return newRowSet(ctx, c.client, handle, c.options), nil
}
