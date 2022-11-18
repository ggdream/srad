package srad

import (
	"context"
	"sync"

	"google.golang.org/grpc"
)

var (
	p      = make(map[string]*pool)
	locker sync.Mutex
)

func Register(ctx context.Context, schema, service, host string, port int, weight int64, endpoints []string) (*etcdRegistry, error) {
	return register(ctx, schema, service, host, port, endpoints, 10, weight)
}

func Discover(schema, service string, endpoints []string) (*grpc.ClientConn, error) {
	prefix := joinPathPrefix(schema, service)
	curPool, ok := p[prefix]
	if !ok {
		err := genPool(schema, service, endpoints)
		if err != nil {
			return nil, err
		}
		curPool = p[prefix]
	}

	return curPool.Get()
}

func genPool(schema, service string, endpoints []string) error {
	locker.Lock()
	defer locker.Unlock()

	prefix := joinPathPrefix(schema, service)
	if _, ok := p[prefix]; !ok {
		onePool, err := discoverPool(schema, service, endpoints)
		if err != nil {
			return err
		}

		p[prefix] = onePool
	}

	return nil
}
