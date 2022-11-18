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

func Register(ctx context.Context, scheme, service, host string, port int, weight int64, endpoints []string) (*etcdRegistry, error) {
	return register(ctx, scheme, service, host, port, endpoints, 10, weight)
}

func Discover(scheme, service string, endpoints []string) (*grpc.ClientConn, error) {
	prefix := joinPathPrefix(scheme, service)
	curPool, ok := p[prefix]
	if !ok {
		err := genPool(scheme, service, endpoints)
		if err != nil {
			return nil, err
		}
		curPool = p[prefix]
	}

	return curPool.Get()
}

func genPool(scheme, service string, endpoints []string) error {
	locker.Lock()
	defer locker.Unlock()

	prefix := joinPathPrefix(scheme, service)
	if _, ok := p[prefix]; !ok {
		onePool, err := discoverPool(scheme, service, endpoints)
		if err != nil {
			return err
		}

		p[prefix] = onePool
	}

	return nil
}
