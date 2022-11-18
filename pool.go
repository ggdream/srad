package srad

import (
	"errors"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	ErrConnClosed = errors.New("grpc: conn closed")
)

type pool struct {
	target string

	size   uint32
	cursor uint32

	clients []*grpc.ClientConn
	fn      func() (*grpc.ClientConn, error)

	mutex sync.Mutex
}

func newPool(fn func() (*grpc.ClientConn, error), target string, size uint32) (*pool, error) {
	clients := make([]*grpc.ClientConn, 0, size)
	for i := 0; i < int(size); i++ {
		cc, err := fn()
		if err != nil {
			return nil, err
		}
		clients = append(clients, cc)
	}

	return &pool{
		target:  target,
		size:    size,
		clients: clients,
		fn:      fn,
	}, nil
}

func (p *pool) Get() (*grpc.ClientConn, error) {
	idx := atomic.AddUint32(&p.cursor, 1) % p.size
	cc := p.clients[idx]
	if cc != nil && p.checkState(cc) == nil {
		return cc, nil
	}
	if cc != nil {
		cc.Close()
		p.clients[idx] = nil
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()
	cc = p.clients[idx]
	if cc != nil && p.checkState(cc) == nil {
		return cc, nil
	}

	cc, err := p.fn()
	if err != nil {
		return nil, err
	}
	p.clients[idx] = cc

	return cc, nil
}

func (p *pool) checkState(cc *grpc.ClientConn) error {
	switch cc.GetState() {
	case connectivity.TransientFailure, connectivity.Shutdown:
		return ErrConnClosed
	default:
		return nil
	}
}
