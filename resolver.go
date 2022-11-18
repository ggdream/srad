package srad

import (
	"context"
	"fmt"
	"net/url"
	"time"
	"unsafe"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

var (
	_ resolver.Builder  = (*etcdBuilder)(nil)
	_ resolver.Resolver = (*etcdResolver)(nil)
)

// func discover(scheme, service, host string, port int, endpoints []string) (*grpc.ClientConn, error) {
// 	builder := &etcdBuilder{
// 		scheme:    scheme,
// 		service:   service,
// 		address:   net.JoinHostPort(host, strconv.Itoa(port)),
// 		endpoints: endpoints,
// 	}
// 	resolver.Register(builder)

// 	return grpc.Dial(joinPathPrefix(service, service), grpc.WithTransportCredentials(insecure.NewCredentials()))
// }

func discoverPool(scheme, service string, endpoints []string) (*pool, error) {
	builder := &etcdBuilder{
		scheme:    scheme,
		service:   service,
		endpoints: endpoints,
	}
	resolver.Register(builder)

	return newPool(
		func() (*grpc.ClientConn, error) {
			return grpc.Dial(
				joinPathPrefix(service, service),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": %s}`, smoothWeightedRoundRobin)),
			)
		},
		1<<2,
	)
}

type etcdBuilder struct {
	scheme, service string
	endpoints       []string
}

func (e *etcdBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	client, addrs, err := e.initEtcD()
	if err != nil {
		return nil, err
	}
	err = cc.UpdateState(resolver.State{Addresses: addrs})
	if err != nil {
		return nil, err
	}

	r := &etcdResolver{
		cc:     cc,
		prefix: joinPathPrefix(e.scheme, e.service),
		client: client,
	}
	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

func (e *etcdBuilder) initEtcD() (*clientv3.Client, []resolver.Address, error) {
	cfg := clientv3.Config{
		Endpoints:   e.endpoints,
		DialTimeout: time.Second * 3,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	res, err := client.Get(ctx, joinPathPrefix(e.scheme, e.service), clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	addrs := make([]resolver.Address, 0, len(res.Kvs))
	for _, kv := range res.Kvs {
		u, err := url.Parse(*(*string)(unsafe.Pointer(&kv.Key)))
		if err != nil {
			return nil, nil, err
		}
		weight := u.Query().Get("weight")

		addr := resolver.Address{
			Addr:               *(*string)(unsafe.Pointer(&kv.Value)),
			BalancerAttributes: attributes.New("weight", weight),
		}
		addrs = append(addrs, addr)
	}

	return client, addrs, nil
}

func (e *etcdBuilder) Scheme() string { return e.scheme }

type etcdResolver struct {
	cc         resolver.ClientConn
	prefix     string
	client     *clientv3.Client
	addrs      []resolver.Address
	cancelFunc context.CancelFunc
}

func (e *etcdResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	ctx, cancel := context.WithCancel(context.TODO())
	e.cancelFunc = cancel

	go e.watch(ctx)
}

func (e *etcdResolver) watch(ctx context.Context) {
	for v := range e.client.Watch(ctx, e.prefix, clientv3.WithPrefix()) {
		if v.Canceled {
			return
		}

		for _, event := range v.Events {
			addr := *(*string)(unsafe.Pointer(&event.Kv.Value))

			switch event.Type {
			case mvccpb.PUT:
				if !exist(e.addrs, addr) {
					u, err := url.Parse(*(*string)(unsafe.Pointer(&event.Kv.Key)))
					if err != nil {
						return
					}
					weight := u.Query().Get("weight")

					rAddr := resolver.Address{
						Addr:               addr,
						BalancerAttributes: attributes.New("weight", weight),
					}
					e.addrs = append(e.addrs, rAddr)
					if err := e.updateState(); err != nil {
						return
					}
				}
			case mvccpb.DELETE:
				if addrs, ok := delete(e.addrs, addr); ok {
					e.addrs = addrs
					if err := e.updateState(); err != nil {
						return
					}
				}
			}
		}
	}
}

func (e *etcdResolver) updateState() error {
	state := resolver.State{Addresses: e.addrs}
	return e.cc.UpdateState(state)
}

func (e *etcdResolver) Close() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.client.Close()
}
