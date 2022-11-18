package srad

import (
	"context"
	"net"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/v3"
)

func register(ctx context.Context, scheme, service, host string, port int, endpoints []string, ttl, weight int64) (*etcdRegistry, error) {
	r := new(etcdRegistry)
	err := r.Register(ctx, scheme, service, host, port, endpoints, ttl, weight)
	if err != nil {
		return nil, err
	}

	return r, nil
}

type etcdRegistry struct {
	client  *clientv3.Client
	leaseID clientv3.LeaseID
}

func (r *etcdRegistry) Register(ctx context.Context, scheme, service, host string, port int, endpoints []string, ttl, weight int64) error {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 3,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	r.client = client

	grantRes, err := client.Grant(ctx, ttl)
	if err != nil {
		return err
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))
	_, err = client.Put(ctx, joinFullPath(scheme, service, address, weight), address, clientv3.WithLease(grantRes.ID))
	if err != nil {
		return err
	}
	res, err := client.KeepAlive(context.TODO(), grantRes.ID)
	if err != nil {
		return err
	}
	r.leaseID = grantRes.ID

	go func() {
		for {
			_, ok := <-res
			if !ok {
				return
			}
		}
	}()

	return nil
}

func (r *etcdRegistry) Unregister() error {
	_, err := r.client.Revoke(context.Background(), r.leaseID)
	if err != nil {
		return err
	}

	return r.client.Close()
}
