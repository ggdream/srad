package srad

import (
	"fmt"

	"google.golang.org/grpc/resolver"
)

func joinFullPath(scheme, service, address string, weight int64) string {
	return fmt.Sprintf("%s:///%s/%s?weight=%d", scheme, service, address, weight)
}

func joinPathPrefix(scheme, service string) string {
	return fmt.Sprintf("%s:///%s/", scheme, service)
}

func exist(addrs []resolver.Address, target string) bool {
	for _, addr := range addrs {
		if addr.Addr == target {
			return true
		}
	}

	return false
}

func delete(addrs []resolver.Address, target string) ([]resolver.Address, bool) {
	for i, addr := range addrs {
		if addr.Addr == target {
			return append(addrs[:i], addrs[i+1:]...), true
		}
	}

	return nil, false
}
