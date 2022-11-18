package srad

import (
	"errors"
	"strconv"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const (
	smoothWeightedRoundRobin = "smooth_weighted_round_robin"
)

var (
	_ base.PickerBuilder = (*wrrPickerBuilder)(nil)
	_ balancer.Picker    = (*wrrPicker)(nil)

	ErrNoPickerResult = errors.New("picker: no result")
)

func init() {
	balancer.Register(newSmoothWRRBuilder())
}

func newSmoothWRRBuilder() balancer.Builder {
	return base.NewBalancerBuilder(smoothWeightedRoundRobin, new(wrrPickerBuilder), base.Config{HealthCheck: true})
}

type wrrPickerBuilder struct{}

func (w *wrrPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	scs := make([]*connInfo, 0, len(info.ReadySCs))
	var totalWeight, weight int64 = 0, 0
	var err error
	for sc, sci := range info.ReadySCs {
		weightStr := sci.Address.BalancerAttributes.Value("weight").(string)
		if weightStr == "" {
			weight = 1
		} else {
			weight, err = strconv.ParseInt(weightStr, 10, 64)
			if err != nil {
				return base.NewErrPicker(err)
			}
		}
		totalWeight += weight

		ci := connInfo{
			sc:      sc,
			weight:  weight,
			current: weight,
		}
		scs = append(scs, &ci)
	}

	return &wrrPicker{
		connInfos: scs,
		weight:    totalWeight,
	}
}

type connInfo struct {
	sc balancer.SubConn

	weight  int64
	current int64
}

type wrrPicker struct {
	connInfos []*connInfo
	weight    int64
}

func (w *wrrPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var bigest *connInfo
	for _, v := range w.connInfos {
		v.current += v.weight

		if bigest == nil || v.current > bigest.current {
			bigest = v
		}
	}
	bigest.current -= w.weight

	return balancer.PickResult{
		SubConn: bigest.sc,
	}, nil
}
