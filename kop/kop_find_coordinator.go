package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactFindCoordinator(req *codec.FindCoordinatorReq, config *Config) (*codec.FindCoordinatorResp, error) {
	logrus.Debug("req ", req)
	resp := &codec.FindCoordinatorResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		NodeId: config.NodeId,
		Host:   config.AdvertiseHost,
		Port:   config.AdvertisePort,
	}
	logrus.Debug("resp ", resp)
	return resp, nil
}
