package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactHeartbeat(heartbeatReqV4 *codec.HeartbeatReq, context *NetworkContext) (*codec.HeartbeatResp, error) {
	logrus.Debugf("heartbeat req: %+v", heartbeatReqV4)
	beat := b.HeartBeatAction(context.Addr, *heartbeatReqV4)
	return &codec.HeartbeatResp{
		BaseResp: codec.BaseResp{
			CorrelationId: heartbeatReqV4.CorrelationId,
		},
		ErrorCode: beat.ErrorCode,
	}, nil
}
