package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactSyncGroup(ctx *NetworkContext, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	if !b.checkSaslGroup(ctx, req.GroupId) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	logrus.Debugf("sync group req: %+v", req)
	lowResp, err := b.GroupSyncAction(ctx.Addr, req)
	if err != nil {
		return nil, fmt.Errorf("group sync failed: %v", err)
	}
	return &codec.SyncGroupResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		ThrottleTime:     lowResp.ThrottleTime,
		ErrorCode:        lowResp.ErrorCode,
		ProtocolType:     lowResp.ProtocolType,
		ProtocolName:     lowResp.ProtocolName,
		MemberAssignment: lowResp.MemberAssignment,
	}, nil
}
