package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactJoinGroup(ctx *NetworkContext, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	if !b.checkSaslGroup(ctx, req.GroupId) {
		return nil, fmt.Errorf("connection is not authed")
	}
	logrus.Debugf("join group req: %+v", req)
	lowResp, err := b.GroupJoinAction(ctx.Addr, req)
	if err != nil {
		return nil, fmt.Errorf("join group failed: %v", err)
	}
	resp := &codec.JoinGroupResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		ErrorCode:    lowResp.ErrorCode,
		GenerationId: lowResp.GenerationId,
		ProtocolType: lowResp.ProtocolType,
		ProtocolName: lowResp.ProtocolName,
		LeaderId:     lowResp.LeaderId,
		MemberId:     lowResp.MemberId,
		Members:      lowResp.Members,
	}

	logrus.Debugf("resp: %+v", resp)
	return resp, nil
}
