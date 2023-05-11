package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactLeaveGroup(ctx *NetworkContext, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	if !b.checkSaslGroup(ctx, req.GroupId) {
		return nil, fmt.Errorf("connection is not authed")
	}
	logrus.Debugf("leave group req: %+v", req)

	lowResp, err := b.GroupLeaveAction(ctx.Addr, req)
	if err != nil {
		return nil, fmt.Errorf("leave group failed: %v", err)
	}
	return &codec.LeaveGroupResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		ErrorCode:       lowResp.ErrorCode,
		ThrottleTime:    0,
		Members:         lowResp.Members,
		MemberErrorCode: lowResp.MemberErrorCode,
	}, nil
}
