package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ListOffsetsVersion(ctx *NetworkContext, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	if !b.checkSasl(ctx) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	logrus.Debugf("list offset req: %+v", req)
	resp := make([]*codec.ListOffsetsTopicResp, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !b.checkSaslTopic(ctx, topicReq.Topic, constant.ConsumerPermissionType) {
			return nil, fmt.Errorf("check topic read permission failed: %s", topicReq.Topic)
		}
		f := &codec.ListOffsetsTopicResp{
			Topic:             topicReq.Topic,
			PartitionRespList: make([]*codec.ListOffsetsPartitionResp, len(topicReq.PartitionReqList)),
		}
		for j, partitionReq := range topicReq.PartitionReqList {
			var err error
			f.PartitionRespList[j], err = b.OffsetListPartitionAction(ctx.Addr, f.Topic, req.ClientId, partitionReq)
			if err != nil {
				return nil, fmt.Errorf("offset list partition failed: %v", err)
			}
		}
		resp[i] = f
	}
	return &codec.ListOffsetsResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		TopicRespList: resp,
	}, nil
}
