package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
)

func (b *Broker) OffsetForLeaderEpochVersion(ctx *NetworkContext, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	if !b.checkSasl(ctx) {
		return nil, fmt.Errorf("connection is not authed")
	}
	resp := make([]*codec.OffsetForLeaderEpochTopicResp, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !b.checkSaslTopic(ctx, topicReq.Topic, constant.ConsumerPermissionType) {
			return nil, fmt.Errorf("check topic read permission failed: %s", topicReq.Topic)
		}
		f := &codec.OffsetForLeaderEpochTopicResp{
			Topic:             topicReq.Topic,
			PartitionRespList: make([]*codec.OffsetForLeaderEpochPartitionResp, 0),
		}
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, err := b.OffsetLeaderEpochAction(ctx.Addr, topicReq.Topic, partitionReq)
			if err != nil {
				return nil, fmt.Errorf("offset leader epoch failed: %v", err)
			}
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		resp[i] = f
	}

	return &codec.OffsetForLeaderEpochResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		TopicRespList: resp,
	}, nil
}
