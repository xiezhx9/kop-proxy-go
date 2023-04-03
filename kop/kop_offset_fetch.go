package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/sirupsen/logrus"
)

func (b *Broker) OffsetFetchVersion(ctx *NetworkContext, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	if !b.checkSasl(ctx) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	logrus.Debugf("offset fetch req: %+v", req)
	resp := &codec.OffsetFetchResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		TopicRespList: make([]*codec.OffsetFetchTopicResp, len(req.TopicReqList)),
	}
	for i, topicReq := range req.TopicReqList {
		if !b.authGroupTopic(topicReq.Topic, req.GroupId) {
			return nil, fmt.Errorf("auth group topic failed, topic: %s, group: %s", topicReq.Topic, req.GroupId)
		}
		if !b.checkSaslTopic(ctx, topicReq.Topic, constant.ConsumerPermissionType) {
			return nil, fmt.Errorf("check topic read permission failed: %s", topicReq.Topic)
		}
		f := &codec.OffsetFetchTopicResp{
			Topic:             topicReq.Topic,
			PartitionRespList: make([]*codec.OffsetFetchPartitionResp, 0),
		}
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, err := b.OffsetFetchAction(ctx.Addr, topicReq.Topic, req.ClientId, req.GroupId, partitionReq)
			if err != nil {
				return nil, fmt.Errorf("offset fetch failed: %v", err)
			}
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		resp.TopicRespList[i] = f
	}

	return resp, nil
}
