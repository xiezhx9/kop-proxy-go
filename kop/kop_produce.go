package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactProduce(ctx *NetworkContext, req *codec.ProduceReq, config *Config) (*codec.ProduceResp, error) {
	if !b.checkSasl(ctx) {
		return nil, fmt.Errorf("connection is not authed")
	}
	logrus.Debugf("produce req: %+v", req)
	result := &codec.ProduceResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		TopicRespList: make([]*codec.ProduceTopicResp, len(req.TopicReqList)),
	}
	for i, topicReq := range req.TopicReqList {
		if !b.checkSaslTopic(ctx, topicReq.Topic, constant.ProducerPermissionType) {
			return nil, fmt.Errorf("check topic write permission failed: %v", topicReq.Topic)
		}
		f := &codec.ProduceTopicResp{
			Topic:             topicReq.Topic,
			PartitionRespList: make([]*codec.ProducePartitionResp, 0),
		}
		for _, partitionReq := range topicReq.PartitionReqList {
			partition, err := b.ProduceAction(ctx.Addr, topicReq.Topic, partitionReq.PartitionId, partitionReq)
			if err != nil {
				return nil, fmt.Errorf("produce failed: %v", err)
			}
			if partition != nil {
				f.PartitionRespList = append(f.PartitionRespList, partition)
			}
		}
		result.TopicRespList[i] = f
	}
	return result, nil
}
