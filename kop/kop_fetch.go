package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactFetch(ctx *NetworkContext, req *codec.FetchReq) (*codec.FetchResp, error) {
	if !b.checkSasl(ctx) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	logrus.Debugf("fetch req: %+v", req)
	for _, topicReq := range req.TopicReqList {
		if !b.checkSaslTopic(ctx, topicReq.Topic, constant.ConsumerPermissionType) {
			return nil, fmt.Errorf("check topic read permission failed: %s", topicReq.Topic)
		}
	}
	lowTopicRespList, err := b.FetchAction(ctx.Addr, req)
	if err != nil {
		return nil, fmt.Errorf("fetch failed: %v", err)
	}
	resp := codec.NewFetchResp(req.CorrelationId)
	resp.TopicRespList = lowTopicRespList
	for i, lowTopicResp := range lowTopicRespList {
		for _, p := range lowTopicResp.PartitionRespList {
			if p.RecordBatch != nil {
				p.RecordBatch = &codec.RecordBatch{
					Offset:          p.RecordBatch.Offset,
					MessageSize:     p.RecordBatch.MessageSize,
					LeaderEpoch:     p.RecordBatch.LeaderEpoch,
					MagicByte:       2,
					Flags:           0,
					LastOffsetDelta: p.RecordBatch.LastOffsetDelta,
					FirstTimestamp:  p.RecordBatch.FirstTimestamp,
					LastTimestamp:   p.RecordBatch.LastTimestamp,
					ProducerId:      -1,
					ProducerEpoch:   -1,
					BaseSequence:    p.RecordBatch.BaseSequence,
					Records:         p.RecordBatch.Records,
				}
			}
		}
		resp.TopicRespList[i] = lowTopicResp
	}
	return resp, nil
}
