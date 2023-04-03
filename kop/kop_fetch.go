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
				p.RecordBatch = b.convertRecordBatchResp(p.RecordBatch)
			}
		}
		resp.TopicRespList[i] = lowTopicResp
	}
	return resp, nil
}

func (b *Broker) convertRecordBatchResp(lowRecordBatch *codec.RecordBatch) *codec.RecordBatch {
	return &codec.RecordBatch{
		Offset:          lowRecordBatch.Offset,
		MessageSize:     lowRecordBatch.MessageSize,
		LeaderEpoch:     lowRecordBatch.LeaderEpoch,
		MagicByte:       2,
		Flags:           0,
		LastOffsetDelta: lowRecordBatch.LastOffsetDelta,
		FirstTimestamp:  lowRecordBatch.FirstTimestamp,
		LastTimestamp:   lowRecordBatch.LastTimestamp,
		ProducerId:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    lowRecordBatch.BaseSequence,
		Records:         lowRecordBatch.Records,
	}
}
