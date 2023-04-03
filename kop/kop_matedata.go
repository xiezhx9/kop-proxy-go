package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactMetadata(ctx *NetworkContext, req *codec.MetadataReq, config *Config) (*codec.MetadataResp, error) {
	logrus.Debugf("metadata req: %+v", req)
	topics := req.Topics
	topicList := make([]string, 0)
	if len(topics) > 1 {
		logrus.Error("currently, not support more than one topic", ctx.Addr)
		return nil, fmt.Errorf("")
	}
	if len(topics) == 1 {
		topicList = append(topicList, topics[0].Topic)
	}
	if len(topics) == 0 {
		logrus.Warnf("%s request metadata topic length is 0", ctx.Addr)
		list, err := b.TopicListAction(ctx.Addr)
		if err != nil {
			return nil, fmt.Errorf("%s get topic list failed: %v", ctx.Addr, err)
		}
		topicList = list
	}

	var metadataResp = &codec.MetadataResp{
		BaseResp:                   codec.BaseResp{CorrelationId: req.CorrelationId},
		ClusterId:                  config.ClusterId,
		ControllerId:               config.NodeId,
		ClusterAuthorizedOperation: -2147483648,
		BrokerMetadataList: []*codec.BrokerMetadata{
			{NodeId: config.NodeId, Host: config.AdvertiseHost, Port: config.AdvertisePort, Rack: nil},
		},
	}

	metadataResp.TopicMetadataList = make([]*codec.TopicMetadata, len(topicList))
	for index, topic := range topicList {
		partitionNum, err := b.PartitionNumAction(ctx.Addr, topic)
		if err != nil {
			topicMetadata := codec.TopicMetadata{ErrorCode: codec.UNKNOWN_SERVER_ERROR, Topic: topic, IsInternal: false, TopicAuthorizedOperation: -2147483648}
			topicMetadata.PartitionMetadataList = make([]*codec.PartitionMetadata, 0)
			metadataResp.TopicMetadataList[index] = &topicMetadata
		} else {
			topicMetadata := codec.TopicMetadata{ErrorCode: 0, Topic: topic, IsInternal: false, TopicAuthorizedOperation: -2147483648}
			topicMetadata.PartitionMetadataList = make([]*codec.PartitionMetadata, partitionNum)
			for i := 0; i < partitionNum; i++ {
				partitionMetadata := &codec.PartitionMetadata{ErrorCode: 0, PartitionId: i, LeaderId: config.NodeId, LeaderEpoch: 0, OfflineReplicas: nil}
				replicas := make([]*codec.Replica, 1)
				replicas[0] = &codec.Replica{ReplicaId: config.NodeId}
				partitionMetadata.Replicas = replicas
				partitionMetadata.CaughtReplicas = replicas
				topicMetadata.PartitionMetadataList[i] = partitionMetadata
			}
			metadataResp.TopicMetadataList[index] = &topicMetadata
		}
	}
	return metadataResp, nil
}
