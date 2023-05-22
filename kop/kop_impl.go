package kop

import (
	"context"
	"fmt"
	"github.com/Shoothzj/gox/listx"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/protocol-laboratory/kop-proxy-go/metrics"
	"github.com/protocol-laboratory/kop-proxy-go/utils"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

func (b *Broker) ConnectionOpened(conn *knet.Conn) {
	if atomic.LoadInt32(&b.connCount) > b.config.MaxConn {
		b.logger.Addr(conn.RemoteAddr()).Info("connection reach max, refused to connect")
		return
	}

	b.ConnMap.Store(conn.RemoteAddr().String(), conn.Conn)

	count := atomic.AddInt32(&b.connCount, 1)

	b.logger.Addr(conn.RemoteAddr()).Infof("new connection opened, connCount %d", count)
}

func (b *Broker) ConnectionClosed(conn *knet.Conn) {
	b.logger.Addr(conn.RemoteAddr()).Info("connection closed")
	b.DisconnectAction(conn.RemoteAddr())
	if err := conn.Close(); err != nil {
		b.logger.Addr(conn.RemoteAddr()).Errorf("close connection failed: %s", err.Error())
	}
	_, exists := b.ConnMap.LoadAndDelete(conn.RemoteAddr().String())
	if exists {
		b.SaslMap.Delete(conn.RemoteAddr().String())
		atomic.AddInt32(&b.connCount, -1)
	}
}

func (b *Broker) AcceptError(conn *knet.Conn, err error) {
	b.logger.Addr(conn.RemoteAddr()).Errorf("accept failed: %v", err)
}

func (b *Broker) ReadError(conn *knet.Conn, err error) {
	b.logger.Addr(conn.RemoteAddr()).Errorf("read failed: %v", err)
}

func (b *Broker) ReactError(conn *knet.Conn, err error) {
	b.logger.Addr(conn.RemoteAddr()).Errorf("react failed: %v", err)
}

func (b *Broker) WriteError(conn *knet.Conn, err error) {
	b.logger.Addr(conn.RemoteAddr()).Errorf("write failed: %v", err)
}

func (b *Broker) UnSupportedApi(conn *knet.Conn, apiKey codec.ApiCode, apiVersion int16) {
	b.logger.Addr(conn.RemoteAddr()).Errorf("connect unsupported api, key: %d, version: %d", apiKey, apiVersion)
}

func (b *Broker) ApiVersion(conn *knet.Conn, req *codec.ApiReq) (*codec.ApiResp, error) {
	version := req.ApiVersion
	if version <= 3 {
		startAt := time.Now()
		apiResp, err := b.ReactApiVersion(req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolApiVersionsLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolApiVersionsFailCount.Inc()
			return apiResp, err
		}
		metrics.KafkaProtocolApiVersionsSuccessCount.Inc()
		return apiResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported ApiVersion version %d", version)
	return nil, fmt.Errorf("unsupported ApiVersion version %d", version)
}

func (b *Broker) Fetch(conn *knet.Conn, req *codec.FetchReq) (*codec.FetchResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 10 || version == 11 {
		startAt := time.Now()
		if b.config.NetworkDebugEnable {
			b.logger.Addr(conn.RemoteAddr()).ClientID(req.ClientId).Infof("fetch start correlation id: %d", req.CorrelationId)
		}
		fetchResp, err := b.ReactFetch(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		if b.config.NetworkDebugEnable {
			b.logger.Addr(conn.RemoteAddr()).ClientID(req.ClientId).Infof("fetch end correlation id: %d cost %f", req.CorrelationId, cost)
		}
		metrics.KafkaProtocolFetchLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolFetchFailCount.Inc()
			return fetchResp, err
		}
		metrics.KafkaProtocolFetchSuccessCount.Inc()
		return fetchResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported Fetch version %d", version)
	return nil, fmt.Errorf("unsupported Fetch version %d", version)
}

func (b *Broker) FindCoordinator(conn *knet.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 0 || version == 3 {
		startAt := time.Now()
		if b.config.NetworkDebugEnable {
			b.logger.Addr(conn.RemoteAddr()).ClientID(req.ClientId).Infof("find coordinator start correlation id: %d", req.CorrelationId)
		}
		findCoordinatorResp, err := b.ReactFindCoordinator(req, b.config)
		cost := float64(time.Since(startAt).Milliseconds())
		if b.config.NetworkDebugEnable {
			b.logger.Addr(conn.RemoteAddr()).ClientID(req.ClientId).Infof("find coordinator end correlation id: %d, cost: %f", req.CorrelationId, cost)
		}
		metrics.KafkaProtocolFindCoordinatorLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolFindCoordinatorFailCount.Inc()
			return findCoordinatorResp, err
		}
		metrics.KafkaProtocolFindCoordinatorSuccessCount.Inc()
		return findCoordinatorResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported FindCoordinator version %d", version)
	return nil, fmt.Errorf("unsupported FindCoordinator version %d", version)
}

func (b *Broker) Heartbeat(conn *knet.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error) {
	networkContext := b.getCtx(conn)
	version := req.ApiVersion
	if version == 4 {
		startAt := time.Now()
		heartbeatResp, err := b.ReactHeartbeat(req, networkContext)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolHeartbeatLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolHeartbeatFailCount.Inc()
			return heartbeatResp, err
		}
		metrics.KafkaProtocolHeartbeatSuccessCount.Inc()
		return heartbeatResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported Heartbeat version %d", version)
	return nil, fmt.Errorf("unsupported Heartbeat version %d", version)
}

func (b *Broker) JoinGroup(conn *knet.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 1 || version == 6 {
		startAt := time.Now()
		joinGroupResp, err := b.ReactJoinGroup(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolJoinGroupLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolJoinGroupFailCount.Inc()
			return joinGroupResp, err
		}
		metrics.KafkaProtocolJoinGroupSuccessCount.Inc()
		return joinGroupResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported JoinGroup version %d", version)
	return nil, fmt.Errorf("unsupported JoinGroup version %d", version)
}

func (b *Broker) LeaveGroup(conn *knet.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 0 || version == 4 {
		startAt := time.Now()
		leaveGroupResp, err := b.ReactLeaveGroup(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolLeaveGroupLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolLeaveGroupFailCount.Inc()
			return leaveGroupResp, err
		}
		metrics.KafkaProtocolLeaveGroupSuccessCount.Inc()
		return leaveGroupResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported LeaveGroup version %d", version)
	return nil, fmt.Errorf("unsupported LeaveGroup version %d", version)
}

func (b *Broker) ListOffsets(conn *knet.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 1 || version == 5 || version == 6 {
		startAt := time.Now()
		listOffsetsResp, err := b.ListOffsetsVersion(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolListOffsetsLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolListOffsetsFailCount.Inc()
			return listOffsetsResp, err
		}
		metrics.KafkaProtocolListOffsetsSuccessCount.Inc()
		return listOffsetsResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported ListOffsets version %d", version)
	return nil, fmt.Errorf("unsupported ListOffsets version %d", version)
}

func (b *Broker) Metadata(conn *knet.Conn, req *codec.MetadataReq) (*codec.MetadataResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version <= 9 {
		startAt := time.Now()
		metadataResp, err := b.ReactMetadata(networkContext, req, b.config)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolMetadataLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolMetadataFailCount.Inc()
			return metadataResp, err
		}
		metrics.KafkaProtocolMetadataSuccessCount.Inc()
		return metadataResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported Metadata version %d", version)
	return nil, fmt.Errorf("unsupported Metadata version %d", version)
}

func (b *Broker) OffsetCommit(conn *knet.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 2 || version == 8 {
		startAt := time.Now()
		offsetCommitResp, err := b.OffsetCommitVersion(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolOffsetCommitLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolOffsetCommitFailCount.Inc()
			return offsetCommitResp, err
		}
		metrics.KafkaProtocolOffsetCommitSuccessCount.Inc()
		return offsetCommitResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported OffsetCommit version %d", version)
	return nil, fmt.Errorf("unsupported OffsetCommit version %d", version)
}

func (b *Broker) OffsetFetch(conn *knet.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 1 || version == 6 || version == 7 {
		startAt := time.Now()
		offsetFetchResp, err := b.OffsetFetchVersion(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolOffsetFetchLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolOffsetFetchFailCount.Inc()
			return offsetFetchResp, err
		}
		metrics.KafkaProtocolOffsetFetchSuccessCount.Inc()
		return offsetFetchResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported OffsetFetch version %d", version)
	return nil, fmt.Errorf("unsupported OffsetFetch version %d", version)
}

func (b *Broker) OffsetForLeaderEpoch(conn *knet.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 3 {
		startAt := time.Now()
		leaderEpochResp, err := b.OffsetForLeaderEpochVersion(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolOffsetForLeaderEpochLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolOffsetForLeaderEpochFailCount.Inc()
			return leaderEpochResp, err
		}
		metrics.KafkaProtocolOffsetForLeaderEpochSuccessCount.Inc()
		return leaderEpochResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported OffsetForLeaderEpoch version %d", version)
	return nil, fmt.Errorf("unsupported OffsetForLeaderEpoch version %d", version)
}

func (b *Broker) Produce(conn *knet.Conn, req *codec.ProduceReq) (*codec.ProduceResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 7 || version == 8 {
		startAt := time.Now()
		if b.config.NetworkDebugEnable {
			b.logger.Addr(conn.RemoteAddr()).ClientID(req.ClientId).Infof("produce start correlation id: %d", req.CorrelationId)
		}
		produceResp, err := b.ReactProduce(networkContext, req, b.config)
		cost := float64(time.Since(startAt).Milliseconds())
		if b.config.NetworkDebugEnable {
			b.logger.Addr(conn.RemoteAddr()).ClientID(req.ClientId).Infof("produce end correlation id: %d, cost: %f", req.CorrelationId, cost)
		}
		metrics.KafkaProtocolProduceLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolProduceFailCount.Inc()
			return produceResp, err
		}
		metrics.KafkaProtocolProduceSuccessCount.Inc()
		return produceResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported Produce version %d", version)
	return nil, fmt.Errorf("unsupported Produce version %d", version)
}

func (b *Broker) SaslAuthenticate(conn *knet.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error) {
	networkContext := b.getCtx(conn)
	version := req.ApiVersion
	if version == 1 || version == 2 {
		startAt := time.Now()
		saslAuthenticateResp, err := b.ReactSaslHandshakeAuth(req, networkContext)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolSaslAuthenticateLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolSaslAuthenticateFailCount.Inc()
			return saslAuthenticateResp, err
		}
		metrics.KafkaProtocolSaslAuthenticateSuccessCount.Inc()
		return saslAuthenticateResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported SaslAuthenticate version %d", version)
	return nil, fmt.Errorf("unsupported SaslAuthenticate version %d", version)
}

func (b *Broker) SaslHandshake(conn *knet.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	version := req.ApiVersion
	if version <= 1 {
		startAt := time.Now()
		saslHandshakeResp, err := b.ReactSasl(req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolSaslHandshakeLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolSaslHandshakeFailCount.Inc()
			return saslHandshakeResp, err
		}
		metrics.KafkaProtocolSaslHandshakeSuccessCount.Inc()
		return saslHandshakeResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported SaslHandshake version %d", version)
	return nil, fmt.Errorf("unsupported SaslHandshake version %d", version)
}

func (b *Broker) SyncGroup(conn *knet.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("connection is not authed")
	}
	version := req.ApiVersion
	if version == 1 || version == 4 || version == 5 {
		startAt := time.Now()
		syncGroupResp, err := b.ReactSyncGroup(networkContext, req)
		cost := float64(time.Since(startAt).Milliseconds())
		metrics.KafkaProtocolSyncGroupLatency.Observe(cost)
		if err != nil {
			metrics.KafkaProtocolSyncGroupFailCount.Inc()
			return syncGroupResp, err
		}
		metrics.KafkaProtocolSyncGroupSuccessCount.Inc()
		return syncGroupResp, nil
	}
	b.logger.Addr(conn.RemoteAddr()).Warnf("unsupported SyncGroup version %d", version)
	return nil, fmt.Errorf("unsupported SyncGroup version %d", version)
}

// kafka action interface implement

func (b *Broker) PartitionNumAction(addr net.Addr, topic string) (int, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).Topic(topic).Error("get partitionNum failed. user is not found.")
		return 0, fmt.Errorf("user not found")
	}
	num, err := b.server.PartitionNum(user.username, topic)
	if err != nil {
		b.logger.Addr(addr).Topic(topic).Errorf("get partition num failed: %s", err)
		return 0, fmt.Errorf("get partition num failed")
	}

	return num, nil
}

func (b *Broker) TopicListAction(addr net.Addr) ([]string, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).Error("get topics list failed. user not found.")
		return nil, fmt.Errorf("user not found")
	}
	topic, err := b.server.ListTopic(user.username)
	if err != nil {
		b.logger.Addr(addr).Errorf("get topic list failed: %s", err)
		return nil, err
	}
	return topic, nil
}

func (b *Broker) FetchAction(addr net.Addr, req *codec.FetchReq) ([]*codec.FetchTopicResp, error) {
	var maxWaitTime int
	if req.MaxWaitTime < b.config.MaxFetchWaitMs {
		maxWaitTime = req.MaxWaitTime
	} else {
		maxWaitTime = b.config.MaxFetchWaitMs
	}
	reqList := req.TopicReqList
	result := make([]*codec.FetchTopicResp, len(reqList))
	for i, topicReq := range reqList {
		f := &codec.FetchTopicResp{}
		f.Topic = topicReq.Topic
		f.PartitionRespList = make([]*codec.FetchPartitionResp, len(topicReq.PartitionReqList))
		for j, partitionReq := range topicReq.PartitionReqList {
			var err error
			f.PartitionRespList[j], err = b.FetchPartition(addr, topicReq.Topic, req.ClientId, partitionReq,
				req.MaxBytes, req.MinBytes, maxWaitTime/len(topicReq.PartitionReqList))
			if err != nil {
				return nil, err
			}
		}
		result[i] = f
	}
	return result, nil
}

// FetchPartition visible for testing
func (b *Broker) FetchPartition(addr net.Addr, kafkaTopic, clientID string, req *codec.FetchPartitionReq, maxBytes int, minBytes int, maxWaitMs int) (*codec.FetchPartitionResp, error) {
	start := time.Now()
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	records := make([]*codec.Record, 0)
	recordBatch := codec.RecordBatch{Records: records}
	if !exist {
		b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Error("fetch partition failed when get userinfo by addr.")
		return &codec.FetchPartitionResp{
			PartitionIndex: req.PartitionId,
			ErrorCode:      codec.UNKNOWN_SERVER_ERROR,
			RecordBatch:    &recordBatch,
		}, nil
	}
	partitionedTopic, err := b.partitionedTopic(user, kafkaTopic, req.PartitionId)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Error("fetch partition failed when get topic.")
		return &codec.FetchPartitionResp{
			PartitionIndex: req.PartitionId,
			ErrorCode:      codec.UNKNOWN_SERVER_ERROR,
			RecordBatch:    &recordBatch,
		}, nil
	}
	b.mutex.RLock()
	consumerMetadata, exist := b.consumerManager[partitionedTopic+clientID]
	if !exist {
		groupId, exist := b.topicGroupManager[partitionedTopic+clientID]
		b.mutex.RUnlock()
		if exist {
			group, err := b.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Infof(
					"group is preparing rebalance. partitionedTopic: %s", partitionedTopic)
				return &codec.FetchPartitionResp{
					LastStableOffset: 0,
					ErrorCode:        codec.NONE,
					LogStartOffset:   0,
					RecordBatch:      &recordBatch,
					PartitionIndex:   req.PartitionId,
				}, nil
			}
		}
		b.groupCoordinator.DelGroup(user.username, groupId)
		b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Errorf(
			"can not find consumer for topic when fetch partition %s", partitionedTopic)
		return nil, fmt.Errorf("can not find consumer for topic: %s", partitionedTopic)
	}
	b.mutex.RUnlock()
	byteLength := 0
	var baseOffset int64
	fistMessage := true
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxWaitMs)*time.Millisecond)
	defer cancel()
OUT:
	for {
		if time.Since(start).Milliseconds() >= int64(maxWaitMs) || len(recordBatch.Records) >= b.config.MaxFetchRecord {
			break OUT
		}
		flowControl := b.server.HasFlowQuota(user.username, partitionedTopic)
		if !flowControl {
			break
		}
		consumerMetadata.mutex.Lock()
		message, err := consumerMetadata.consumer.Receive(ctx)
		if err != nil {
			consumerMetadata.mutex.Unlock()
			if ctx.Err() != nil {
				break OUT
			}
			b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Errorf(
				"partitionedTopic %s read msg failed: %s", partitionedTopic, err)
			continue
		}
		offset := convOffset(message, b.config.ContinuousOffset)
		if utils.DebugTopicMatch(b.config.DebugKafkaTopicSet, b.config.DebugPulsarTopicSet, kafkaTopic, partitionedTopic) {
			b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Infof(
				"message received. kafka topic from pulsar topic: %s, partition: %d, offset: %d, messageId: %s",
				partitionedTopic, req.PartitionId, offset, message.ID())
		}
		err = consumerMetadata.consumer.Ack(message)
		if err != nil {
			b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Errorf("ack topic message failed: %s", err)
		}
		consumerMetadata.mutex.Unlock()
		byteLength = byteLength + utils.CalculateMsgLength(message)
		if fistMessage {
			fistMessage = false
			baseOffset = offset
		}
		relativeOffset := offset - baseOffset
		var record codec.Record
		if b.config.RecordHeaderSupport {
			record = codec.Record{
				Value:          message.Payload(),
				RelativeOffset: int(relativeOffset),
				Headers:        utils.ConvertMap2Headers(message.Properties()),
			}
		} else {
			record = codec.Record{
				Value:          message.Payload(),
				RelativeOffset: int(relativeOffset),
			}
		}
		if utils.DebugTopicMatch(b.config.DebugKafkaTopicSet, b.config.DebugPulsarTopicSet, kafkaTopic, partitionedTopic) {
			b.logger.Addr(addr).ClientID(clientID).Topic(kafkaTopic).Infof(
				"message add to batch. pulsar topic: %s, partition: %d, offset: %d, messageId: %s",
				partitionedTopic, req.PartitionId, offset, message.ID())
		}
		recordBatch.Records = append(recordBatch.Records, &record)
		consumerMetadata.mutex.Lock()
		consumerMetadata.messageIds.PushBack(MessageIdPair{
			MessageId: message.ID(),
			Offset:    offset,
		})
		consumerMetadata.mutex.Unlock()
		if byteLength > minBytes && time.Since(start).Milliseconds() >= int64(b.config.MinFetchWaitMs) {
			break
		}
		if byteLength > maxBytes {
			break
		}
	}
	recordBatch.Offset = baseOffset
	return &codec.FetchPartitionResp{
		ErrorCode:        codec.NONE,
		PartitionIndex:   req.PartitionId,
		LastStableOffset: 0,
		LogStartOffset:   0,
		RecordBatch:      &recordBatch,
	}, nil
}

func (b *Broker) GroupJoinAction(addr net.Addr, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(req.ClientId).Error("username not found in join group")
		return &codec.JoinGroupResp{
			ErrorCode:    codec.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	b.logger.Addr(addr).ClientID(req.ClientId).Infof("joining to group, memberId: %s", req.MemberId)
	joinGroupResp, err := b.groupCoordinator.HandleJoinGroup(user.username, req.GroupId, req.MemberId, req.ClientId, req.ProtocolType,
		req.SessionTimeout, req.GroupProtocols)
	if err != nil {
		b.logger.Addr(addr).ClientID(req.ClientId).Errorf("unexpected exception in join group, error: %s", err)
		return &codec.JoinGroupResp{
			ErrorCode:    codec.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	memberInfo := MemberInfo{
		memberId:        joinGroupResp.MemberId,
		groupId:         req.GroupId,
		groupInstanceId: req.GroupInstanceId,
		clientId:        req.ClientId,
	}
	b.mutex.Lock()
	b.memberManager[addr] = &memberInfo
	b.mutex.Unlock()
	return joinGroupResp, nil
}

func (b *Broker) GroupLeaveAction(addr net.Addr, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(req.ClientId).Error("username not found in leave group.")
		return &codec.LeaveGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.logger.Addr(addr).ClientID(req.ClientId).Infof("leaving group, members: %+v", req.Members)
	leaveGroupResp, err := b.groupCoordinator.HandleLeaveGroup(user.username, req.GroupId, req.Members)
	if err != nil {
		b.logger.Addr(addr).ClientID(req.ClientId).Errorf("unexpected exception in leaving group, error: %s", err)
		return &codec.LeaveGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	group, err := b.groupCoordinator.GetGroup(user.username, req.GroupId)
	if err != nil {
		b.logger.Addr(addr).ClientID(req.ClientId).Errorf("get group failed: %s", err)
		return &codec.LeaveGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	for _, topic := range group.partitionedTopic {
		b.mutex.Lock()
		consumerHandle, exist := b.consumerManager[topic+req.ClientId]
		if exist {
			consumerHandle.mutex.Lock()
			consumerHandle.consumer.Close()
			consumerHandle.mutex.Unlock()
			b.logger.Addr(addr).ClientID(req.ClientId).Infof("success close consumer topic: %s", group.partitionedTopic)
			if err := b.offsetManager.GracefulSendOffsetMessage(topic, consumerHandle); err != nil {
				b.logger.Addr(addr).ClientID(req.ClientId).Errorf("graceful send offset message failed: %v", err)
			}
			delete(b.consumerManager, topic+req.ClientId)
			consumerHandle = nil
		}
		delete(b.topicGroupManager, topic+req.ClientId)
		b.mutex.Unlock()
	}
	b.mutex.Lock()
	delete(b.memberManager, addr)
	b.mutex.Unlock()
	return leaveGroupResp, nil
}

func (b *Broker) GroupSyncAction(addr net.Addr, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(req.ClientId).Error("username not found in sync group.")
		return &codec.SyncGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.logger.Addr(addr).ClientID(req.ClientId).Infof("syncing group, memberId: %s", req.MemberId)
	syncGroupResp, err := b.groupCoordinator.HandleSyncGroup(user.username, req.GroupId, req.MemberId, req.GenerationId, req.GroupAssignments)
	if err != nil {
		b.logger.Addr(addr).ClientID(req.ClientId).Errorf("unexpected exception in sync group, error: %s", err)
		return &codec.SyncGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	syncGroupResp.ProtocolName = req.ProtocolName
	syncGroupResp.ProtocolType = req.ProtocolType
	return syncGroupResp, nil
}

func (b *Broker) OffsetListPartitionAction(addr net.Addr, topic, clientID string, req *codec.ListOffsetsPartition) (*codec.ListOffsetsPartitionResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Error("offset list failed when get username by addr.")
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.logger.Addr(addr).ClientID(clientID).Topic(topic).Infof("offset list topic, partition: %d", req.PartitionId)
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Errorf("get topic failed. err: %s", err)
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.mutex.RLock()
	consumerHandle, exist := b.consumerManager[partitionedTopic+clientID]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Error("offset list failed, topic does not exist.")
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	offset := constant.DefaultOffset
	if req.Time == constant.TimeLasted {
		latestMsgId, err := utils.GetLatestMsgId(partitionedTopic, b.pAdmin)
		if err != nil {
			b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf("get topic latest offset failed %s", err)
			return &codec.ListOffsetsPartitionResp{
				PartitionId: req.PartitionId,
				ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		lastedMsg, err := utils.ReadLatestMsg(partitionedTopic, b.config.MaxFetchWaitMs, latestMsgId, consumerHandle.client)
		if err != nil {
			b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf("read lasted latestMsgId failed. err: %s", err)
			return &codec.ListOffsetsPartitionResp{
				PartitionId: req.PartitionId,
				ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		if lastedMsg != nil {
			consumerHandle.mutex.Lock()
			err := consumerHandle.consumer.Seek(lastedMsg.ID())
			consumerHandle.mutex.Unlock()
			if err != nil {
				b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Errorf("offset list failed: %s", err)
				return &codec.ListOffsetsPartitionResp{
					PartitionId: req.PartitionId,
					ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
				}, nil
			}
			b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Infof(
				"kafka topic previous message id: %s, when trigger offset list partition action", lastedMsg.ID())
			offset = convOffset(lastedMsg, b.config.ContinuousOffset) + 1
		}
	}
	return &codec.ListOffsetsPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      offset,
		Timestamp:   constant.TimeEarliest,
	}, nil
}

func (b *Broker) OffsetCommitPartitionAction(addr net.Addr, topic, clientID string, req *codec.OffsetCommitPartitionReq) (*codec.OffsetCommitPartitionResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Error("offset commit failed when get userinfo by addr.")
		return &codec.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf("offset commit failed when get topic, err: %v", err)
		return &codec.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.mutex.RLock()
	consumerMessages, exist := b.consumerManager[partitionedTopic+clientID]
	if !exist {
		groupId, exist := b.topicGroupManager[partitionedTopic+clientID]
		b.mutex.RUnlock()
		if exist {
			group, err := b.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Warn(
					"group is preparing rebalance.")
				return &codec.OffsetCommitPartitionResp{ErrorCode: codec.REBALANCE_IN_PROGRESS}, nil
			}
		}
		b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Error(
			"commit offset failed, does not exist.")
		return &codec.OffsetCommitPartitionResp{ErrorCode: codec.UNKNOWN_TOPIC_ID}, nil
	}
	b.mutex.RUnlock()
	consumerMessages.mutex.RLock()
	length := consumerMessages.messageIds.Len()
	consumerMessages.mutex.RUnlock()
	for i := 0; i < length; i++ {
		consumerMessages.mutex.RLock()
		front := consumerMessages.messageIds.Front()
		consumerMessages.mutex.RUnlock()
		if front == nil {
			break
		}
		messageIdPair := front.Value
		// kafka commit offset maybe greater than current offset
		if messageIdPair.Offset == req.Offset || ((messageIdPair.Offset < req.Offset) && (i == length-1)) {
			err := b.offsetManager.CommitOffset(user.username, topic, consumerMessages.groupId, req.PartitionId, messageIdPair)
			if err != nil {
				b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf("commit offset failed. err: %s", err)
				return &codec.OffsetCommitPartitionResp{
					PartitionId: req.PartitionId,
					ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
				}, nil
			}
			b.logger.Addr(addr).ClientID(clientID).Topic(partitionedTopic).Infof(
				"ack topic, messageID: %s, offset: %d", messageIdPair.MessageId, messageIdPair.Offset)
			consumerMessages.mutex.Lock()
			consumerMessages.messageIds.Remove(front)
			consumerMessages.mutex.Unlock()
			break
		}
		if messageIdPair.Offset > req.Offset {
			break
		}
		consumerMessages.mutex.Lock()
		consumerMessages.messageIds.Remove(front)
		consumerMessages.mutex.Unlock()
	}
	return &codec.OffsetCommitPartitionResp{
		PartitionId: req.PartitionId,
		ErrorCode:   codec.NONE,
	}, nil
}

func (b *Broker) createConsumerHandle(partitionedTopic string, subscriptionName string, messageId pulsar.MessageID, clientId string) (*PulsarConsumerHandle, error) {
	var (
		handle = &PulsarConsumerHandle{messageIds: listx.New[MessageIdPair]()}
		err    error
	)
	pulsarUrl := fmt.Sprintf("pulsar://%s:%d", b.config.PulsarConfig.Host, b.config.PulsarConfig.TcpPort)
	handle.client, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
	if err != nil {
		b.logger.ClientID(clientId).Topic(partitionedTopic).Errorf("create pulsar client failed: %v", err)
		return nil, err
	}
	handle.channel = make(chan pulsar.ConsumerMessage, b.config.ConsumerReceiveQueueSize)
	options := pulsar.ConsumerOptions{
		Topic:                       partitionedTopic,
		Name:                        subscriptionName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Failover,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              handle.channel,
		ReceiverQueueSize:           b.config.ConsumerReceiveQueueSize,
	}
	handle.consumer, err = handle.client.Subscribe(options)
	if err != nil {
		b.logger.ClientID(clientId).Topic(partitionedTopic).Warnf("subscribe consumer failed: %s", err)
		handle.close()
		return nil, err
	}
	if messageId != pulsar.EarliestMessageID() {
		err = handle.consumer.Seek(messageId)
		if err != nil {
			b.logger.ClientID(clientId).Topic(partitionedTopic).Warnf("seek message failed: %s", err)
			handle.close()
			return nil, err
		}
		b.logger.ClientID(clientId).Topic(partitionedTopic).Infof("kafka topic previous message id: %s", messageId)
	}
	b.logger.ClientID(clientId).Topic(partitionedTopic).Infof("create consumer success, subscription name: %s", subscriptionName)
	return handle, nil
}

func (b *Broker) checkPartitionTopicExist(topics []string, partitionTopic string) bool {
	for _, topic := range topics {
		if strings.EqualFold(topic, partitionTopic) {
			return true
		}
	}
	return false
}

func (b *Broker) OffsetFetchAction(addr net.Addr, topic, clientID, groupID string, req *codec.OffsetFetchPartitionReq) (*codec.OffsetFetchPartitionResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Error(
			"offset fetch failed when get userinfo by addr.")
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	pulsarTopic, partitionedTopic, err := b.pulsarTopic(user, topic, req.PartitionId)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Error(
			"offset fetch failed when get topic.")
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	subscriptionName, err := b.server.SubscriptionName(groupID)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf(
			"sync group failed when offset fetch, error: %s", err)
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	messagePair, flag := b.offsetManager.AcquireOffset(user.username, topic, groupID, req.PartitionId)
	messageId := pulsar.EarliestMessageID()
	kafkaOffset := constant.UnknownOffset
	if flag {
		kafkaOffset = messagePair.Offset + 1
		messageId = messagePair.MessageId
	}

	kafkaKey := b.offsetManager.GenerateKey(user.username, topic, groupID, req.PartitionId)
	b.logger.Addr(addr).ClientID(clientID).Topic(topic).Infof(
		"acquire offset, key: %s, partition: %d, offset: %d, message id: %s",
		kafkaKey, req.PartitionId, kafkaOffset, messageId)

	group, err := b.groupCoordinator.GetGroup(user.username, groupID)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf("get group failed: %s", err)
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.REBALANCE_IN_PROGRESS,
		}, nil
	}

	b.mutex.RLock()
	_, exist = b.consumerManager[partitionedTopic+clientID]
	b.mutex.RUnlock()
	if !exist {
		b.mutex.Lock()
		consumerHandle, err := b.createConsumerHandle(partitionedTopic, subscriptionName, messageId, clientID)
		if err != nil {
			b.mutex.Unlock()
			b.logger.Addr(addr).ClientID(clientID).Topic(topic).Errorf("create channel failed: %s", err)
			return &codec.OffsetFetchPartitionResp{
				ErrorCode: codec.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		consumerHandle.groupId = groupID
		consumerHandle.username = user.username
		b.consumerManager[partitionedTopic+clientID] = consumerHandle
		b.mutex.Unlock()
	}
	b.topicAddrManager.Set(pulsarTopic, partitionedTopic, addr)

	if !b.checkPartitionTopicExist(group.partitionedTopic, partitionedTopic) {
		group.partitionedTopic = append(group.partitionedTopic, partitionedTopic)
	}
	b.mutex.Lock()
	b.topicGroupManager[partitionedTopic+clientID] = group.groupId
	b.mutex.Unlock()

	return &codec.OffsetFetchPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      kafkaOffset,
		LeaderEpoch: -1,
		Metadata:    nil,
		ErrorCode:   codec.NONE,
	}, nil
}

func (b *Broker) OffsetLeaderEpochAction(addr net.Addr, topic string, req *codec.OffsetLeaderEpochPartitionReq) (*codec.OffsetForLeaderEpochPartitionResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).Topic(topic).Error("offset fetch failed when get userinfo by addr.")
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.logger.Addr(addr).Topic(topic).Infof("offset leader epoch topic, partition: %d", req.PartitionId)
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		b.logger.Addr(addr).Topic(topic).Error("get partitioned topic failed.")
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	latestMsgId, err := utils.GetLatestMsgId(partitionedTopic, b.pAdmin)
	if err != nil {
		b.logger.Addr(addr).Topic(topic).Error("get last msgId failed.")
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	msg, err := utils.ReadLatestMsg(partitionedTopic, b.config.MaxFetchWaitMs, latestMsgId, b.pClient)
	if err != nil {
		b.logger.Addr(addr).Topic(topic).Error("get last msgId failed.")
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	offset := convOffset(msg, b.config.ContinuousOffset)
	return &codec.OffsetForLeaderEpochPartitionResp{
		ErrorCode:   codec.NONE,
		PartitionId: req.PartitionId,
		LeaderEpoch: req.LeaderEpoch,
		Offset:      offset,
	}, nil
}

func (b *Broker) getProducer(addr net.Addr, username, pulsarTopic string) (pulsar.Producer, error) {
	b.mutex.Lock()
	producer, exist := b.producerManager[addr]
	var err error
	if !exist {
		options := pulsar.ProducerOptions{}
		options.Topic = pulsarTopic
		options.MaxPendingMessages = b.config.MaxProducerRecordSize
		options.BatchingMaxSize = uint(b.config.MaxBatchSize)
		options.BatchingMaxMessages = uint(b.config.MaxProducerRecordSize)
		producer, err = b.pClient.CreateProducer(options)
		if err != nil {
			b.mutex.Unlock()
			b.logger.Addr(addr).Topic(pulsarTopic).Errorf("create producer failed. err: %v", err)
			return nil, err
		}
		b.logger.Addr(addr).Topic(pulsarTopic).Info("create producer success.")
		b.producerManager[addr] = producer
	}
	b.mutex.Unlock()
	return producer, nil
}

func (b *Broker) ProduceAction(addr net.Addr, topic string, partition int, req *codec.ProducePartitionReq) (*codec.ProducePartitionResp, error) {
	errList := make([]*codec.RecordError, 0)
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).Topic(topic).Errorf("user not exist, username: %s", user.username)
		return &codec.ProducePartitionResp{
			ErrorCode:       codec.TOPIC_AUTHORIZATION_FAILED,
			RecordErrorList: errList,
			PartitionId:     req.PartitionId,
		}, nil
	}
	pulsarTopic, err := b.server.PulsarTopic(user.username, topic)
	if err != nil {
		b.logger.Addr(addr).Topic(topic).Errorf("get pulsar topic failed. username: %s", user.username)
		return nil, err
	}
	producer, err := b.getProducer(addr, user.username, pulsarTopic)
	if err != nil {
		b.logger.Addr(addr).Topic(topic).Errorf("create producer failed, username: %s", user.username)
		return &codec.ProducePartitionResp{
			ErrorCode:       codec.TOPIC_AUTHORIZATION_FAILED,
			RecordErrorList: errList,
			PartitionId:     req.PartitionId,
		}, nil
	}
	batch := req.RecordBatch.Records
	count := int32(0)
	producerChan := make(chan bool)
	var offset int64
	for _, kafkaMsg := range batch {
		message := pulsar.ProducerMessage{}
		message.Payload = kafkaMsg.Value
		if kafkaMsg.Key != nil {
			message.Key = string(kafkaMsg.Key)
		}
		startTime := time.Now()
		producer.SendAsync(context.Background(), &message, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			atomic.AddInt32(&count, 1)
			if err != nil {
				metrics.PulsarSendFailCount.Inc()
				if !b.config.TopicLevelMetricsDisable {
					metrics.PulsarTopicSendFailCount.WithLabelValues(topic, pulsarTopic).Inc()
				}
				b.logger.Addr(addr).Topic(topic).Errorf("send msg failed. username: %s, err: %s", user.username, err)
			} else {
				metrics.PulsarSendSuccessCount.Inc()
				if !b.config.TopicLevelMetricsDisable {
					metrics.PulsarTopicSendSuccessCount.WithLabelValues(topic, pulsarTopic).Inc()
				}
			}
			cost := float64(time.Since(startTime).Milliseconds())
			metrics.PulsarSendLatency.Observe(cost)
			if !b.config.TopicLevelMetricsDisable {
				metrics.PulsarTopicSendLatency.WithLabelValues(topic, pulsarTopic).Observe(cost)
			}
			if count == int32(len(batch)) {
				offset = ConvertMsgId(id)
				producerChan <- true
			}
		})
	}
	<-producerChan
	return &codec.ProducePartitionResp{
		PartitionId:     partition,
		Offset:          offset,
		Time:            -1,
		RecordErrorList: errList,
		LogStartOffset:  0,
	}, nil
}

func (b *Broker) SaslAuthAction(addr net.Addr, req codec.SaslAuthenticateReq) (bool, codec.ErrorCode) {
	auth, err := b.server.Auth(req.Username, req.Password, req.ClientId)
	if err != nil || !auth {
		return false, codec.SASL_AUTHENTICATION_FAILED
	}
	b.mutex.RLock()
	_, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.mutex.Lock()
		b.userInfoManager[addr] = &userInfo{
			username: req.Username,
			clientId: req.ClientId,
		}
		b.mutex.Unlock()
	}
	return true, codec.NONE
}

func (b *Broker) SaslAuthTopicAction(addr net.Addr, req codec.SaslAuthenticateReq, topic, permissionType string) (bool, codec.ErrorCode) {
	auth, err := b.server.AuthTopic(req.Username, req.Password, req.ClientId, topic, permissionType)
	if err != nil || !auth {
		return false, codec.SASL_AUTHENTICATION_FAILED
	}
	return true, codec.NONE
}

func (b *Broker) SaslAuthConsumerGroupAction(addr net.Addr, req codec.SaslAuthenticateReq, consumerGroup string) (bool, codec.ErrorCode) {
	auth, err := b.server.AuthTopicGroup(req.Username, req.Password, req.ClientId, consumerGroup)
	if err != nil || !auth {
		return false, codec.SASL_AUTHENTICATION_FAILED
	}
	return true, codec.NONE
}

func (b *Broker) HeartBeatAction(addr net.Addr, req codec.HeartbeatReq) *codec.HeartbeatResp {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr]
	b.mutex.RUnlock()
	if !exist {
		b.logger.Addr(addr).Error("heartbeat user not exist when get userinfo by addr.")
		return &codec.HeartbeatResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}
	}
	resp := b.groupCoordinator.HandleHeartBeat(user.username, req.GroupId, req.MemberId)
	if resp.ErrorCode == codec.REBALANCE_IN_PROGRESS {
		group, err := b.groupCoordinator.GetGroup(user.username, req.GroupId)
		if err != nil {
			b.logger.Addr(addr).Errorf("rebalance but heartbeat failed when get group, err: %v", err)
			return resp
		}
		for _, topic := range group.partitionedTopic {
			b.mutex.Lock()
			consumerHandle, exist := b.consumerManager[topic+req.ClientId]
			if exist {
				consumerHandle.mutex.Lock()
				consumerHandle.close()
				consumerHandle.mutex.Unlock()
				if err := b.offsetManager.GracefulSendOffsetMessage(topic, consumerHandle); err != nil {
					b.logger.Addr(addr).Errorf("graceful send offset message failed: %v", err)
				}
				b.logger.Addr(addr).Topic(topic).Info("success close consumer topic due to heartbeat failed.")
				delete(b.consumerManager, topic+req.ClientId)
				consumerHandle = nil
			}
			b.mutex.Unlock()

			topicInfo, err := utils.GetTenantNamespaceTopicFromPartitionedPrefix(topic)
			if err != nil {
				b.logger.Topic(topic).Errorf("get topic info failed: %s", err)
				continue
			}
			b.topicAddrManager.Delete(topicInfo.Topic, topic)
		}
	}
	return resp
}

func (b *Broker) ProducePulsarProducerSize() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return len(b.producerManager)
}

func (b *Broker) ConsumePulsarConsumerSize() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return len(b.consumerManager)
}

func (b *Broker) DisconnectAll() {
	b.logger.Info("connection all closed.")

	for addr := range b.producerManager {
		conn, loaded := b.ConnMap.Load(addr.String())
		if !loaded {
			continue
		}

		err := conn.Close()
		if err != nil {
			b.logger.Addr(addr).Errorf("conn closed failed: %v.", err.Error())
		}
	}
}

func (b *Broker) DisconnectAllLocalAddr(localAddr string) {
	b.logger.Infof("connection all local addr %s closed.", localAddr)

	for addr := range b.producerManager {
		conn, loaded := b.ConnMap.Load(addr.String())
		if !loaded {
			continue
		}

		if conn.LocalAddr().String() != localAddr {
			continue
		}

		err := conn.Close()
		if err != nil {
			b.logger.Addr(addr).Errorf("conn closed failed: %v.", err.Error())
		}
	}
}

func (b *Broker) DisconnectRemoteAddr(addrList []string) {
	b.logger.Infof("connection remote addr closed : %v.", addrList)

	for _, addr := range addrList {
		conn, loaded := b.ConnMap.Load(addr)
		if !loaded {
			continue
		}

		err := conn.Close()
		if err != nil {
			b.logger.Addr(conn.RemoteAddr()).Errorf("conn closed failed: %v.", err.Error())
		}
	}
}

func (b *Broker) DisconnectAction(addr net.Addr) {
	b.logger.Addr(addr).Info("lost connection.")
	if addr == nil {
		return
	}
	b.mutex.RLock()
	memberInfo, exist := b.memberManager[addr]
	producer, producerExist := b.producerManager[addr]
	b.mutex.RUnlock()
	if producerExist {
		producer.Close()
		b.mutex.Lock()
		delete(b.producerManager, addr)
		b.mutex.Unlock()
	}
	if !exist {
		b.mutex.Lock()
		delete(b.userInfoManager, addr)
		b.mutex.Unlock()
		return
	}
	memberList := []*codec.LeaveGroupMember{
		{
			MemberId:        memberInfo.memberId,
			GroupInstanceId: memberInfo.groupInstanceId,
		},
	}
	req := codec.LeaveGroupReq{
		BaseReq: codec.BaseReq{ClientId: memberInfo.clientId},
		GroupId: memberInfo.groupId,
		Members: memberList,
	}
	_, err := b.GroupLeaveAction(addr, &req)
	if err != nil {
		b.logger.Addr(addr).Errorf("leave group failed. err: %s", err)
	}
	// leave group will use user information
	b.mutex.Lock()
	delete(b.userInfoManager, addr)
	b.mutex.Unlock()
}

func (b *Broker) Close() {
	// offsetManager create failed, will call broker.Close, cause nil pointer error
	if b.offsetManager != nil {
		if err := b.offsetManager.GracefulSendOffsetMessages(b.consumerManager); err != nil {
			b.logger.Errorf("graceful send offset messages failed: %v", err)
		}
		b.offsetManager.Close()
	}
	b.CloseServer()
	b.mutex.Lock()
	for key, consumerHandle := range b.consumerManager {
		consumerHandle.close()
		delete(b.consumerManager, key)
	}
	for key, value := range b.producerManager {
		value.Close()
		delete(b.producerManager, key)
	}
	b.mutex.Unlock()
}

func (b *Broker) DisconnectConsumer(topic string) {
	addrList, ok := b.topicAddrManager.LoadAndDelete(topic)
	if !ok {
		return
	}
	for _, addr := range addrList {
		b.DisconnectAction(addr)
	}
}
