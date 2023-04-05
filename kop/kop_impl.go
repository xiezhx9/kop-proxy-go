package kop

import (
	"container/list"
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/protocol-laboratory/kop-proxy-go/utils"
	"github.com/sirupsen/logrus"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

func (b *Broker) ConnectionClosed(conn *knet.Conn) {
	logrus.Infof("connection closed from %s", conn.RemoteAddr())
	b.DisconnectAction(conn.RemoteAddr())
	if err := conn.Close(); err != nil {
		logrus.Errorf("close connection %s failed: %s", conn.RemoteAddr(), err.Error())
	}
	b.ConnMap.Delete(conn.RemoteAddr())
	b.SaslMap.Delete(conn.RemoteAddr())
	atomic.AddInt32(&b.connCount, -1)
}

func (b *Broker) AcceptError(conn *knet.Conn, err error) {
	logrus.Errorf("accept failed from %s, err: %v", conn.RemoteAddr(), err)
}

func (b *Broker) ReadError(conn *knet.Conn, err error) {
	logrus.Errorf("read failed from %s, err: %v", conn.RemoteAddr(), err)
}

func (b *Broker) ReactError(conn *knet.Conn, err error) {
	logrus.Errorf("react failed from %s, err: %v", conn.RemoteAddr(), err)
}

func (b *Broker) WriteError(conn *knet.Conn, err error) {
	logrus.Errorf("write failed from %s, err: %v", conn.RemoteAddr(), err)
}

func (b *Broker) UnSupportedApi(conn *knet.Conn, apiKey codec.ApiCode, apiVersion int16) {
	logrus.Errorf("%s connect unsupported api, key: %d, version: %d", conn.RemoteAddr(), apiKey, apiVersion)
}

func (b *Broker) ApiVersion(conn *knet.Conn, req *codec.ApiReq) (*codec.ApiResp, error) {
	version := req.ApiVersion
	if version <= 3 {
		return b.ReactApiVersion(req)
	}
	logrus.Warnf("unsupported ApiVersion version %d", version)
	return nil, fmt.Errorf("unsupported ApiVersion version %d", version)
}

func (b *Broker) Fetch(conn *knet.Conn, req *codec.FetchReq) (*codec.FetchResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 10 || version == 11 {
		return b.ReactFetch(networkContext, req)
	}
	logrus.Warnf("unsupported Fetch version %d", version)
	return nil, fmt.Errorf("unsupported Fetch version %d", version)
}

func (b *Broker) FindCoordinator(conn *knet.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 0 || version == 3 {
		return b.ReactFindCoordinator(req, b.config)
	}
	logrus.Warnf("unsupported FindCoordinator version %d", version)
	return nil, fmt.Errorf("unsupported FindCoordinator version %d", version)
}

func (b *Broker) Heartbeat(conn *knet.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error) {
	networkContext := b.getCtx(conn)
	version := req.ApiVersion
	if version == 4 {
		return b.ReactHeartbeat(req, networkContext)
	}
	logrus.Warnf("unsupported Heartbeat version %d", version)
	return nil, fmt.Errorf("unsupported Heartbeat version %d", version)
}

func (b *Broker) JoinGroup(conn *knet.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 1 || version == 6 {
		return b.ReactJoinGroup(networkContext, req)
	}
	logrus.Warnf("unsupported JoinGroup version %d", version)
	return nil, fmt.Errorf("unsupported JoinGroup version %d", version)
}

func (b *Broker) LeaveGroup(conn *knet.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 0 || version == 4 {
		return b.ReactLeaveGroup(networkContext, req)
	}
	logrus.Warnf("unsupported LeaveGroup version %d", version)
	return nil, fmt.Errorf("unsupported LeaveGroup version %d", version)
}

func (b *Broker) ListOffsets(conn *knet.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 1 || version == 5 || version == 6 {
		return b.ListOffsetsVersion(networkContext, req)
	}
	logrus.Warnf("unsupported ListOffsets version %d", version)
	return nil, fmt.Errorf("unsupported ListOffsets version %d", version)
}

func (b *Broker) Metadata(conn *knet.Conn, req *codec.MetadataReq) (*codec.MetadataResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version <= 9 {
		return b.ReactMetadata(networkContext, req, b.config)
	}
	logrus.Warnf("unsupported Metadata version %d", version)
	return nil, fmt.Errorf("unsupported Metadata version %d", version)
}

func (b *Broker) OffsetCommit(conn *knet.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 2 || version == 8 {
		return b.OffsetCommitVersion(networkContext, req)
	}
	logrus.Warnf("unsupported OffsetCommit version %d", version)
	return nil, fmt.Errorf("unsupported OffsetCommit version %d", version)
}

func (b *Broker) OffsetFetch(conn *knet.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 1 || version == 6 || version == 7 {
		return b.OffsetFetchVersion(networkContext, req)
	}
	logrus.Warnf("unsupported OffsetFetch version %d", version)
	return nil, fmt.Errorf("unsupported OffsetFetch version %d", version)
}

func (b *Broker) OffsetForLeaderEpoch(conn *knet.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 3 {
		return b.OffsetForLeaderEpochVersion(networkContext, req)
	}
	logrus.Warnf("unsupported OffsetForLeaderEpoch version %d", version)
	return nil, fmt.Errorf("unsupported OffsetForLeaderEpoch version %d", version)
}

func (b *Broker) Produce(conn *knet.Conn, req *codec.ProduceReq) (*codec.ProduceResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 7 || version == 8 {
		return b.ReactProduce(networkContext, req, b.config)
	}
	logrus.Warnf("unsupported Produce version %d", version)
	return nil, fmt.Errorf("unsupported Produce version %d", version)
}

func (b *Broker) SaslAuthenticate(conn *knet.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error) {
	networkContext := b.getCtx(conn)
	version := req.ApiVersion
	if version == 1 || version == 2 {
		return b.ReactSaslHandshakeAuth(req, networkContext)
	}
	logrus.Warnf("unsupported SaslAuthenticate version %d", version)
	return nil, fmt.Errorf("unsupported SaslAuthenticate version %d", version)
}

func (b *Broker) SaslHandshake(conn *knet.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	version := req.ApiVersion
	if version <= 1 {
		return b.ReactSasl(req)
	}
	logrus.Warnf("unsupported SaslHandshake version %d", version)
	return nil, fmt.Errorf("unsupported SaslHandshake version %d", version)
}

func (b *Broker) SyncGroup(conn *knet.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	networkContext := b.getCtx(conn)
	if !b.Authed(networkContext) {
		return nil, fmt.Errorf("only supports sasl in current version")
	}
	version := req.ApiVersion
	if version == 1 || version == 4 || version == 5 {
		return b.ReactSyncGroup(networkContext, req)
	}
	logrus.Warnf("unsupported SyncGroup version %d", version)
	return nil, fmt.Errorf("unsupported SyncGroup version %d", version)
}

// kafka action interface implement

func (b *Broker) PartitionNumAction(addr net.Addr, topic string) (int, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("get partitionNum failed. user is not found. topic: %s", topic)
		return 0, fmt.Errorf("user not found")
	}
	num, err := b.server.PartitionNum(user.username, topic)
	if err != nil {
		logrus.Errorf("get partition num failed. topic: %s, err: %s", topic, err)
		return 0, fmt.Errorf("get partition num failed")
	}
	return num, nil
}

func (b *Broker) TopicListAction(addr net.Addr) ([]string, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("get topics list failed. user not found. addr: %s", addr.String())
		return nil, fmt.Errorf("user not found")
	}
	topic, err := b.server.ListTopic(user.username)
	if err != nil {
		logrus.Errorf("get topic list failed. err: %s", err)
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
			f.PartitionRespList[j] = b.FetchPartition(addr, topicReq.Topic, req.ClientId, partitionReq,
				req.MaxBytes, req.MinBytes, maxWaitTime/len(topicReq.PartitionReqList))
		}
		result[i] = f
	}
	return result, nil
}

func (b *Broker) partitionedTopic(user *userInfo, kafkaTopic string, partitionId int) (string, error) {
	pulsarTopic, err := b.server.PulsarTopic(user.username, kafkaTopic)
	if err != nil {
		return "", err
	}
	return pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partitionId), nil
}

// FetchPartition visible for testing
func (b *Broker) FetchPartition(addr net.Addr, kafkaTopic, clientID string, req *codec.FetchPartitionReq, maxBytes int, minBytes int, maxWaitMs int) *codec.FetchPartitionResp {
	start := time.Now()
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	records := make([]*codec.Record, 0)
	recordBatch := codec.RecordBatch{Records: records}
	if !exist {
		logrus.Errorf("fetch partition failed when get userinfo by addr %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &codec.FetchPartitionResp{
			PartitionIndex: req.PartitionId,
			ErrorCode:      codec.UNKNOWN_SERVER_ERROR,
			RecordBatch:    &recordBatch,
		}
	}
	logrus.Infof("%s fetch topic: %s partition %d", addr.String(), kafkaTopic, req.PartitionId)
	partitionedTopic, err := b.partitionedTopic(user, kafkaTopic, req.PartitionId)
	if err != nil {
		logrus.Errorf("fetch partition failed when get pulsar topic %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &codec.FetchPartitionResp{
			PartitionIndex: req.PartitionId,
			ErrorCode:      codec.UNKNOWN_SERVER_ERROR,
			RecordBatch:    &recordBatch,
		}
	}
	b.mutex.RLock()
	consumerMetadata, exist := b.consumerManager[partitionedTopic+clientID]
	if !exist {
		groupId, exist := b.topicGroupManager[partitionedTopic]
		b.mutex.RUnlock()
		if exist {
			group, err := b.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				logrus.Infof("group is preparing rebalance. grouId: %s, topic: %s", groupId, partitionedTopic)
				return &codec.FetchPartitionResp{
					LastStableOffset: 0,
					ErrorCode:        codec.NONE,
					LogStartOffset:   0,
					RecordBatch:      &recordBatch,
					PartitionIndex:   req.PartitionId,
				}
			}
		}
		// Maybe this partition-topic is already assigned to another member
		logrus.Warnf("can not find consumer for topic: %s when fetch partition %s", partitionedTopic, partitionedTopic+clientID)
		return &codec.FetchPartitionResp{
			LastStableOffset: 0,
			ErrorCode:        codec.NONE,
			LogStartOffset:   0,
			RecordBatch:      &recordBatch,
			PartitionIndex:   req.PartitionId,
		}
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
			logrus.Errorf("partitionedTopic %s read msg failed. err: %s", partitionedTopic, err)
			continue
		}
		err = consumerMetadata.consumer.Ack(message)
		if err != nil {
			logrus.Errorf("ack msg failed. topic: %s err: %s", kafkaTopic, err)
		}
		consumerMetadata.mutex.Unlock()
		byteLength = byteLength + utils.CalculateMsgLength(message)
		logrus.Infof("receive msg: %s from %s", message.ID(), message.Topic())
		offset := convOffset(message, b.config.ContinuousOffset)
		if fistMessage {
			fistMessage = false
			baseOffset = offset
		}
		relativeOffset := offset - baseOffset
		record := codec.Record{
			Value:          message.Payload(),
			RelativeOffset: int(relativeOffset),
		}
		recordBatch.Records = append(recordBatch.Records, &record)
		consumerMetadata.mutex.Lock()
		consumerMetadata.messageIds.PushBack(MessageIdPair{
			MessageId: message.ID(),
			Offset:    offset,
		})
		consumerMetadata.mutex.Unlock()
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
	}
}

func (b *Broker) GroupJoinAction(addr net.Addr, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("username not found in join group: %s", req.GroupId)
		return &codec.JoinGroupResp{
			ErrorCode:    codec.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	logrus.Infof("%s joining to group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	joinGroupResp, err := b.groupCoordinator.HandleJoinGroup(user.username, req.GroupId, req.MemberId, req.ClientId, req.ProtocolType,
		req.SessionTimeout, req.GroupProtocols)
	if err != nil {
		logrus.Errorf("unexpected exception in join group: %s, error: %s", req.GroupId, err)
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
	b.memberManager[addr.String()] = &memberInfo
	b.mutex.Unlock()
	return joinGroupResp, nil
}

func (b *Broker) GroupLeaveAction(addr net.Addr, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("username not found in leave group: %s", req.GroupId)
		return &codec.LeaveGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s leaving group: %s, members: %+v", addr.String(), req.GroupId, req.Members)
	leaveGroupResp, err := b.groupCoordinator.HandleLeaveGroup(user.username, req.GroupId, req.Members)
	if err != nil {
		logrus.Errorf("unexpected exception in leaving group: %s, error: %s", req.GroupId, err)
		return &codec.LeaveGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	group, err := b.groupCoordinator.GetGroup(user.username, req.GroupId)
	if err != nil {
		logrus.Errorf("get group %s failed, error: %s", req.GroupId, err)
		return &codec.LeaveGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	for _, topic := range group.partitionedTopic {
		b.mutex.Lock()
		consumerMetadata, exist := b.consumerManager[topic+req.ClientId]
		if exist {
			consumerMetadata.mutex.Lock()
			consumerMetadata.consumer.Close()
			consumerMetadata.mutex.Unlock()
			logrus.Infof("success close consumer topic: %s", group.partitionedTopic)
			delete(b.consumerManager, topic+req.ClientId)
			consumerMetadata = nil
		}
		client, exist := b.pulsarClientManage[topic+req.ClientId]
		if exist {
			client.Close()
			delete(b.pulsarClientManage, topic+req.ClientId)
			client = nil
		}
		delete(b.topicGroupManager, topic)
		b.mutex.Unlock()
	}
	return leaveGroupResp, nil
}

func (b *Broker) GroupSyncAction(addr net.Addr, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("username not found in sync group: %s", req.GroupId)
		return &codec.SyncGroupResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s syncing group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	syncGroupResp, err := b.groupCoordinator.HandleSyncGroup(user.username, req.GroupId, req.MemberId, req.GenerationId, req.GroupAssignments)
	if err != nil {
		logrus.Errorf("unexpected exception in sync group: %s, error: %s", req.GroupId, err)
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
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset list failed when get username by addr %s, kafka topic: %s", addr.String(), topic)
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s offset list topic: %s, partition: %d", addr.String(), topic, req.PartitionId)
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		logrus.Errorf("get topic failed. err: %s", err)
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.mutex.RLock()
	client, exist := b.pulsarClientManage[partitionedTopic+clientID]
	if !exist {
		groupId, exist := b.topicGroupManager[partitionedTopic]
		b.mutex.RUnlock()
		if exist {
			group, err := b.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				logrus.Infof("group is preparing rebalance. grouId: %s, topic: %s", groupId, partitionedTopic)
				return &codec.ListOffsetsPartitionResp{
					PartitionId: req.PartitionId,
					ErrorCode:   codec.LEADER_NOT_AVAILABLE,
					Timestamp:   constant.TimeEarliest,
				}, nil
			}
		}
		logrus.Errorf("get pulsar client failed. err: %v", err)
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
			Timestamp:   constant.TimeEarliest,
		}, nil
	}
	consumerMessages, exist := b.consumerManager[partitionedTopic+clientID]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset list failed, topic: %s, does not exist", partitionedTopic)
		return &codec.ListOffsetsPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	offset := constant.DefaultOffset
	if req.Time == constant.TimeLasted {
		latestMsgId, err := utils.GetLatestMsgId(partitionedTopic, b.pAdmin)
		if err != nil {
			logrus.Errorf("get topic %s latest offset failed %s", topic, err)
			return &codec.ListOffsetsPartitionResp{
				PartitionId: req.PartitionId,
				ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		lastedMsg, err := utils.ReadLatestMsg(partitionedTopic, b.config.MaxFetchWaitMs, latestMsgId, client)
		if err != nil {
			logrus.Errorf("read lasted latestMsgId failed. topic: %s, err: %s", topic, err)
			return &codec.ListOffsetsPartitionResp{
				PartitionId: req.PartitionId,
				ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		if lastedMsg != nil {
			consumerMessages.mutex.Lock()
			err := consumerMessages.consumer.Seek(lastedMsg.ID())
			consumerMessages.mutex.Unlock()
			if err != nil {
				logrus.Errorf("offset list failed, topic: %s, err: %s", partitionedTopic, err)
				return &codec.ListOffsetsPartitionResp{
					PartitionId: req.PartitionId,
					ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
				}, nil
			}
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
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset commit failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &codec.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		logrus.Errorf("offset commit failed when get pulsar topic %s, kafka topic: %s", addr.String(), topic)
		return &codec.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	b.mutex.RLock()
	consumerMessages, exist := b.consumerManager[partitionedTopic+clientID]
	if !exist {
		groupId, exist := b.topicGroupManager[partitionedTopic]
		b.mutex.RUnlock()
		if exist {
			group, err := b.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				logrus.Warnf("group is preparing rebalance. groupId: %s, topic: %s", groupId, partitionedTopic)
				return &codec.OffsetCommitPartitionResp{ErrorCode: codec.REBALANCE_IN_PROGRESS}, nil
			}
		}
		logrus.Warnf("commit offset failed, topic: %s, does not exist", partitionedTopic)
		return &codec.OffsetCommitPartitionResp{ErrorCode: codec.REBALANCE_IN_PROGRESS}, nil
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
		messageIdPair := front.Value.(MessageIdPair)
		// kafka commit offset maybe greater than current offset
		if messageIdPair.Offset == req.Offset || ((messageIdPair.Offset < req.Offset) && (i == length-1)) {
			err := b.offsetManager.CommitOffset(user.username, topic, consumerMessages.groupId, req.PartitionId, messageIdPair)
			if err != nil {
				logrus.Errorf("commit offset failed. topic: %s, err: %s", topic, err)
				return &codec.OffsetCommitPartitionResp{
					PartitionId: req.PartitionId,
					ErrorCode:   codec.UNKNOWN_SERVER_ERROR,
				}, nil
			}
			logrus.Infof("ack pulsar %s for %s", partitionedTopic, messageIdPair.MessageId)
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

func (b *Broker) createConsumer(partitionedTopic string, subscriptionName string, messageId pulsar.MessageID, clientId string) (chan pulsar.ConsumerMessage, pulsar.Consumer, error) {
	client, exist := b.pulsarClientManage[partitionedTopic+clientId]
	if !exist {
		var err error
		pulsarUrl := fmt.Sprintf("pulsar://%s:%d", b.config.PulsarConfig.Host, b.config.PulsarConfig.TcpPort)
		client, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
		if err != nil {
			logrus.Errorf("create pulsar client failed.")
			return nil, nil, err
		}
		b.pulsarClientManage[partitionedTopic+clientId] = client
	}
	channel := make(chan pulsar.ConsumerMessage, b.config.ConsumerReceiveQueueSize)
	options := pulsar.ConsumerOptions{
		Topic:                       partitionedTopic,
		Name:                        subscriptionName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Failover,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              channel,
		ReceiverQueueSize:           b.config.ConsumerReceiveQueueSize,
	}
	consumer, err := client.Subscribe(options)
	if err != nil {
		logrus.Warningf("subscribe consumer failed. topic: %s, err: %s", partitionedTopic, err)
		return nil, nil, err
	}
	err = consumer.Seek(messageId)
	if err != nil {
		consumer.Close()
		logrus.Warningf("seek message failed. topic: %s, err: %s", partitionedTopic, err)
	}
	return channel, consumer, nil
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
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset fetch failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s fetch topic: %s offset, partition: %d", addr.String(), topic, req.PartitionId)
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		logrus.Errorf("offset fetch failed when get pulsar topic %s, kafka topic: %s", addr.String(), topic)
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	subscriptionName, err := b.server.SubscriptionName(groupID)
	if err != nil {
		logrus.Errorf("sync group %s failed when offset fetch, error: %s", groupID, err)
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
	b.mutex.RLock()
	_, exist = b.consumerManager[partitionedTopic+clientID]
	b.mutex.RUnlock()
	if !exist {
		b.mutex.Lock()
		metadata := ConsumerMetadata{groupId: groupID, messageIds: list.New()}
		channel, consumer, err := b.createConsumer(partitionedTopic, subscriptionName, messageId, clientID)
		if err != nil {
			b.mutex.Unlock()
			logrus.Errorf("%s, create channel failed, error: %s", topic, err)
			return &codec.OffsetFetchPartitionResp{
				ErrorCode: codec.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		metadata.consumer = consumer
		metadata.channel = channel
		b.consumerManager[partitionedTopic+clientID] = &metadata
		b.mutex.Unlock()
	}
	group, err := b.groupCoordinator.GetGroup(user.username, groupID)
	if err != nil {
		logrus.Errorf("get group %s failed, error: %s", groupID, err)
		return &codec.OffsetFetchPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	if !b.checkPartitionTopicExist(group.partitionedTopic, partitionedTopic) {
		group.partitionedTopic = append(group.partitionedTopic, partitionedTopic)
	}
	b.mutex.Lock()
	b.topicGroupManager[partitionedTopic] = group.groupId
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
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset fetch failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s offset leader epoch topic: %s, partition: %d", addr.String(), topic, req.PartitionId)
	partitionedTopic, err := b.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		logrus.Errorf("get partitioned topic failed. topic: %s", topic)
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	latestMsgId, err := utils.GetLatestMsgId(partitionedTopic, b.pAdmin)
	if err != nil {
		logrus.Errorf("get last msgId failed. topic: %s", topic)
		return &codec.OffsetForLeaderEpochPartitionResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	msg, err := utils.ReadLatestMsg(partitionedTopic, b.config.MaxFetchWaitMs, latestMsgId, b.pClient)
	if err != nil {
		logrus.Errorf("get last msgId failed. topic: %s", topic)
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

func (b *Broker) getProducer(addr net.Addr, username string, topic string) (pulsar.Producer, error) {
	pulsarTopic, err := b.server.PulsarTopic(username, topic)
	if err != nil {
		logrus.Errorf("get pulsar topic failed. username: %s, topic: %s", username, topic)
		return nil, err
	}
	b.mutex.Lock()
	producer, exist := b.producerManager[addr.String()]
	if !exist {
		options := pulsar.ProducerOptions{}
		options.Topic = pulsarTopic
		options.MaxPendingMessages = b.config.MaxProducerRecordSize
		options.BatchingMaxSize = uint(b.config.MaxBatchSize)
		producer, err = b.pClient.CreateProducer(options)
		if err != nil {
			b.mutex.Unlock()
			logrus.Errorf("crate producer failed. topic: %s, err: %s", pulsarTopic, err)
			return nil, err
		}
		logrus.Infof("create producer success. addr: %s", addr.String())
		b.producerManager[addr.String()] = producer
	}
	b.mutex.Unlock()
	return producer, nil
}

func (b *Broker) ProduceAction(addr net.Addr, topic string, partition int, req *codec.ProducePartitionReq) (*codec.ProducePartitionResp, error) {
	errList := make([]*codec.RecordError, 0)
	b.mutex.RLock()
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("user not exist. username: %s, kafkaTopic: %s", user.username, topic)
		return &codec.ProducePartitionResp{
			ErrorCode:       codec.TOPIC_AUTHORIZATION_FAILED,
			RecordErrorList: errList,
			PartitionId:     req.PartitionId,
		}, nil
	}
	producer, err := b.getProducer(addr, user.username, topic)
	if err != nil {
		logrus.Errorf("create producer failed. username: %s, kafkaTopic: %s", user.username, topic)
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
		producer.SendAsync(context.Background(), &message, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			atomic.AddInt32(&count, 1)
			if err != nil {
				logrus.Errorf("send msg failed. username: %s, kafkaTopic: %s, err: %s", user.username, topic, err)
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
	_, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		b.mutex.Lock()
		b.userInfoManager[addr.String()] = &userInfo{
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

func (b *Broker) AuthGroupTopicAction(topic, groupId string) bool {
	return b.server.AuthGroupTopic(topic, groupId)
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
	user, exist := b.userInfoManager[addr.String()]
	b.mutex.RUnlock()
	if !exist {
		logrus.Errorf("HeartBeat failed when get userinfo by addr %s", addr.String())
		return &codec.HeartbeatResp{
			ErrorCode: codec.UNKNOWN_SERVER_ERROR,
		}
	}
	resp := b.groupCoordinator.HandleHeartBeat(user.username, req.GroupId, req.MemberId)
	if resp.ErrorCode == codec.REBALANCE_IN_PROGRESS {
		group, err := b.groupCoordinator.GetGroup(user.username, req.GroupId)
		if err != nil {
			logrus.Errorf("HeartBeat failed when get group by addr %s", addr.String())
			return resp
		}
		for _, topic := range group.partitionedTopic {
			b.mutex.Lock()
			consumerMetadata, exist := b.consumerManager[topic+req.ClientId]
			if exist {
				consumerMetadata.consumer.Close()
				logrus.Infof("success close consumer topic by heartbeat rebalance: %s", group.partitionedTopic)
				delete(b.consumerManager, topic+req.ClientId)
				consumerMetadata = nil
			}
			client, exist := b.pulsarClientManage[topic+req.ClientId]
			if exist {
				client.Close()
				delete(b.pulsarClientManage, topic+req.ClientId)
				client = nil
			}
			b.mutex.Unlock()
		}
	}
	return resp
}

func (b *Broker) DisconnectAction(addr net.Addr) {
	logrus.Infof("lost connection: %s", addr)
	if addr == nil {
		return
	}
	b.mutex.RLock()
	memberInfo, exist := b.memberManager[addr.String()]
	producer, producerExist := b.producerManager[addr.String()]
	b.mutex.RUnlock()
	if producerExist {
		producer.Close()
		b.mutex.Lock()
		delete(b.producerManager, addr.String())
		b.mutex.Unlock()
	}
	if !exist {
		b.mutex.Lock()
		delete(b.userInfoManager, addr.String())
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
		logrus.Errorf("leave group failed. err: %s", err)
	}
	// leave group will use user information
	b.mutex.Lock()
	delete(b.userInfoManager, addr.String())
	b.mutex.Unlock()
}
