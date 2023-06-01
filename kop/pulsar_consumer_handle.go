package kop

import (
	"fmt"
	"github.com/Shoothzj/gox/listx"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"net"
	"sync"
)

type PulsarConsumerHandle struct {
	username   string
	groupId    string
	address    string
	channel    chan pulsar.ConsumerMessage
	client     pulsar.Client
	consumer   pulsar.Consumer
	messageIds *listx.List[MessageIdPair]
	mutex      sync.RWMutex
}

func (pc *PulsarConsumerHandle) close() {
	if pc.consumer != nil {
		pc.consumer.Close()
		pc.consumer = nil
	}
	if pc.client != nil {
		pc.client.Close()
		pc.client = nil
	}
}

func (b *Broker) createConsumer(addr net.Addr, username, clientID, kafkaTopic, partitionTopic string, partitionID int) error {
	groupID, groupExist := b.topicGroupManager[partitionTopic+clientID]
	if !groupExist {
		b.logger.Addr(addr).ClientID(clientID).PartitionTopic(partitionTopic).Errorf("create consumer failed: topic group not exist")
		return fmt.Errorf("create consumer failed: topic group not exist")
	}
	subscriptionName, err := b.server.SubscriptionName(groupID)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).PartitionTopic(partitionTopic).Errorf("create consumer failed when get subscription name: %v", err)
		return err
	}
	messagePair, flag := b.offsetManager.AcquireOffset(username, kafkaTopic, groupID, partitionID)
	messageId := pulsar.EarliestMessageID()
	kafkaOffset := constant.UnknownOffset
	if flag {
		kafkaOffset = messagePair.Offset + 1
		messageId = messagePair.MessageId
	}
	kafkaKey := b.offsetManager.GenerateKey(username, kafkaTopic, groupID, partitionID)
	b.logger.Addr(addr).ClientID(clientID).PartitionTopic(partitionTopic).Infof("acquire offset key: %s, partition: %d, offset: %d, message id: %s",
		kafkaKey, partitionID, kafkaOffset, messageId)

	consumerHandle, err := b.createConsumerHandle(partitionTopic, subscriptionName, clientID, messageId)
	if err != nil {
		b.logger.Addr(addr).ClientID(clientID).PartitionTopic(partitionTopic).Errorf("create consumer handle failed: %v", err)
		return err
	}
	consumerHandle.groupId = groupID
	consumerHandle.username = username
	consumerHandle.address = addr.String()
	b.consumerManager[partitionTopic+clientID] = consumerHandle
	b.logger.Addr(addr).ClientID(clientID).PartitionTopic(partitionTopic).Infof("create consumer success")
	return nil
}

func (b *Broker) createConsumerHandle(partitionedTopic string, subscriptionName, clientId string, messageId pulsar.MessageID) (*PulsarConsumerHandle, error) {
	var (
		handle = &PulsarConsumerHandle{messageIds: listx.New[MessageIdPair]()}
		err    error
	)
	pulsarUrl := fmt.Sprintf("pulsar://%s:%d", b.config.PulsarConfig.Host, b.config.PulsarConfig.TcpPort)
	handle.client, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
	if err != nil {
		b.logger.ClientID(clientId).PartitionTopic(partitionedTopic).Errorf("create pulsar client failed: %v", err)
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
		b.logger.ClientID(clientId).PartitionTopic(partitionedTopic).Warnf("subscribe consumer failed: %s", err)
		handle.close()
		return nil, err
	}
	if messageId != pulsar.EarliestMessageID() {
		err = handle.consumer.Seek(messageId)
		if err != nil {
			b.logger.ClientID(clientId).PartitionTopic(partitionedTopic).Warnf("seek message failed: %s", err)
			handle.close()
			return nil, err
		}
		b.logger.ClientID(clientId).PartitionTopic(partitionedTopic).Infof("kafka topic previous message id: %s", messageId)
	}
	b.logger.ClientID(clientId).PartitionTopic(partitionedTopic).Infof("create consumer success, subscription name: %s", subscriptionName)
	return handle, nil
}
