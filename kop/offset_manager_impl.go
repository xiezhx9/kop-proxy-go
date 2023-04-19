package kop

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/protocol-laboratory/kop-proxy-go/model"
	"github.com/protocol-laboratory/kop-proxy-go/utils"
	"github.com/protocol-laboratory/pulsar-admin-go/padmin"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"time"
)

type OffsetManagerImpl struct {
	client            pulsar.Client
	admin             *padmin.PulsarAdmin
	producer          pulsar.Producer
	consumer          pulsar.Consumer
	offsetMap         map[string]MessageIdPair
	offsetRateLimiter *utils.KeyBasedRateLimiter
	mutex             sync.RWMutex
	offsetTenant      string
	offsetNamespace   string
	offsetTopic       string
	// startFlag is used to indicate whether offset manager has caught up with the latest offset.
	startFlag bool
}

func NewOffsetManager(client pulsar.Client, config *Config, admin *padmin.PulsarAdmin) (OffsetManager, error) {
	if config.AutoCreateOffsetTopic {
		err := admin.PersistentTopics.CreatePartitioned(config.PulsarTenant, config.PulsarNamespace, config.OffsetTopic, 1)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return nil, err
			}
		}
	}
	consumer, err := getOffsetConsumer(client, config)
	if err != nil {
		return nil, err
	}
	producer, err := getOffsetProducer(client, config)
	if err != nil {
		consumer.Close()
		return nil, err
	}
	impl := OffsetManagerImpl{
		producer:        producer,
		consumer:        consumer,
		client:          client,
		admin:           admin,
		offsetTenant:    config.PulsarTenant,
		offsetNamespace: config.PulsarNamespace,
		offsetTopic:     config.OffsetTopic,
		offsetMap:       make(map[string]MessageIdPair),
	}
	if config.OffsetPersistentFrequency != 0 {
		impl.offsetRateLimiter = utils.NewKeyBasedRateLimiter(config.OffsetPersistentFrequency, 1)
	}
	return &impl, nil
}

func (o *OffsetManagerImpl) Start() chan bool {
	offsetChannel := make(chan bool)
	o.startOffsetConsumer(offsetChannel)
	return offsetChannel
}

func (o *OffsetManagerImpl) startOffsetConsumer(c chan bool) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logrus.Errorf("startOffsetConsumer panic. err: %s", err)
			}
		}()
		var msg pulsar.Message
		for {
			var err error
			msg, err = o.getCurrentLatestMsg()
			if err != nil {
				continue
			}
			break
		}
		if msg == nil {
			o.startFlag = true
			c <- true
		}
		for receive := range o.consumer.Chan() {
			logrus.Infof("receive key: %s, msg: %s", receive.Key(), string(receive.Payload()))
			payload := receive.Payload()
			publishTime := receive.PublishTime()
			// At present, the three abnormal scenarios can be directly ack
			if err := receive.Ack(receive.Message); err != nil {
				logrus.Errorf("ack message failed. key: %s, topic: %s, error: %s",
					receive.Key(), receive.Topic(), err.Error())
			}
			if len(payload) == 0 {
				o.checkTime(msg, publishTime, c)
				logrus.Errorf("payload length is 0. key: %s", receive.Key())
				o.mutex.Lock()
				delete(o.offsetMap, receive.Key())
				o.mutex.Unlock()
				continue
			}
			var msgIdData model.MessageIdData
			err := json.Unmarshal(payload, &msgIdData)
			if err != nil {
				o.checkTime(msg, publishTime, c)
				logrus.Errorf("unmarshal failed. key: %s, topic: %s", receive.Key(), receive.Topic())
				continue
			}
			idData := msgIdData.MessageId
			msgId, err := pulsar.DeserializeMessageID(idData)
			if err != nil {
				o.checkTime(msg, publishTime, c)
				logrus.Errorf("deserialize message id failed. key: %s, err: %s", receive.Key(), err)
				continue
			}
			pair := MessageIdPair{
				MessageId: msgId,
				Offset:    msgIdData.Offset,
			}
			o.mutex.Lock()
			// If the offset is smaller than the current offset, doesn't update
			oldPair := o.offsetMap[receive.Key()]
			if oldPair.Offset < pair.Offset {
				o.offsetMap[receive.Key()] = pair
			}
			o.mutex.Unlock()
			o.checkTime(msg, publishTime, c)
		}
	}()
}

func (o *OffsetManagerImpl) getCurrentLatestMsg() (pulsar.Message, error) {
	partitionedTopic := o.offsetTopic + fmt.Sprintf(constant.PartitionSuffixFormat, 0)
	messageId, err := o.admin.PersistentTopics.GetLastMessageId(o.offsetTenant, o.offsetNamespace, partitionedTopic)
	if err != nil {
		logrus.Errorf("get lasted msgId failed. err: %s", err)
		return nil, err
	}
	msg, err := utils.ReadLatestMsg(partitionedTopic, 500, messageId, o.client)
	if err != nil {
		logrus.Errorf("read Lasted Msg failed. err: %s", err)
		return nil, err
	}
	return msg, nil
}

func (o *OffsetManagerImpl) CommitOffset(username, kafkaTopic, groupId string, partition int, pair MessageIdPair) error {
	key := o.GenerateKey(username, kafkaTopic, groupId, partition)
	o.mutex.Lock()
	o.offsetMap[key] = pair
	o.mutex.Unlock()

	if o.offsetRateLimiter != nil && !o.offsetRateLimiter.Acquire(key) {
		return nil
	}

	data := model.MessageIdData{}
	data.MessageId = pair.MessageId.Serialize()
	data.Offset = pair.Offset
	marshal, err := json.Marshal(data)
	if err != nil {
		logrus.Errorf("convert msg to bytes failed. kafkaTopic: %s, err: %s", kafkaTopic, err)
		return err
	}
	message := pulsar.ProducerMessage{}
	message.Payload = marshal
	message.Key = key
	_, err = o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Errorf("commit offset failed. kafkaTopic: %s, offset: %d, err: %s", kafkaTopic, pair.Offset, err)
		return err
	}
	logrus.Infof("kafkaTopic: %s commit offset %d success", kafkaTopic, pair.Offset)
	return nil
}

func (o *OffsetManagerImpl) AcquireOffset(username, kafkaTopic, groupId string, partition int) (MessageIdPair, bool) {
	key := o.GenerateKey(username, kafkaTopic, groupId, partition)
	o.mutex.RLock()
	pair, exist := o.offsetMap[key]
	o.mutex.RUnlock()
	return pair, exist
}

func (o *OffsetManagerImpl) RemoveOffset(username, kafkaTopic, groupId string, partition int) bool {
	key := o.GenerateKey(username, kafkaTopic, groupId, partition)
	logrus.Infof("begin remove offset key: %s", key)
	message := pulsar.ProducerMessage{}
	message.Key = key
	_, err := o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Errorf("send msg failed. kafkaTopic: %s, err: %s", kafkaTopic, err)
		return false
	}
	logrus.Infof("kafkaTopic: %s remove offset success", kafkaTopic)
	return true
}

func (o *OffsetManagerImpl) Close() {
	o.producer.Close()
	o.consumer.Close()
}

func (o *OffsetManagerImpl) GenerateKey(username, kafkaTopic, groupId string, partition int) string {
	return username + kafkaTopic + groupId + strconv.Itoa(partition)
}

func (o *OffsetManagerImpl) RemoveOffsetWithKey(key string) {
	value, exist := o.offsetMap[key]
	if !exist {
		logrus.Warnf("key is not exist in offset map. key: %s", key)
		return
	}
	o.mutex.Lock()
	delete(o.offsetMap, key)
	o.mutex.Unlock()
	message := pulsar.ProducerMessage{}
	message.Key = key
	_, err := o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Warningf("remove offset failed. key: %s, err: %s", key, err)
		return
	}
	logrus.Infof("remove offset success. key: %s, offset: %d", key, value.Offset)
}

func (o *OffsetManagerImpl) GracefulSendOffsetMessages(metadataMap map[string]*PulsarConsumerHandle) error {
	logrus.Infof("begin graceful send offset messages")
	if len(o.offsetMap) == 0 {
		logrus.Infof("offset map is empty")
		return nil
	}

	o.mutex.Lock()
	defer o.mutex.Unlock()

	for key, metadata := range metadataMap {
		topicInfo, err := utils.GetTenantNamespaceTopicFromPartitionedPrefix(key)
		if err != nil {
			logrus.Errorf("get topic info failed. key: %s, err: %s", key, err)
			continue
		}

		offsetKey := o.GenerateKey(metadata.username, topicInfo.Topic, metadata.groupId, topicInfo.Partition)

		message, ok := o.offsetMap[offsetKey]
		if !ok {
			logrus.Infof("offset is not exist. key: %s", offsetKey)
			continue
		}

		err = o.gracefulSendOffsetMessage(topicInfo, key, message)
		if err != nil {
			logrus.Errorf("graceful send offset message failed. key: %s, err: %s", key, err)
			continue
		}
	}

	return nil
}

func (o *OffsetManagerImpl) GracefulSendOffsetMessage(key string, metadata *PulsarConsumerHandle) error {
	if len(o.offsetMap) == 0 {
		logrus.Infof("offset map is empty")
		return nil
	}

	topicInfo, err := utils.GetTenantNamespaceTopicFromPartitionedPrefix(key)
	if err != nil {
		logrus.Errorf("get topic info failed. key: %s, err: %s", key, err)
		return err
	}

	offsetKey := o.GenerateKey(metadata.username, topicInfo.Topic, metadata.groupId, topicInfo.Partition)

	o.mutex.Lock()
	message, ok := o.offsetMap[offsetKey]
	o.mutex.Unlock()

	if !ok {
		logrus.Infof("offset is not exist. key: %s", offsetKey)
		return nil
	}

	return o.gracefulSendOffsetMessage(topicInfo, key, message)
}

func (o *OffsetManagerImpl) gracefulSendOffsetMessage(info utils.PartitionedTopicInfo, key string, pair MessageIdPair) error {
	logrus.Infof("begin graceful send offset message. key: %s, topic %s, partition: %d, offset: %d, message id: %s",
		key, info.Topic, info.Partition, pair.Offset, pair.MessageId)
	data := model.MessageIdData{}
	data.MessageId = pair.MessageId.Serialize()
	data.Offset = pair.Offset
	marshal, err := json.Marshal(data)
	if err != nil {
		logrus.Errorf("convert msg to bytes failed. key: %s, topic: %s partition %d, offset: %d, message id: %s, err: %s",
			key, info.Topic, info.Partition, pair.Offset, pair.MessageId, err)
		return err
	}
	message := pulsar.ProducerMessage{}
	message.Payload = marshal
	message.Key = key
	_, err = o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Errorf("commit offset failed. key: %s, topic: %s partition %d, offset: %d, message id: %s, err: %s",
			key, info.Topic, info.Partition, pair.Offset, pair.MessageId, err)
		return err
	}
	logrus.Infof("key: %s, topic: %s partition %d, offset: %d, message id: %s commit success",
		key, info.Topic, info.Partition, pair.Offset, pair.MessageId)
	return nil
}

func (o *OffsetManagerImpl) GetOffsetMap() map[string]MessageIdPair {
	return o.offsetMap
}

func (o *OffsetManagerImpl) checkTime(lastMsg pulsar.Message, currentTime time.Time, c chan bool) {
	if lastMsg != nil && (currentTime.Equal(lastMsg.PublishTime()) || currentTime.After(lastMsg.PublishTime())) && !o.startFlag {
		o.startFlag = true
		c <- true
	}
}

func getOffsetConsumer(client pulsar.Client, config *Config) (pulsar.Consumer, error) {
	subscribeName := uuid.New().String()
	logrus.Infof("start offset consume subscribe name %s", subscribeName)
	offsetTopic := getOffsetTopic(config)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       offsetTopic,
		Type:                        pulsar.Failover,
		SubscriptionName:            subscribeName,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		ReadCompacted:               true,
	})
	if err != nil {
		logrus.Errorf("subscribe consumer failed. topic: %s, err: %s", offsetTopic, err)
		return nil, err
	}
	return consumer, nil
}

func getOffsetProducer(client pulsar.Client, config *Config) (pulsar.Producer, error) {
	options := pulsar.ProducerOptions{}
	options.Topic = getOffsetTopic(config)
	options.SendTimeout = constant.DefaultProducerSendTimeout
	options.MaxPendingMessages = constant.DefaultMaxPendingMsg
	options.DisableBlockIfQueueFull = true
	producer, err := client.CreateProducer(options)
	if err != nil {
		logrus.Errorf("create producer failed. topic: %s, err: %s", options.Topic, err)
		return nil, err
	}
	return producer, nil
}

func getOffsetTopic(config *Config) string {
	return fmt.Sprintf("persistent://%s/%s/%s", config.PulsarTenant, config.PulsarNamespace, config.OffsetTopic)
}
