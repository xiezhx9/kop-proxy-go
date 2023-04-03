package utils

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
	"github.com/protocol-laboratory/pulsar-admin-go/padmin"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

func PartitionedTopic(topic string, partition int) string {
	return topic + fmt.Sprintf(constant.PartitionSuffixFormat, partition)
}

func ReadEarliestMsg(partitionedTopic string, maxWaitMs int, pulsarClient pulsar.Client) (pulsar.Message, error) {
	readerOptions := pulsar.ReaderOptions{
		Topic:          partitionedTopic,
		Name:           constant.OffsetReaderEarliestName,
		StartMessageID: pulsar.EarliestMessageID(),
	}

	return readNextMsg(readerOptions, maxWaitMs, pulsarClient)
}

func ReadLatestMsg(partitionedTopic string, maxWaitMs int, messageId *padmin.MessageId, pulsarClient pulsar.Client) (pulsar.Message, error) {
	var msgId pulsar.MessageID
	bytes, err := generateMsgBytes(messageId)
	if err != nil {
		logrus.Errorf("genrate msg bytes failed. topic: %s, err: %s", partitionedTopic, err)
		return nil, err
	}
	msgId, err = pulsar.DeserializeMessageID(bytes)
	if err != nil {
		logrus.Errorf("deserialize messageId failed. msgBytes: %v, topic: %s, err: %s", messageId, partitionedTopic, err)
		return nil, err
	}
	readerOptions := pulsar.ReaderOptions{
		Topic:                   partitionedTopic,
		Name:                    constant.OffsetReaderEarliestName,
		StartMessageID:          msgId,
		StartMessageIDInclusive: true,
	}
	return readNextMsg(readerOptions, maxWaitMs, pulsarClient)
}

func GetLatestMsgId(partitionedTopic string, client *padmin.PulsarAdmin) (*padmin.MessageId, error) {
	tenant, namespace, topic, err := getTenantNamespaceTopicFromPartitionedTopic(partitionedTopic)
	if err != nil {
		return nil, err
	}
	return client.PersistentTopics.GetLastMessageId(tenant, namespace, topic)
}

func getTenantNamespaceTopicFromPartitionedTopic(partitionedTopic string) (tenant, namespace, shortPartitionedTopic string, err error) {
	if strings.Contains(partitionedTopic, "//") {
		topicArr := strings.Split(partitionedTopic, "//")
		if len(topicArr) < 2 {
			return "", "", "", errors.New("get tenant and namespace failed")
		}
		list := strings.Split(topicArr[1], "/")
		if len(list) < 3 {
			return "", "", "", errors.New("get tenant and namespace failed")
		}
		return list[0], list[1], list[2], nil
	}
	return "", "", "", errors.New("get tenant and namespace failed")
}

func readNextMsg(operation pulsar.ReaderOptions, maxWaitMs int, pulsarClient pulsar.Client) (pulsar.Message, error) {
	reader, err := pulsarClient.CreateReader(operation)
	if err != nil {
		logrus.Warnf("create pulsar latest read failed. topic: %s, err: %s", operation.Topic, err)
		return nil, err
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxWaitMs)*time.Millisecond)
	defer cancel()
	message, err := reader.Next(ctx)
	if err != nil {
		logrus.Errorf("get message failed. topic: %s, err: %s", operation.Topic, err)
		if strings.Contains(err.Error(), constant.ReadMsgTimeoutErr) {
			return message, nil
		}
		return nil, err
	}
	return message, nil
}

func CalculateMsgLength(message pulsar.Message) int {
	length := 0
	length += len([]byte(message.Key()))
	length += len(message.Payload())
	properties := message.Properties()
	for key, value := range properties {
		length += len([]byte(key))
		length += len([]byte(value))
	}
	return length
}

func generateMsgBytes(messageId *padmin.MessageId) ([]byte, error) {
	pulsarMessageData := pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(messageId.LedgerId)),
		EntryId:    proto.Uint64(uint64(messageId.EntryId)),
		BatchIndex: proto.Int32(0),
		Partition:  proto.Int32(messageId.PartitionIndex),
	}
	data, err := proto.Marshal(&pulsarMessageData)
	if err != nil {
		logrus.Errorf("unmarsha failed. msg: %v, err: %s", messageId, err)
		return nil, err
	}
	return data, nil
}
