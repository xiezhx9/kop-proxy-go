package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kop-proxy-go/constant"
)

func (b *Broker) pulsarTopic(user *userInfo, kafkaTopic string, partitionId int) (string, string, error) {
	pulsarTopic, err := b.server.PulsarTopic(user.username, kafkaTopic)
	if err != nil {
		return "", "", err
	}
	return pulsarTopic, pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partitionId), nil
}

func (b *Broker) partitionedTopic(user *userInfo, kafkaTopic string, partitionId int) (string, error) {
	pulsarTopic, err := b.server.PulsarTopic(user.username, kafkaTopic)
	if err != nil {
		return "", err
	}
	return pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partitionId), nil
}
