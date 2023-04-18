package kop

import (
	"container/list"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

type PulsarConsumerHandle struct {
	username   string
	groupId    string
	channel    chan pulsar.ConsumerMessage
	consumer   pulsar.Consumer
	messageIds *list.List
	mutex      sync.RWMutex
}
