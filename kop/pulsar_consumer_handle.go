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
	client     pulsar.Client
	consumer   pulsar.Consumer
	messageIds *list.List
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
