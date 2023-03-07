package kop

import "github.com/apache/pulsar-client-go/pulsar"

type MessageIdPair struct {
	MessageId pulsar.MessageID
	Offset    int64
}
