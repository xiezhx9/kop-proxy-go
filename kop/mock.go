package kop

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"sync"
)

type MockMessageID struct{}

func (m MockMessageID) Serialize() []byte {
	return []byte("mock_message_id")
}

func (m MockMessageID) LedgerID() int64 {
	return 0
}

func (m MockMessageID) EntryID() int64 {
	return 0
}

func (m MockMessageID) BatchIdx() int32 {
	return 0
}

func (m MockMessageID) PartitionIdx() int32 {
	return 0
}

func (m MockMessageID) BatchSize() int32 {
	return 0
}

func (m MockMessageID) String() string {
	return "mock_message_id"
}

type MockProducer struct {
	sync.Mutex
	messages []pulsar.ProducerMessage
}

func (mp *MockProducer) Topic() string {
	return "mock_topic"
}

func (mp *MockProducer) Name() string {
	return "mock_producer"
}

func (mp *MockProducer) Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	mp.Lock()
	defer mp.Unlock()

	if msg == nil {
		return nil, errors.New("nil message")
	}

	mp.messages = append(mp.messages, *msg)

	return nil, nil
}

func (mp *MockProducer) SendAsync(ctx context.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	// Not implemented for this test
}

func (mp *MockProducer) LastSequenceID() int64 {
	return -1
}

func (mp *MockProducer) Flush() error {
	return nil
}

func (mp *MockProducer) Close() {
	// Not implemented for this test
}
