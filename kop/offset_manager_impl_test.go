package kop

import (
	"encoding/json"
	"github.com/protocol-laboratory/kop-proxy-go/model"
	"github.com/protocol-laboratory/kop-proxy-go/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOffsetManagerCommitOffset(t *testing.T) {
	mockProducer := &MockProducer{}
	offsetRateLimiter := utils.NewKeyBasedRateLimiter(1, 1)

	offsetManager := &OffsetManagerImpl{
		producer:          mockProducer,
		offsetRateLimiter: offsetRateLimiter,
		offsetMap:         make(map[string]MessageIdPair),
	}

	username := "user"
	kafkaTopic := "topic"
	groupId := "group"
	partition := 1
	mockMessageID := MockMessageID{}
	pair := MessageIdPair{
		MessageId: mockMessageID,
		Offset:    123,
	}

	err := offsetManager.CommitOffset(username, kafkaTopic, groupId, partition, pair)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mockProducer.messages))

	var data model.MessageIdData
	err = json.Unmarshal(mockProducer.messages[0].Payload, &data)
	assert.Nil(t, err)

	assert.Equal(t, pair.MessageId.Serialize(), data.MessageId)
	assert.Equal(t, pair.Offset, data.Offset)

	key := offsetManager.GenerateKey(username, kafkaTopic, groupId, partition)
	assert.Equal(t, key, mockProducer.messages[0].Key)
}
