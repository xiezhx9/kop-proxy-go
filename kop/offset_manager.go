package kop

type OffsetManager interface {
	Start() chan bool

	CommitOffset(username, kafkaTopic, groupId string, partition int, pair MessageIdPair) error

	AcquireOffset(username, kafkaTopic, groupId string, partition int) (MessageIdPair, bool)

	RemoveOffset(username, kafkaTopic, groupId string, partition int) bool

	GenerateKey(username, kafkaTopic, groupId string, partition int) string

	RemoveOffsetWithKey(key string)

	GetOffsetMap() map[string]MessageIdPair

	Close()
}
