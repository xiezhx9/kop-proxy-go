package utils

import "github.com/Shoothzj/gox/set"

func DebugTopicMatch(kSet, pSet set.Set[string], kTopic, pTopic string) bool {
	return DebugKafkaTopicMatch(kSet, kTopic) || DebugPulsarPartitionTopicMatch(pSet, pTopic)
}

func DebugKafkaTopicMatch(s set.Set[string], topic string) bool {
	return s.Contains(topic)
}

func DebugPulsarPartitionTopicMatch(s set.Set[string], partitionTopic string) bool {
	return s.Contains(partitionTopic)
}
