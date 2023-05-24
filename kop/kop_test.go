package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"github.com/protocol-laboratory/kop-proxy-go/test"
)

func testSetupKop() (*Broker, int) {
	port, err := test.AcquireUnusedPort()
	if err != nil {
		panic(err)
	}
	kopImpl, err := testSetupKopInternal(port)
	if err != nil {
		panic(err)
	}
	return kopImpl, port
}

func testSetupKopInternal(port int) (*Broker, error) {
	config := &Config{}

	config.PulsarConfig = PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650

	netServerConfig := knet.KafkaNetServerConfig{
		Host: "localhost",
		Port: port,
	}
	config.NetConfig = netServerConfig

	config.AdvertiseHost = "localhost"
	config.AdvertisePort = port

	config.MaxConsumersPerGroup = 100
	config.GroupMaxSessionTimeoutMs = 60000
	config.GroupMinSessionTimeoutMs = 0
	config.MaxFetchRecord = 10
	config.MinFetchWaitMs = 10
	config.MaxFetchWaitMs = 100
	config.PulsarTenant = "public"
	config.PulsarNamespace = "default"
	config.OffsetTopic = "kafka_offset"
	config.AutoCreateOffsetTopic = true
	config.GroupCoordinatorType = GroupCoordinatorTypeMemory
	kopImpl := &testKopImpl{}
	server, err := NewKop(kopImpl, config)
	if err != nil {
		return nil, err
	}
	err = server.Run()
	if err != nil {
		return nil, err
	}
	return server, err
}

const (
	TestSubscriptionPrefix = "kop_sub_"
	TestTopicPrefix        = "kop_topic_"
	TestDefaultTopicType   = "persistent://public/default/"
)

type testKopImpl struct {
}

func (t testKopImpl) Auth(username string, password string, clientId string) (bool, error) {
	return true, nil
}

func (t testKopImpl) AuthTopic(username string, password, clientId, topic, permissionType string) (bool, error) {
	return true, nil
}

func (t testKopImpl) AuthTopicGroup(username string, password, clientId, consumerGroup string) (bool, error) {
	return true, nil
}

func (t testKopImpl) AuthGroupTopic(topic, groupId string) bool {
	return true
}

func (t testKopImpl) SubscriptionName(groupId string) (string, error) {
	return TestSubscriptionPrefix + groupId, nil
}

func (t testKopImpl) PulsarTopic(username, topic string) (string, error) {
	return TestDefaultTopicType + TestTopicPrefix + topic, nil
}

func (t testKopImpl) PartitionNum(username, topic string) (int, error) {
	return 1, nil
}

func (t testKopImpl) ListTopic(username string) ([]string, error) {
	return nil, nil
}

func (t testKopImpl) HasFlowQuota(username, topic string) bool {
	return true
}
