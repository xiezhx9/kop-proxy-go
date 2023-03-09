package kop

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"github.com/protocol-laboratory/pulsar-admin-go/padmin"
	"github.com/sirupsen/logrus"
	"sync"
)

type Config struct {
	PulsarConfig PulsarConfig
	knetConfig   knet.KafkaNetServerConfig

	ClusterId                string
	NodeId                   int32
	AdvertiseHost            string
	AdvertisePort            int
	NeedSasl                 bool
	MaxConn                  int32
	MaxConsumersPerGroup     int
	GroupMinSessionTimeoutMs int
	GroupMaxSessionTimeoutMs int
	ConsumerReceiveQueueSize int
	MaxFetchRecord           int
	MinFetchWaitMs           int
	MaxFetchWaitMs           int
	ContinuousOffset         bool
	// PulsarTenant use for kafsar internal
	PulsarTenant string
	// PulsarNamespace use for kafsar internal
	PulsarNamespace string
	// OffsetTopic use to store kafka offset
	OffsetTopic string
	// AutoCreateOffsetTopic if true, create offset topic automatically
	AutoCreateOffsetTopic bool
	// GroupCoordinatorType enum: Standalone, Cluster; default Standalone
	GroupCoordinatorType GroupCoordinatorType
	// InitialDelayedJoinMs
	InitialDelayedJoinMs int
	// RebalanceTickMs
	RebalanceTickMs int
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

type Server interface {
	Auth(username string, password string, clientId string) (bool, error)

	AuthTopic(username string, password, clientId, topic, permissionType string) (bool, error)

	AuthTopicGroup(username string, password, clientId, consumerGroup string) (bool, error)

	AuthGroupTopic(topic, groupId string) bool

	SubscriptionName(groupId string) (string, error)

	// PulsarTopic the corresponding topic in pulsar
	PulsarTopic(username, topic string) (string, error)

	PartitionNum(username, topic string) (int, error)

	ListTopic(username string) ([]string, error)

	HasFlowQuota(username, topic string) bool
}

type Broker struct {
	server     Server
	knetServer *knet.KafkaNetServer

	connCount int32
	connMutex sync.Mutex
	ConnMap   sync.Map
	SaslMap   sync.Map

	config             *Config
	pClient            pulsar.Client
	pAdmin             *padmin.PulsarAdmin
	mutex              sync.RWMutex
	pulsarClientManage map[string]pulsar.Client
	groupCoordinator   GroupCoordinator
	producerManager    map[string]pulsar.Producer
	consumerManager    map[string]*ConsumerMetadata
	userInfoManager    map[string]*userInfo
	memberManager      map[string]*MemberInfo
	topicGroupManager  map[string]string
	offsetManager      OffsetManager
}

func NewKop(impl Server, config *Config) (*Broker, error) {
	broker := &Broker{server: impl, config: config}
	pulsarUrl := fmt.Sprintf("pulsar://%s:%d", config.PulsarConfig.Host, config.PulsarConfig.TcpPort)
	var err error
	broker.pClient, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
	if err != nil {
		return nil, err
	}
	broker.pAdmin, err = padmin.NewPulsarAdmin(padmin.Config{
		Host: config.PulsarConfig.Host,
		Port: config.PulsarConfig.HttpPort,
	})
	if err != nil {
		return nil, err
	}
	broker.offsetManager, err = NewOffsetManager(broker.pClient, config, broker.pAdmin)
	if err != nil {
		broker.Close()
		return nil, err
	}
	offsetChannel := broker.offsetManager.Start()
	for {
		if <-offsetChannel {
			break
		}
	}
	if config.GroupCoordinatorType == Cluster {
		broker.groupCoordinator = NewGroupCoordinatorCluster()
	} else if config.GroupCoordinatorType == Standalone {
		broker.groupCoordinator = NewGroupCoordinatorStandalone(config, broker.pClient)
	} else {
		return nil, errors.Errorf("unexpect GroupCoordinatorType: %v", config.GroupCoordinatorType)
	}
	broker.consumerManager = make(map[string]*ConsumerMetadata)
	broker.userInfoManager = make(map[string]*userInfo)
	broker.memberManager = make(map[string]*MemberInfo)
	broker.pulsarClientManage = make(map[string]pulsar.Client)
	broker.topicGroupManager = make(map[string]string)
	broker.producerManager = make(map[string]pulsar.Producer)
	broker.knetServer, err = knet.NewKafkaNetServer(config.knetConfig, broker)
	if err != nil {
		broker.Close()
		return nil, err
	}
	return broker, nil
}

func (b *Broker) Run() error {
	b.knetServer.Run()
	return nil
}

func (b *Broker) Close() {
	if err := b.knetServer.Stop(); err != nil {
		logrus.Errorf("stop broker failed: %v", err)
	}
	b.pClient.Close()
}
