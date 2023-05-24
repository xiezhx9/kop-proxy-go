package kop

import (
	"fmt"
	"github.com/Shoothzj/gox/set"
	"github.com/Shoothzj/gox/syncx"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"github.com/protocol-laboratory/kop-proxy-go/log"
	"github.com/protocol-laboratory/pulsar-admin-go/padmin"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
)

type Config struct {
	PulsarConfig PulsarConfig
	NetConfig    knet.KafkaNetServerConfig
	RedisConfig  RedisConfig

	ClusterId     string
	NodeId        int32
	AdvertiseHost string
	AdvertisePort int

	NeedSasl            bool
	ContinuousOffset    bool
	RecordHeaderSupport bool

	MaxConn                  int32
	MaxConsumersPerGroup     int
	GroupMinSessionTimeoutMs int
	GroupMaxSessionTimeoutMs int
	ConsumerReceiveQueueSize int
	MaxFetchRecord           int
	MinFetchWaitMs           int
	MaxFetchWaitMs           int
	MaxProducerRecordSize    int
	MaxBatchSize             int
	// PulsarTenant use for kop internal
	PulsarTenant string
	// PulsarNamespace use for kop internal
	PulsarNamespace string
	// OffsetTopic use to store kafka offset
	OffsetTopic string
	// OffsetPersistentFrequency specifies the frequency at which committed offsets should be persistent
	// per topic per consumer group per partition, in seconds. 0 means immediately
	OffsetPersistentFrequency int
	// AutoCreateOffsetTopic if true, create offset topic automatically
	AutoCreateOffsetTopic bool
	// GroupCoordinatorType enum: Standalone, Cluster; default Standalone
	GroupCoordinatorType GroupCoordinatorType
	// InitialDelayedJoinMs
	InitialDelayedJoinMs int
	// RebalanceTickMs
	RebalanceTickMs int
	// TopicLevelMetricsDisable if true, disable topic level metrics
	TopicLevelMetricsDisable bool
	// NetworkDebugEnable print network layer log
	NetworkDebugEnable bool

	DebugKafkaTopicSet  set.Set[string]
	DebugPulsarTopicSet set.Set[string]

	LogFormatter logrus.Formatter
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

type RedisType string

const (
	RedisStandalone RedisType = "standalone"
	RedisCluster    RedisType = "cluster"
)

type RedisConfig struct {
	Addr      []string
	Password  string
	DB        int
	RedisType RedisType
}

type Server interface {
	Auth(username string, password string, clientId string) (bool, error)

	AuthTopic(username string, password, clientId, topic, permissionType string) (bool, error)

	// AuthTopicGroup check if group is valid
	AuthTopicGroup(username string, password, clientId, consumerGroup string) (bool, error)

	// AuthGroupTopic check if group has permission on topic
	AuthGroupTopic(topic, groupId string) bool

	SubscriptionName(groupId string) (string, error)

	// PulsarTopic the corresponding topic in pulsar
	PulsarTopic(username, topic string) (string, error)

	PartitionNum(username, topic string) (int, error)

	ListTopic(username string) ([]string, error)

	HasFlowQuota(username, topic string) bool
}

type Broker struct {
	server            Server
	knetServer        *knet.KafkaNetServer
	connCount         int32
	connMutex         sync.Mutex
	ConnMap           syncx.Map[string, net.Conn]
	SaslMap           syncx.Map[string, codec.SaslAuthenticateReq]
	config            *Config
	pClient           pulsar.Client
	pAdmin            *padmin.PulsarAdmin
	mutex             sync.RWMutex
	groupCoordinator  GroupCoordinator
	producerManager   map[net.Addr]pulsar.Producer
	consumerManager   map[string]*PulsarConsumerHandle
	userInfoManager   map[net.Addr]*userInfo
	memberManager     map[net.Addr]*MemberInfo
	topicGroupManager map[string]string
	topicAddrManager  *syncx.SyncTable[string, string, net.Addr]
	offsetManager     OffsetManager
	logger            log.Logger
}

func NewKop(impl Server, config *Config) (*Broker, error) {
	if config.DebugKafkaTopicSet == nil {
		config.DebugKafkaTopicSet = set.Set[string]{}
	}
	if config.DebugPulsarTopicSet == nil {
		config.DebugPulsarTopicSet = set.Set[string]{}
	}
	broker := &Broker{
		server: impl,
		config: config,
		logger: log.NewLoggerWithLogrus(logrus.New(), config.LogFormatter),
	}
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
		logrus.Errorf("new offset manager failed: %v", err)
		broker.Close()
		return nil, err
	}
	offsetChannel := broker.offsetManager.Start()
	for {
		if <-offsetChannel {
			break
		}
	}
	if config.GroupCoordinatorType == GroupCoordinatorTypeMemory {
		broker.groupCoordinator = NewGroupCoordinatorMemory(config)
	} else if config.GroupCoordinatorType == GroupCoordinatorTypeRedis {
		if broker.groupCoordinator, err = NewGroupCoordinatorRedis(config.RedisConfig); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.Errorf("unexpect GroupCoordinatorType: %v", config.GroupCoordinatorType)
	}
	broker.consumerManager = make(map[string]*PulsarConsumerHandle)
	broker.userInfoManager = make(map[net.Addr]*userInfo)
	broker.memberManager = make(map[net.Addr]*MemberInfo)
	broker.topicGroupManager = make(map[string]string)
	broker.topicAddrManager = syncx.NewSyncTable[string, string, net.Addr]()
	broker.producerManager = make(map[net.Addr]pulsar.Producer)
	broker.knetServer, err = knet.NewKafkaNetServer(config.NetConfig, broker)
	if err != nil {
		logrus.Errorf("start failed: %v", err)
		broker.Close()
		return nil, err
	}
	return broker, nil
}

func (b *Broker) Run() error {
	logrus.Infof("kop broker running")
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logrus.Errorf("panic: kop broker stop: %v", err)
			}
			logrus.Errorf("kop broker stopped")
		}()
		b.knetServer.Run()
	}()
	return nil
}

func (b *Broker) CloseServer() {
	if err := b.knetServer.Stop(); err != nil {
		logrus.Errorf("stop broker failed: %v", err)
	}
	b.pClient.Close()
}

func (b *Broker) getCtx(conn *knet.Conn) *NetworkContext {
	b.connMutex.Lock()
	defer b.connMutex.Unlock()
	connCtx := conn.Context()
	if connCtx == nil {
		conn.SetContext(&NetworkContext{
			Addr: conn.RemoteAddr(),
		})
	}
	return conn.Context().(*NetworkContext)
}

func (b *Broker) Authed(ctx *NetworkContext) bool {
	if !b.config.NeedSasl {
		return true
	}
	return ctx.authed
}

func (b *Broker) checkSasl(ctx *NetworkContext) bool {
	if !b.config.NeedSasl {
		return true
	}
	_, ok := b.SaslMap.Load(ctx.Addr.String())
	return ok
}

func (b *Broker) checkSaslGroup(ctx *NetworkContext, groupId string) bool {
	if !b.config.NeedSasl {
		return true
	}
	saslReq, ok := b.SaslMap.Load(ctx.Addr.String())
	if !ok {
		return false
	}
	res, code := b.SaslAuthConsumerGroupAction(ctx.Addr, saslReq, groupId)
	if code != 0 || !res {
		return false
	}
	return true
}

func (b *Broker) checkSaslTopic(ctx *NetworkContext, topic, permissionType string) bool {
	if !b.config.NeedSasl {
		return true
	}
	saslReq, ok := b.SaslMap.Load(ctx.Addr.String())
	if !ok {
		return false
	}
	res, code := b.SaslAuthTopicAction(ctx.Addr, saslReq, topic, permissionType)
	if code != 0 || !res {
		return false
	}
	return true
}
