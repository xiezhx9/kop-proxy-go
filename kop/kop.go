package kop

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"github.com/protocol-laboratory/pulsar-admin-go/padmin"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
)

type Config struct {
	PulsarConfig PulsarConfig
	NetConfig    knet.KafkaNetServerConfig
	RedisConfig  RedisConfig

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
	MaxProducerRecordSize    int
	MaxBatchSize             int
	// PulsarTenant use for kafsar internal
	PulsarTenant string
	// PulsarNamespace use for kafsar internal
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

	AuthTopicGroup(username string, password, clientId, consumerGroup string) (bool, error)

	AuthGroupTopic(topic, groupId string) bool

	SubscriptionName(groupId string) (string, error)

	// PulsarTopic the corresponding topic in pulsar
	PulsarTopic(username, topic string) (string, error)

	PartitionNum(username, topic string) (int, error)

	ListTopic(username string) ([]string, error)

	HasFlowQuota(username, topic string) bool
}

type KafkaAction interface {
	PartitionNumAction(addr net.Addr, topic string) (int, error)

	TopicListAction(addr net.Addr) ([]string, error)

	// FetchAction method called this already authed
	FetchAction(addr net.Addr, req *codec.FetchReq) ([]*codec.FetchTopicResp, error)

	// GroupJoinAction method called this already authed
	GroupJoinAction(addr net.Addr, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error)

	// GroupLeaveAction method called this already authed
	GroupLeaveAction(addr net.Addr, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error)

	// GroupSyncAction method called this already authed
	GroupSyncAction(addr net.Addr, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error)

	// OffsetListPartitionAction method called this already authed
	OffsetListPartitionAction(addr net.Addr, topic, clientID string, req *codec.ListOffsetsPartition) (*codec.ListOffsetsPartitionResp, error)

	// OffsetCommitPartitionAction method called this already authed
	OffsetCommitPartitionAction(addr net.Addr, topic, clientID string, req *codec.OffsetCommitPartitionReq) (*codec.OffsetCommitPartitionResp, error)

	// OffsetFetchAction method called this already authed
	OffsetFetchAction(addr net.Addr, topic, clientID, groupID string, req *codec.OffsetFetchPartitionReq) (*codec.OffsetFetchPartitionResp, error)

	// OffsetLeaderEpochAction method called this already authed
	OffsetLeaderEpochAction(addr net.Addr, topic string, req *codec.OffsetLeaderEpochPartitionReq) (*codec.OffsetForLeaderEpochPartitionResp, error)

	// ProduceAction method called this already authed
	ProduceAction(addr net.Addr, topic string, partition int, req *codec.ProducePartitionReq) (*codec.ProducePartitionResp, error)

	SaslAuthAction(addr net.Addr, req codec.SaslAuthenticateReq) (bool, codec.ErrorCode)

	SaslAuthTopicAction(addr net.Addr, req codec.SaslAuthenticateReq, topic, permissionType string) (bool, codec.ErrorCode)

	AuthGroupTopicAction(topic, groupId string) bool

	SaslAuthConsumerGroupAction(addr net.Addr, req codec.SaslAuthenticateReq, consumerGroup string) (bool, codec.ErrorCode)

	HeartBeatAction(addr net.Addr, req codec.HeartbeatReq) *codec.HeartbeatResp

	DisconnectAction(addr net.Addr)
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
	consumerManager    map[string]*PulsarConsumerHandle
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
	broker.userInfoManager = make(map[string]*userInfo)
	broker.memberManager = make(map[string]*MemberInfo)
	broker.pulsarClientManage = make(map[string]pulsar.Client)
	broker.topicGroupManager = make(map[string]string)
	broker.producerManager = make(map[string]pulsar.Producer)
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
	_, ok := b.SaslMap.Load(ctx.Addr)
	return ok
}

func (b *Broker) authGroupTopic(topic, groupId string) bool {
	return b.AuthGroupTopicAction(topic, groupId)
}

func (b *Broker) checkSaslGroup(ctx *NetworkContext, groupId string) bool {
	if !b.config.NeedSasl {
		return true
	}
	saslReq, ok := b.SaslMap.Load(ctx.Addr)
	if !ok {
		return false
	}
	res, code := b.SaslAuthConsumerGroupAction(ctx.Addr, saslReq.(codec.SaslAuthenticateReq), groupId)
	if code != 0 || !res {
		return false
	}
	return true
}

func (b *Broker) checkSaslTopic(ctx *NetworkContext, topic, permissionType string) bool {
	if !b.config.NeedSasl {
		return true
	}
	saslReq, ok := b.SaslMap.Load(ctx.Addr)
	if !ok {
		return false
	}
	res, code := b.SaslAuthTopicAction(ctx.Addr, saslReq.(codec.SaslAuthenticateReq), topic, permissionType)
	if code != 0 || !res {
		return false
	}
	return true
}
