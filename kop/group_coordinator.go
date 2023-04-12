package kop

import (
	"container/list"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

type Group struct {
	groupId            string
	partitionedTopic   []string
	groupStatus        GroupStatus
	supportedProtocol  string
	groupProtocols     map[string]string
	protocolType       string
	leader             string
	members            map[string]*memberMetadata
	canRebalance       bool
	generationId       int
	groupLock          sync.RWMutex
	groupStatusLock    sync.RWMutex
	groupMemberLock    sync.RWMutex
	groupNewMemberLock sync.RWMutex
	sessionTimeoutMs   int
}

type memberMetadata struct {
	clientId         string
	memberId         string
	metadata         []byte
	assignment       []byte
	protocolType     string
	protocols        map[string][]byte
	joinGenerationId int
	syncGenerationId int
}

type ConsumerMetadata struct {
	username   string
	groupId    string
	channel    chan pulsar.ConsumerMessage
	consumer   pulsar.Consumer
	messageIds *list.List
	mutex      sync.RWMutex
}

type GroupStatus int

const (
	PreparingRebalance GroupStatus = 1 + iota
	CompletingRebalance
	Stable
	Dead
	Empty
)

type GroupCoordinatorType string

const (
	GroupCoordinatorTypeMemory GroupCoordinatorType = "memory"
	GroupCoordinatorTypeRedis  GroupCoordinatorType = "redis"
)

const (
	EmptyMemberId = ""
)
