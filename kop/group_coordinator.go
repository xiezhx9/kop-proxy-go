package kop

import (
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
	metadata         string
	assignment       []byte
	protocolType     string
	protocols        map[string]string
	joinGenerationId int
	syncGenerationId int
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
