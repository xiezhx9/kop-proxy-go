package kop

import (
	"github.com/go-redis/redis/v8"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
)

type GroupForRedis struct {
	GroupStatus       GroupStatus                `json:"GroupStatus,omitempty"`
	MemberIds         map[string]*MemberForRedis `json:"MemberIds,omitempty"`
	CanRebalance      bool                       `json:"CanRebalance,omitempty"`
	GenerationId      int                        `json:"GenerationId,omitempty"`
	Leader            string                     `json:"Leader,omitempty"`
	SupportedProtocol string                     `json:"SupportedProtocol,omitempty"`
	ProtocolType      string                     `json:"ProtocolType,omitempty"`
}

type MemberForRedis struct {
	ClientId         string            `json:"ClientId,omitempty"`
	MemberId         string            `json:"MemberId,omitempty"`
	JoinGenerationId int               `json:"JoinGenerationId,omitempty"`
	SyncGenerationId int               `json:"SyncGenerationId,omitempty"`
	Metadata         []byte            `json:"Metadata,omitempty"`
	Assignment       []byte            `json:"Assignment,omitempty"`
	ProtocolType     string            `json:"ProtocolType,omitempty"`
	Protocols        map[string][]byte `json:"Protocols,omitempty"`
}

type GroupCoordinatorRedis struct {
	groupManager map[string]*Group
	redisdb      redis.Cmdable
}

func NewGroupCoordinatorRedis(redisConfig RedisConfig) *GroupCoordinatorRedis {
	g := &GroupCoordinatorRedis{}
	g.groupManager = make(map[string]*Group)
	var redisdb redis.Cmdable
	if redisConfig.RedisType == RedisCluster {
		redisdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    redisConfig.Addr,
			Password: redisConfig.Password,
		})
	} else {
		redisdb = redis.NewClient(&redis.Options{
			Addr:     redisConfig.Addr[0],
			Password: redisConfig.Password,
		})
	}
	g.redisdb = redisdb
	return g
}

func (gcc *GroupCoordinatorRedis) HandleJoinGroup(username, groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
	protocols []*codec.GroupProtocol) (*codec.JoinGroupResp, error) {
	panic("implement handle join group")
}

func (gcc *GroupCoordinatorRedis) HandleSyncGroup(username, groupId, memberId string, generation int,
	groupAssignments []*codec.GroupAssignment) (*codec.SyncGroupResp, error) {
	panic("implement handle sync group")
}

func (gcc *GroupCoordinatorRedis) HandleLeaveGroup(username, groupId string,
	members []*codec.LeaveGroupMember) (*codec.LeaveGroupResp, error) {
	panic("implement handle leave group")
}

func (gcc *GroupCoordinatorRedis) HandleHeartBeat(username, groupId, memberId string) *codec.HeartbeatResp {
	panic("implement handle heart beat")
}

func (gcc *GroupCoordinatorRedis) GetGroup(username, groupId string) (*Group, error) {
	panic("implement get group")
}

func (gcc *GroupCoordinatorRedis) DelGroup(username, groupId string) {
	panic("implement get group")
}
