package kop

import (
	"github.com/go-redis/redis/v8"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
)

type GroupCoordinatorCluster struct {
	groupManager map[string]*Group
	redisdb      redis.Cmdable
}

func NewGroupCoordinatorCluster(redisConfig RedisConfig) *GroupCoordinatorCluster {
	g := &GroupCoordinatorCluster{}
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

func (gcc *GroupCoordinatorCluster) HandleJoinGroup(username, groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
	protocols []*codec.GroupProtocol) (*codec.JoinGroupResp, error) {
	panic("implement handle join group")
}

func (gcc *GroupCoordinatorCluster) HandleSyncGroup(username, groupId, memberId string, generation int,
	groupAssignments []*codec.GroupAssignment) (*codec.SyncGroupResp, error) {
	panic("implement handle sync group")
}

func (gcc *GroupCoordinatorCluster) HandleLeaveGroup(username, groupId string,
	members []*codec.LeaveGroupMember) (*codec.LeaveGroupResp, error) {
	panic("implement handle leave group")
}

func (gcc *GroupCoordinatorCluster) HandleHeartBeat(username, groupId, memberId string) *codec.HeartbeatResp {
	panic("implement handle heart beat")
}

func (gcc *GroupCoordinatorCluster) GetGroup(username, groupId string) (*Group, error) {
	panic("implement get group")
}

func (gcc *GroupCoordinatorCluster) DelGroup(username, groupId string) {
	panic("implement get group")
}
