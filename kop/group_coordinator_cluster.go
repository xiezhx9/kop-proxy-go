package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
)

type GroupCoordinatorCluster struct {
}

func NewGroupCoordinatorCluster() *GroupCoordinatorCluster {
	return &GroupCoordinatorCluster{}
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
