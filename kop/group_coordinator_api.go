package kop

import "github.com/protocol-laboratory/kafka-codec-go/codec"

type GroupCoordinator interface {
	HandleJoinGroup(username, groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
		protocols []*codec.GroupProtocol) (*codec.JoinGroupResp, error)

	HandleSyncGroup(username, groupId, memberId string, generation int,
		groupAssignments []*codec.GroupAssignment) (*codec.SyncGroupResp, error)

	HandleLeaveGroup(username, groupId string, members []*codec.LeaveGroupMember) (*codec.LeaveGroupResp, error)

	HandleHeartBeat(username, groupId, memberId string) *codec.HeartbeatResp

	GetGroup(username, groupId string) (*Group, error)

	DelGroup(username, groupId string)
}
