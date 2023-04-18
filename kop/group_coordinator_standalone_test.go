package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/stretchr/testify/assert"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestAddingMembersToGroupWithMaxConsumersCheck(t *testing.T) {
	config := &Config{
		MaxConsumersPerGroup: 3,
		RebalanceTickMs:      500,
	}
	coordinator := NewGroupCoordinatorMemory(config)

	groupID := "testGroup"
	memberIDPrefix := "testMember"

	coordinator.groupManager[groupID] = &Group{
		groupId:   groupID,
		members:   make(map[string]*memberMetadata),
		groupLock: sync.RWMutex{},
	}

	// Test adding members within the limit
	for i := 1; i <= config.MaxConsumersPerGroup; i++ {
		memberID := memberIDPrefix + string(rune(i))
		coordinator.groupManager[groupID].members[memberID] = newMemberMetadata(memberID)
		assert.True(t, len(coordinator.groupManager[groupID].members) <= config.MaxConsumersPerGroup, "Members should be added successfully within the limit")
	}

	// Test adding members exceeding the limit
	memberID := memberIDPrefix + string(rune(config.MaxConsumersPerGroup+1))
	coordinator.groupManager[groupID].members[memberID] = newMemberMetadata(memberID)
	assert.False(t, len(coordinator.groupManager[groupID].members) <= config.MaxConsumersPerGroup, "Members should not be added when the limit is reached")
}

func TestRemovingMembersFromGroup(t *testing.T) {
	config := &Config{
		MaxConsumersPerGroup: 3,
		RebalanceTickMs:      500,
	}
	coordinator := NewGroupCoordinatorMemory(config)

	groupID := "testGroup"
	memberID := "testMember"

	coordinator.groupManager[groupID] = &Group{
		groupId:   groupID,
		members:   make(map[string]*memberMetadata),
		groupLock: sync.RWMutex{},
	}

	// Test removing a non-existing member
	delete(coordinator.groupManager[groupID].members, memberID)
	_, ok := coordinator.groupManager[groupID].members[memberID]
	assert.False(t, ok, "Non-existing member should not be removed")

	// Test removing an existing member
	coordinator.groupManager[groupID].members[memberID] = newMemberMetadata(memberID)
	delete(coordinator.groupManager[groupID].members, memberID)
	_, ok = coordinator.groupManager[groupID].members[memberID]
	assert.False(t, ok, "Existing member should be removed successfully")
}

func newMemberMetadata(memberId string) *memberMetadata {
	return &memberMetadata{
		memberId:  memberId,
		protocols: make(map[string]string),
	}
}

func TestConcurrentJoinLeaveAndRebalance(t *testing.T) {
	config := &Config{
		MaxConsumersPerGroup: 5,
		RebalanceTickMs:      500,
	}
	coordinator := NewGroupCoordinatorMemory(config)

	groupID := "testGroup"
	memberIDPrefix := "testMember"
	clientIDPrefix := "testClient"

	coordinator.groupManager[groupID] = &Group{
		groupId:   groupID,
		members:   make(map[string]*memberMetadata),
		groupLock: sync.RWMutex{},
	}

	numMembers := 10
	waitTimeMs := 200

	var wg sync.WaitGroup

	// Simulate concurrent joining, waiting, and leaving of members
	for i := 1; i <= numMembers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			memberID := memberIDPrefix + strconv.Itoa(i)
			clientID := clientIDPrefix + strconv.Itoa(i)
			joinWaitLeave(coordinator, groupID, memberID, clientID, waitTimeMs)
		}(i)
	}

	wg.Wait()

	// Verify the state of the coordinator after all operations have completed
	group, err := coordinator.GetGroup("", groupID)
	if err != nil {
		t.Fatalf("Error getting group: %v", err)
	}

	// Assert that the group has no members
	if len(group.members) != 0 {
		t.Errorf("Expected 0 members in the group, got %d", len(group.members))
	}

	// Assert that the group has no leader
	if group.leader != "" {
		t.Errorf("Expected no leader in the group, got '%s'", group.leader)
	}
}

func joinWaitLeave(coordinator *GroupCoordinatorMemory, groupID, memberID, clientID string, waitTimeMs int) {
	protocolType := "testProtocol"
	sessionTimeoutMs := 5000
	protocols := []*codec.GroupProtocol{
		{
			ProtocolName:     protocolType,
			ProtocolMetadata: []byte{},
		},
	}

	// Handle join
	_, err := coordinator.HandleJoinGroup("", groupID, memberID, clientID, protocolType, sessionTimeoutMs, protocols)
	if err != nil {
		log.Printf("Error while joining group: %v", err)
		return
	}

	// Wait for some time
	time.Sleep(time.Duration(waitTimeMs) * time.Millisecond)

	// Handle leave
	members := []*codec.LeaveGroupMember{
		{
			MemberId: memberID,
		},
	}

	_, err = coordinator.HandleLeaveGroup("", groupID, members)
	if err != nil {
		log.Printf("Error while leaving group: %v", err)
		return
	}
}
