package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTenantNamespaceTopicFromPartitionedTopic(t *testing.T) {
	tenant, namespace, shortPartitionedTopic, err := getTenantNamespaceTopicFromPartitionedTopic("persistent://public/default/topic-x-y-z-partition-0")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "public", tenant)
	assert.Equal(t, "default", namespace)
	assert.Equal(t, "topic-x-y-z-partition-0", shortPartitionedTopic)
}
