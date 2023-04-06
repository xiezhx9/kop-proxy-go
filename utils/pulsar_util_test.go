package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTenantNamespaceTopicFromPartitionedPrefix(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    PartitionedTopicInfo
		expectError bool
	}{
		{
			name:        "Invalid pattern",
			input:       "nonexistent://my-tenant/my-namespace/my-topic-partition-5",
			expectError: true,
		},
		{
			name:        "Invalid partition",
			input:       "persistent://my-tenant/my-namespace/my-topic-partition-invalid",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetTenantNamespaceTopicFromPartitionedPrefix(tc.input)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected an error, but got none")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if result.Tenant != tc.expected.Tenant {
					t.Errorf("expected tenant %s, but got %s", tc.expected.Tenant, result.Tenant)
				}

				if result.Namespace != tc.expected.Namespace {
					t.Errorf("expected namespace %s, but got %s", tc.expected.Namespace, result.Namespace)
				}

				if result.Topic != tc.expected.Topic {
					t.Errorf("expected topic %s, but got %s", tc.expected.Topic, result.Topic)
				}

				if result.Partition != tc.expected.Partition {
					t.Errorf("expected partition %d, but got %d", tc.expected.Partition, result.Partition)
				}
			}
		})
	}
}

func TestGetTenantNamespaceTopicFromPartitionedTopic(t *testing.T) {
	tenant, namespace, shortPartitionedTopic, err := getTenantNamespaceTopicFromPartitionedTopic("persistent://public/default/topic-x-y-z-partition-0")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "public", tenant)
	assert.Equal(t, "default", namespace)
	assert.Equal(t, "topic-x-y-z-partition-0", shortPartitionedTopic)
}
