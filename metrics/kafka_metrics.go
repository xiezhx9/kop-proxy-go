package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	KafkaProtocolProduceSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_produce_success_total")},
	)
	KafkaProtocolProduceFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_produce_fail_total")},
	)
	KafkaProtocolProduceLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_produce_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolFetchSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_fetch_success_total")},
	)
	KafkaProtocolFetchFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_fetch_fail_total")},
	)
	KafkaProtocolFetchLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_fetch_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolListOffsetsSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_list_offsets_success_total")},
	)
	KafkaProtocolListOffsetsFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_list_offsets_fail_total")},
	)
	KafkaProtocolListOffsetsLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_list_offsets_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolMetadataSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_metadata_success_total")},
	)
	KafkaProtocolMetadataFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_metadata_fail_total")},
	)
	KafkaProtocolMetadataLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_metadata_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolOffsetCommitSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_offset_commit_success_total")},
	)
	KafkaProtocolOffsetCommitFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_offset_commit_fail_total")},
	)
	KafkaProtocolOffsetCommitLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_offset_commit_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolOffsetFetchSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_offset_fetch_success_total")},
	)
	KafkaProtocolOffsetFetchFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_offset_fetch_fail_total")},
	)
	KafkaProtocolOffsetFetchLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_offset_fetch_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolFindCoordinatorSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_find_coordinator_success_total")},
	)
	KafkaProtocolFindCoordinatorFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_find_coordinator_fail_total")},
	)
	KafkaProtocolFindCoordinatorLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_find_coordinator_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolJoinGroupSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_join_group_success_total")},
	)
	KafkaProtocolJoinGroupFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_join_group_fail_total")},
	)
	KafkaProtocolJoinGroupLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_join_group_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolHeartbeatSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_heartbeat_success_total")},
	)
	KafkaProtocolHeartbeatFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_heartbeat_fail_total")},
	)
	KafkaProtocolHeartbeatLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_heartbeat_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolLeaveGroupSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_leave_group_success_total")},
	)
	KafkaProtocolLeaveGroupFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_leave_group_fail_total")},
	)
	KafkaProtocolLeaveGroupLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_leave_group_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolSyncGroupSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_sync_group_success_total")},
	)
	KafkaProtocolSyncGroupFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_sync_group_fail_total")},
	)
	KafkaProtocolSyncGroupLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_sync_group_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolSaslHandshakeSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_sasl_handshake_success_total")},
	)
	KafkaProtocolSaslHandshakeFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_sasl_handshake_fail_total")},
	)
	KafkaProtocolSaslHandshakeLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_sasl_handshake_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolApiVersionsSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_api_versions_success_total")},
	)
	KafkaProtocolApiVersionsFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_api_versions_fail_total")},
	)
	KafkaProtocolApiVersionsLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_api_versions_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolOffsetForLeaderEpochSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_offset_for_leader_epoch_success_total")},
	)
	KafkaProtocolOffsetForLeaderEpochFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_offset_for_leader_epoch_fail_total")},
	)
	KafkaProtocolOffsetForLeaderEpochLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_offset_for_leader_epoch_latency_ms"),
			Objectives: objectives},
	)
	KafkaProtocolSaslAuthenticateSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_sasl_authenticate_success_total")},
	)
	KafkaProtocolSaslAuthenticateFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "kafka", "protocol_sasl_authenticate_fail_total")},
	)
	KafkaProtocolSaslAuthenticateLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "kafka", "protocol_sasl_authenticate_latency_ms"),
			Objectives: objectives},
	)
)
