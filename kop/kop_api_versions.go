package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactApiVersion(apiRequest *codec.ApiReq) (*codec.ApiResp, error) {
	logrus.Debugf("api request: %+v", apiRequest)
	resp := codec.ApiResp{
		BaseResp: codec.BaseResp{
			CorrelationId: apiRequest.CorrelationId,
		},
	}
	resp.ErrorCode = 0
	apiRespVersions := make([]*codec.ApiRespVersion, 18)
	apiRespVersions[0] = &codec.ApiRespVersion{ApiKey: codec.Produce, MinVersion: 0, MaxVersion: 8}
	apiRespVersions[1] = &codec.ApiRespVersion{ApiKey: codec.Fetch, MinVersion: 0, MaxVersion: 10}
	apiRespVersions[2] = &codec.ApiRespVersion{ApiKey: codec.ListOffsets, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[3] = &codec.ApiRespVersion{ApiKey: codec.Metadata, MinVersion: 0, MaxVersion: 9}
	apiRespVersions[4] = &codec.ApiRespVersion{ApiKey: codec.OffsetCommit, MinVersion: 0, MaxVersion: 8}
	apiRespVersions[5] = &codec.ApiRespVersion{ApiKey: codec.OffsetFetch, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[6] = &codec.ApiRespVersion{ApiKey: codec.FindCoordinator, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[7] = &codec.ApiRespVersion{ApiKey: codec.JoinGroup, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[8] = &codec.ApiRespVersion{ApiKey: codec.Heartbeat, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[9] = &codec.ApiRespVersion{ApiKey: codec.LeaveGroup, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[10] = &codec.ApiRespVersion{ApiKey: codec.SyncGroup, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[11] = &codec.ApiRespVersion{ApiKey: codec.DescribeGroups, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[12] = &codec.ApiRespVersion{ApiKey: codec.ListGroups, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[13] = &codec.ApiRespVersion{ApiKey: codec.SaslHandshake, MinVersion: 0, MaxVersion: 1}
	apiRespVersions[14] = &codec.ApiRespVersion{ApiKey: codec.ApiVersions, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[15] = &codec.ApiRespVersion{ApiKey: codec.DeleteRecords, MinVersion: 0, MaxVersion: 2}
	apiRespVersions[16] = &codec.ApiRespVersion{ApiKey: codec.OffsetForLeaderEpoch, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[17] = &codec.ApiRespVersion{ApiKey: codec.SaslAuthenticate, MinVersion: 0, MaxVersion: 2}
	resp.ApiRespVersions = apiRespVersions
	resp.ThrottleTime = 0
	return &resp, nil
}
