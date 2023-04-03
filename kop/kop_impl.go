package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
)

func (b *Broker) ConnectionClosed(conn *knet.Conn) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) AcceptError(conn *knet.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ReadError(conn *knet.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ReactError(conn *knet.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) WriteError(conn *knet.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) UnSupportedApi(conn *knet.Conn, apiKey codec.ApiCode, apiVersion int16) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ApiVersion(conn *knet.Conn, req *codec.ApiReq) (*codec.ApiResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Fetch(conn *knet.Conn, req *codec.FetchReq) (*codec.FetchResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) FindCoordinator(conn *knet.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Heartbeat(conn *knet.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) JoinGroup(conn *knet.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) LeaveGroup(conn *knet.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ListOffsets(conn *knet.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Metadata(conn *knet.Conn, req *codec.MetadataReq) (*codec.MetadataResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) OffsetCommit(conn *knet.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) OffsetFetch(conn *knet.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) OffsetForLeaderEpoch(conn *knet.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Produce(conn *knet.Conn, req *codec.ProduceReq) (*codec.ProduceResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) SaslAuthenticate(conn *knet.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) SaslHandshake(conn *knet.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) SyncGroup(conn *knet.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	//TODO implement me
	panic("implement me")
}
