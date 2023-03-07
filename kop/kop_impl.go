package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"net"
)

func (b *Broker) ConnectionClosed(conn net.Conn) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) AcceptError(conn net.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ReadError(conn net.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ReactError(conn net.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) WriteError(conn net.Conn, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) UnSupportedApi(conn net.Conn, apiKey codec.ApiCode, apiVersion int16) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ApiVersion(conn net.Conn, req *codec.ApiReq) (*codec.ApiResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Fetch(conn net.Conn, req *codec.FetchReq) (*codec.FetchResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) FindCoordinator(conn net.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Heartbeat(conn net.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) JoinGroup(conn net.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) LeaveGroup(conn net.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) ListOffsets(conn net.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Metadata(conn net.Conn, req *codec.MetadataReq) (*codec.MetadataResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) OffsetCommit(conn net.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) OffsetFetch(conn net.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) OffsetForLeaderEpoch(conn net.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) Produce(conn net.Conn, req *codec.ProduceReq) (*codec.ProduceResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) SaslAuthenticate(conn net.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) SaslHandshake(conn net.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) SyncGroup(conn net.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	//TODO implement me
	panic("implement me")
}
