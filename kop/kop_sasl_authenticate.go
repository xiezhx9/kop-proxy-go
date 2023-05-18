package kop

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactSaslHandshakeAuth(req *codec.SaslAuthenticateReq, context *NetworkContext) (*codec.SaslAuthenticateResp, error) {
	logrus.Debugf("sasl handshake req: %+v", req)
	saslHandshakeResp := &codec.SaslAuthenticateResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	saslReq := codec.SaslAuthenticateReq{Username: req.Username, Password: req.Password, BaseReq: codec.BaseReq{ClientId: req.ClientId}}
	authResult, errorCode := b.SaslAuthAction(context.Addr, saslReq)
	if errorCode != 0 {
		logrus.Errorf("sasl auth request failed, source name: %s:%s@%s, error code: %v",
			req.Username, req.Password, context.Addr, errorCode)
		return nil, fmt.Errorf("sasl auth request failed, source name: %s:%s@%s, error code: %v",
			req.Username, req.Password, context.Addr, errorCode)
	}
	if !authResult {
		logrus.Errorf("sasl auth failed, source name: %s:%s@%s", req.Username, req.Password, context.Addr)
		return nil, fmt.Errorf("sasl auth failed, source name: %s:%s@%s", req.Username, req.Password, context.Addr)

	}
	context.Authed(true)
	b.SaslMap.Store(context.Addr.String(), saslReq)
	return saslHandshakeResp, nil
}
