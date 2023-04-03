package kop

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (b *Broker) ReactSasl(req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	logrus.Debugf("sasl handshake req: %+v", req)
	saslHandshakeResp := &codec.SaslHandshakeResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	saslHandshakeResp.EnableMechanisms = make([]*codec.EnableMechanism, 1)
	saslHandshakeResp.EnableMechanisms[0] = &codec.EnableMechanism{SaslMechanism: "PLAIN"}
	return saslHandshakeResp, nil
}
