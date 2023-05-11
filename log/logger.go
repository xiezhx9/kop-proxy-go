package log

import (
	"github.com/sirupsen/logrus"
	"net"
)

const (
	FieldsAddr     = "addr"
	FieldsClientID = "clientId"
	FieldsGroupID  = "groupId"
	FieldsTopic    = "topic"
)

type Logger interface {
	Addr(addr net.Addr) Logger
	ClientID(id string) Logger
	Topic(topic string) Logger
	GroupID(id string) Logger

	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type logrusWrapper struct {
	l logrus.FieldLogger
}

func NewLoggerWithLogrus(logger *logrus.Logger, formatter logrus.Formatter) Logger {
	if formatter != nil {
		logger.SetFormatter(formatter)
	}
	return &logrusWrapper{l: logger}
}

func (l *logrusWrapper) Addr(addr net.Addr) Logger {
	return &logrusWrapper{
		l: l.l.WithFields(logrus.Fields{
			FieldsAddr: addr.String(),
		}),
	}
}

func (l *logrusWrapper) ClientID(id string) Logger {
	return &logrusWrapper{
		l: l.l.WithFields(logrus.Fields{
			FieldsClientID: id,
		}),
	}
}

func (l *logrusWrapper) Topic(topic string) Logger {
	return &logrusWrapper{
		l: l.l.WithFields(logrus.Fields{
			FieldsTopic: topic,
		}),
	}
}

func (l *logrusWrapper) GroupID(id string) Logger {
	return &logrusWrapper{
		l: l.l.WithFields(logrus.Fields{
			FieldsGroupID: id,
		}),
	}
}

func (l *logrusWrapper) Debug(args ...interface{}) {
	l.l.Debug(args...)
}

func (l *logrusWrapper) Info(args ...interface{}) {
	l.l.Info(args...)
}

func (l *logrusWrapper) Warn(args ...interface{}) {
	l.l.Warn(args...)
}

func (l *logrusWrapper) Error(args ...interface{}) {
	l.l.Error(args...)
}

func (l *logrusWrapper) Debugf(format string, args ...interface{}) {
	l.l.Debugf(format, args...)
}

func (l *logrusWrapper) Infof(format string, args ...interface{}) {
	l.l.Infof(format, args...)
}

func (l *logrusWrapper) Warnf(format string, args ...interface{}) {
	l.l.Warnf(format, args...)
}

func (l *logrusWrapper) Errorf(format string, args ...interface{}) {
	l.l.Errorf(format, args...)
}
