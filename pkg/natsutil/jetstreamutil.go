package natsutil

import (
	"errors"

	"github.com/nats-io/nats.go"

	"go.uber.org/zap"
)

const (
	// StreamName is the name of StreamConfig for JetStream
	StreamName = "K-ORDERS"

	// MaxPending is the maximum outstanding async publishes that can be inflight at one time.
	MaxPending = 256
)

// JetStreamConnect creates a new NATS JetStream connection
func JetStreamConnect(jetStreamUrl string, logger *zap.SugaredLogger) (*nats.Conn, error) {
	logger.Infof("JetStreamConnect():  jetStreamUrl: %v", jetStreamUrl)
	nc, err := nats.Connect(jetStreamUrl)
	if err != nil {
		logger.Errorf("Connect(): create new connection failed: %v", err)
		return nil, err
	}
	logger.Infof("Connect(): connection to NATS JetStream established!")

	// Create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(MaxPending))
	if err != nil {
		logger.Errorf("Connect(): create JetStream connection failed: %v", err)
		return nil, err
	}

	info, err := js.StreamInfo(StreamName)
	if err != nil {
		logger.Errorf("Connect(): StreamInfo %#v failed: %s", StreamName, err)
		return nil, err
	}

	if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != StreamName+".*" {
		err = errors.New("invalid stream configuration")
		logger.Errorf("JetstreamConnect(): %s: %#v", err, info)
		return nil, err
	}

	return nc, nil
}
