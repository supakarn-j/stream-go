package stream

import (
	"context"
	"time"
)

type fakeClient struct {
	closed bool

	pushStream  string
	pushMaxLen  int64
	pushMessage map[string]interface{}
	pushErr     error

	registerStream  string
	registerStreams []string
	registerGroup   string
	registerName    string
	registerErr     error

	readStream       string
	readStreams      []string
	readGroup        string
	readConsumerName string
	readCount        int64
	readRetryIn      time.Duration
	readMessages     []Message
	readErr          error

	ackStream string
	ackGroup  string
	ackIDs    []string
	ackErr    error
}

type fakeLogger struct {
	debugfCalled bool
	infofCalled  bool
	errorfCalled bool
}

func (l *fakeLogger) Debug(args ...interface{}) {}
func (l *fakeLogger) Debugf(format string, args ...interface{}) {
	l.debugfCalled = true
}
func (l *fakeLogger) Info(args ...interface{}) {}
func (l *fakeLogger) Infof(format string, args ...interface{}) {
	l.infofCalled = true
}
func (l *fakeLogger) Warn(args ...interface{})                 {}
func (l *fakeLogger) Warnf(format string, args ...interface{}) {}
func (l *fakeLogger) Error(args ...interface{})                {}
func (l *fakeLogger) Errorf(format string, args ...interface{}) {
	l.errorfCalled = true
}

func (f *fakeClient) Close() {
	f.closed = true
}

func (f *fakeClient) Push(ctx context.Context, stream string, maxLen int64, message map[string]interface{}) error {
	f.pushStream = stream
	f.pushMaxLen = maxLen
	f.pushMessage = message
	return f.pushErr
}

func (f *fakeClient) RegisterConsumer(ctx context.Context, stream, group, name string) error {
	f.registerStream = stream
	f.registerStreams = append(f.registerStreams, stream)
	f.registerGroup = group
	f.registerName = name
	return f.registerErr
}

func (f *fakeClient) Read(ctx context.Context, streamName, group, consumerName string, count int64, retryIn time.Duration) ([]Message, error) {
	return f.ReadStreams(ctx, []string{streamName}, group, consumerName, count, retryIn)
}

func (f *fakeClient) ReadStreams(ctx context.Context, streams []string, group, consumerName string, count int64, retryIn time.Duration) ([]Message, error) {
	f.readStreams = append([]string(nil), streams...)
	if len(streams) > 0 {
		f.readStream = streams[0]
	}
	f.readGroup = group
	f.readConsumerName = consumerName
	f.readCount = count
	f.readRetryIn = retryIn
	return f.readMessages, f.readErr
}

func (f *fakeClient) Ack(ctx context.Context, stream, group string, ids ...string) error {
	f.ackStream = stream
	f.ackGroup = group
	f.ackIDs = append([]string(nil), ids...)
	return f.ackErr
}
