package nats_jetstream

import (
	"context"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

const (
	isNatsRunning = true // Start NATS server.  Change this value to true.

	testURL     = "localhost"
	testSubject = "test_stream.ABC"
	testStream  = "test_stream"
	testQueue   = "test_queue"
)

// Test_Send_And_Receive creates a consumer and a sender.  The sender sends a message and sleeps
// for a small amount of time.  The test asserts that it received one message.
//
// To install the NATS server: see https://docs.nats.io/running-a-nats-service/introduction/installation
// To install the NATS CLI: see https://docs.nats.io/using-nats/nats-tools/nats_cli
// To run a NATS server:   $ nats-server -js
func Test_Send_And_Receive(t *testing.T) {
	if !isNatsRunning {
		t.Skip("Need working NATS Server")
	}
	natsOpts := []nats.Option{}
	jsmOpts := []nats.JSOpt{}
	senderOpts := []SenderOption{}
	subOpts := []nats.SubOpt{}
	consumerOpts := []ConsumerOption{}

	ctx := context.Background()
	defer ctx.Done()

	sender, err := NewSender(testURL, testStream, testSubject, natsOpts, jsmOpts, senderOpts...)
	require.NoError(t, err)
	require.NotNil(t, sender)
	defer sender.Close(ctx)

	consumer, err := NewConsumer(testURL, testStream, testSubject, natsOpts, jsmOpts, subOpts, consumerOpts...)
	require.NoError(t, err)
	require.NotNil(t, consumer)
	defer consumer.Close(ctx)

	go func() {
		err = consumer.OpenInbound(ctx)
		require.NoError(t, err)
	}()

	time.Sleep(5 * time.Second)

	evt := test.FullEvent()
	evtMessage := binding.ToMessage(&evt)
	require.NotNil(t, evtMessage)
	err = sender.Send(ctx, evtMessage)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	returnMessage, err := consumer.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, returnMessage)
}
