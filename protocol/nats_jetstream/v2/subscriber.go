/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package nats_jetstream

import (
	"github.com/nats-io/nats.go"
)

var (
	// TODO: SCT: look for this variable usage to see pull processing
	DoPullProcessing = true
)

// The Subscriber interface allows us to configure how the subscription is created
type Subscriber interface {
	Subscribe(jsm nats.JetStreamContext, subject string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error)
}

// RegularSubscriber creates regular subscriptions
type RegularSubscriber struct {
}

// Subscribe implements Subscriber.Subscribe
func (s *RegularSubscriber) Subscribe(jsm nats.JetStreamContext, subject string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	if DoPullProcessing {
		return jsm.SubscribeSync(subject, opts...)
	}
	return jsm.Subscribe(subject, cb, opts...)
}

var _ Subscriber = (*RegularSubscriber)(nil)

// QueueSubscriber creates queue subscriptions
type QueueSubscriber struct {
	Queue string
}

// Subscribe implements Subscriber.Subscribe
func (s *QueueSubscriber) Subscribe(jsm nats.JetStreamContext, subject string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	if DoPullProcessing {
		return jsm.QueueSubscribeSync(subject, s.Queue, opts...)
	}
	return jsm.QueueSubscribe(subject, s.Queue, cb, opts...)
}

var _ Subscriber = (*QueueSubscriber)(nil)
