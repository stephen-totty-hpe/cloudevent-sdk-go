/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package nats_jetstream

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
)

const (
	_EMPTY_           = ""
	prefix            = "ce-"
	contentTypeHeader = "content-type"
	// see https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md
	applicationCloudEvent = "application/cloudevents"
)

var specs = spec.WithPrefix(prefix)

// Message implements binding.Message by wrapping an *nats.Msg.
// This message *can* be read several times safely
type Message struct {
	Msg      *nats.Msg
	encoding binding.Encoding
}

// NewMessage wraps an *nats.Msg in a binding.Message.
// The returned message *can* be read several times safely
func NewMessage(msg *nats.Msg) *Message {
	encoding := binding.EncodingStructured
	if msg.Header != nil {
		if msg.Header.Get(contentTypeHeader) == applicationCloudEvent {
			encoding = binding.EncodingStructured
		} else if msg.Header.Get(specs.PrefixedSpecVersionName()) != _EMPTY_ {
			encoding = binding.EncodingBinary
		}
	}
	return &Message{Msg: msg, encoding: encoding}
}

var _ binding.Message = (*Message)(nil)

// ReadEncoding return the type of the message Encoding.
func (m *Message) ReadEncoding() binding.Encoding {
	return m.encoding
}

// ReadStructured transfers a structured-mode event to a StructuredWriter.
func (m *Message) ReadStructured(ctx context.Context, encoder binding.StructuredWriter) error {
	if m.encoding != binding.EncodingStructured {
		return binding.ErrNotStructured
	}
	return encoder.SetStructuredEvent(ctx, format.JSON, bytes.NewReader(m.Msg.Data))
}

// ReadBinary transfers a binary-mode event to an BinaryWriter.
func (m *Message) ReadBinary(ctx context.Context, encoder binding.BinaryWriter) error {
	if m.encoding != binding.EncodingBinary {
		return binding.ErrNotBinary
	}

	version := m.GetVersion()
	if version == nil {
		return binding.ErrNotBinary
	}

	var err error
	for k, v := range m.Msg.Header {
		headerValue := v[0]
		if strings.HasPrefix(k, prefix) {
			attr := version.Attribute(k)
			if attr != nil {
				err = encoder.SetAttribute(attr, string(headerValue))
			} else {
				err = encoder.SetExtension(strings.TrimPrefix(k, prefix), string(headerValue))
			}
		} else if k == contentTypeHeader {
			err = encoder.SetAttribute(version.AttributeFromKind(spec.DataContentType), string(headerValue))
		}
		if err != nil {
			return err
		}
	}

	if m.Msg.Data != nil {
		err = encoder.SetData(bytes.NewBuffer(m.Msg.Data))
	}

	return err
}

// Finish *must* be called when message from a Receiver can be forgotten by the receiver.
func (m *Message) Finish(err error) error {
	return nil
}

// GetAttribute implements binding.MessageMetadataReader
func (m *Message) GetAttribute(attributeKind spec.Kind) (spec.Attribute, interface{}) {
	key := withPrefix(attributeKind.String())
	if m.Msg.Header != nil {
		headerValue, ok := m.Msg.Header[key]
		if ok {
			version := m.GetVersion()
			return version.Attribute(key), headerValue
		}
	}
	return nil, nil
}

// GetExtension implements binding.MessageMetadataReader
func (m *Message) GetExtension(name string) interface{} {
	key := withPrefix(name)
	if m.Msg.Header != nil {
		headerValue, ok := m.Msg.Header[key]
		if ok {
			return headerValue
		}
	}
	return nil
}

// GetVersion looks for specVersion header and returns a Version object
func (m *Message) GetVersion() spec.Version {
	if m.Msg.Header == nil {
		return nil
	}
	versionValue := m.Msg.Header[specs.PrefixedSpecVersionName()]
	if len(versionValue) == 0 {
		return nil
	}
	return specs.Version(string(versionValue[0]))
}

// withPrefix prepends the prefix to the attribute name
func withPrefix(attributeName string) string {
	return fmt.Sprintf("%s%s", prefix, attributeName)
}
