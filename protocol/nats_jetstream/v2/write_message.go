/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package nats_jetstream

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
)

// WriteMsg fills the provided writer with the bindings.Message m.
// Using context you can tweak the encoding processing (more details on binding.Write documentation).
func WriteMsg(ctx context.Context, m binding.Message, writer io.ReaderFrom, transformers ...binding.Transformer) error {
	structuredWriter := &natsMessageWriter{writer}
	binaryWriter := &natsBinaryMessageWriter{ReaderFrom: writer}

	_, err := binding.Write(
		ctx,
		m,
		structuredWriter,
		binaryWriter,
		transformers...,
	)
	return err
}

type natsMessageWriter struct {
	io.ReaderFrom
}

// StructuredWriter  implements StructuredWriter.SetStructuredEvent
func (w *natsMessageWriter) SetStructuredEvent(_ context.Context, _ format.Format, event io.Reader) error {
	if _, err := w.ReadFrom(event); err != nil {
		return err
	}

	return nil
}

var _ binding.StructuredWriter = (*natsMessageWriter)(nil) // Test it conforms to the interface

type natsHeader map[string][]string

// Add adds the key, value pair to the header. It is case-sensitive
// and appends to any existing values associated with key.
func (h natsHeader) Add(key, value string) {
	h[key] = append(h[key], value)
}

// Set sets the header entries associated with key to the single
// element value. It is case-sensitive and replaces any existing
// values associated with key.
func (h natsHeader) Set(key, value string) {
	h[key] = []string{value}
}

// Get gets the first value associated with the given key.
// It is case-sensitive.
func (h natsHeader) Get(key string) string {
	if h == nil {
		return _EMPTY_
	}
	if v := h[key]; v != nil {
		return v[0]
	}
	return _EMPTY_
}

type natsHeaderContextKeyType string

const (
	natsHeaderContextKey natsHeaderContextKeyType = "nats.Header"
)

type natsBinaryMessageWriter struct {
	io.ReaderFrom
	header natsHeader
}

// SetAttribute implements MessageMetadataWriter.SetAttribute
func (w *natsBinaryMessageWriter) SetAttribute(attribute spec.Attribute, value interface{}) error {
	prefixedName := withPrefix(attribute.Name())
	convertedValue := fmt.Sprint(value)
	switch attribute.Kind().String() {
	case spec.Time.String():
		timeValue := value.(time.Time)
		convertedValue = timeValue.Format(time.RFC3339Nano)
	}
	w.header.Set(prefixedName, convertedValue)
	return nil
}

// SetExtension implements MessageMetadataWriter.SetExtension
func (w *natsBinaryMessageWriter) SetExtension(name string, value interface{}) error {
	prefixedName := withPrefix(name)
	convertedValue := fmt.Sprint(value)
	w.header.Set(prefixedName, convertedValue)
	return nil
}

// Start implements BinaryWriter.Start
func (w *natsBinaryMessageWriter) Start(ctx context.Context) error {
	w.header = GetHeaderFromContext(ctx)
	return nil
}

// SetData implements BinaryWriter.SetData
func (w *natsBinaryMessageWriter) SetData(data io.Reader) error {
	if _, err := w.ReadFrom(data); err != nil {
		return err
	}

	return nil
}

// End implements BinaryWriter.End
func (w *natsBinaryMessageWriter) End(ctx context.Context) error {
	return nil
}

func GetHeaderFromContext(ctx context.Context) natsHeader {
	headerCtxValue := ctx.Value(natsHeaderContextKey)
	if headerCtxValue != nil {
		return headerCtxValue.(natsHeader)
	}
	return natsHeader{}
}
