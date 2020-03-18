package sdb

import (
	"context"
	"io"

	"github.com/gogo/protobuf/types"
	"github.com/streamsdb/driver/go/sdb/internal/api"
)

type SliceIterator interface {
	io.Closer

	Advance() bool
	Get() (*Slice, error)
}

type MessageIterator interface {
	io.Closer

	Advance() bool
	Get() (Message, error)
}

type messageIterator struct {
	cancel       context.CancelFunc
	subscription api.Streams_IterateStreamClient

	message Message
	err     error
}

func (iterator *messageIterator) Close() error {
	iterator.cancel()
	return nil
}

func (iterator *messageIterator) Advance() bool {
	m, err := iterator.subscription.Recv()
	iterator.err = err

	if err != nil {
		if err == io.EOF {
			return false
		}
		return true
	}

	timestamp, _ := types.TimestampFromProto(m.Timestamp)
	iterator.message = Message{
		Position:  m.Position,
		Type:      m.Type,
		Timestamp: timestamp,
		Header:    m.Header,
		Value:     m.Value,
	}

	return true
}

func (iterator *messageIterator) Get() (Message, error) {
	return iterator.message, iterator.err
}

type sliceIterator struct {
	done    bool
	slice   *Slice
	err     error
	advance func() (bool, *Slice, error)
}

func (iterator *sliceIterator) Advance() bool {
	if iterator.done {
		return false
	}

	iterator.done, iterator.slice, iterator.err = iterator.advance()
	return iterator.done
}

func (iterator *sliceIterator) Get() (*Slice, error) {
	return iterator.slice, iterator.err
}
