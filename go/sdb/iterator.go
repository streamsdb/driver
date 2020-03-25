package sdb

import (
	"context"
	"io"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/streamsdb/driver/go/sdb/internal/api"
)

type SliceIterator interface {
	io.Closer

	Advance() bool
	Get() (*Slice, error)
}

type MessageIterator interface {
	io.Closer

	Version() int64
	Head() int64

	Advance() bool
	Get() (Snapshot, error)
}

type emptyMessageIterator struct{}

func (e *emptyMessageIterator) Version() int64 {
	return 0
}

func (e *emptyMessageIterator) Head() int64 {
	return 0
}

func (e *emptyMessageIterator) Close() error {
	return nil
}

func (e *emptyMessageIterator) Advance() bool {
	return false
}

func (e *emptyMessageIterator) Get() (Snapshot, error) {
	return Snapshot{}, errors.New("iterator never advanced")
}

func (e *messageIterator) Version() int64 {
	return e.streamVersion
}

func (e *messageIterator) Head() int64 {
	return e.streamHead
}

type messageIterator struct {
	streamVersion int64
	streamHead    int64

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
	for {
		m, err := iterator.subscription.Recv()
		iterator.err = err

		if err != nil {
			if err == io.EOF {
				return false
			}
			return true
		}

		switch c := m.Content.(type) {
		case *api.IterationMessage_Snapshot:
			iterator.streamHead = c.Snapshot.Head
			iterator.streamVersion = c.Snapshot.Version

		case *api.IterationMessage_Message:
			timestamp, _ := types.TimestampFromProto(c.Message.Timestamp)
			iterator.message = Message{
				Position:  c.Message.Position,
				Type:      c.Message.Type,
				Timestamp: timestamp,
				Header:    c.Message.Header,
				Value:     c.Message.Value,
			}

			return true
		}
	}
}

func (iterator *messageIterator) Get() (Snapshot, error) {
	return Snapshot{Version: iterator.streamVersion, Head: iterator.streamHead, Message: iterator.message}, iterator.err
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
