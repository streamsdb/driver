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

var ErrIteratorNeverAdvanced = errors.New("iterator has never advanced")
var ErrIteratorExhausted = errors.New("iterator exhausted")

type MessageIterator interface {
	io.Closer

	// Advance attempts to advance the iterator the the next available message. Advance
	// return true if there are is at least one more message available, or false if the
	// iterator has been exhausted. You must call this method before every call to Get.
	Advance() bool

	Get() (Snapshot, error)
}

type emptyMessageIterator struct{}

func (e *emptyMessageIterator) Close() error {
	return nil
}

func (e *emptyMessageIterator) Get() (*Snapshot, error) {
	return nil, ErrIteratorNeverAdvanced
}

type messageIterator struct {
	streamVersion int64
	streamHead    int64

	ctx          context.Context
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
		select {
		case <-iterator.ctx.Done():
			iterator.message = Message{}
			iterator.err = iterator.ctx.Err()
			return true
		default:
			m, err := iterator.subscription.Recv()
			if err != nil {
				iterator.message = Message{}
				iterator.err = err
				return true
			}

			switch c := m.Content.(type) {
			case *api.IterationMessage_Snapshot_:
				iterator.streamHead = c.Snapshot.Head
				iterator.streamVersion = c.Snapshot.Version

				return iterator.Advance()

			case *api.IterationMessage_Eof_:
				iterator.message = Message{}
				iterator.err = ErrIteratorExhausted
				return false

			case *api.IterationMessage_Message:
				timestamp, _ := types.TimestampFromProto(c.Message.Timestamp)
				iterator.message = Message{
					Position:  c.Message.Position,
					Type:      c.Message.Type,
					Timestamp: timestamp,
					Header:    c.Message.Header,
					Value:     c.Message.Value,
				}
				iterator.err = nil

				return true
			}
		}
	}
}

func (iterator *messageIterator) Get() (Snapshot, error) {
	return Snapshot{
		Version: iterator.streamVersion,
		Head:    iterator.streamHead,
		Message: iterator.message,
	}, iterator.err
}
