package client

import (
	"context"

	"github.com/pjvds/streamsdb/api"
	"google.golang.org/grpc"
)

type MessageInput struct {
	Header []byte
	Value  []byte
}

type Watch struct {
	done     chan struct{}
	Messages <-chan Message
}

func (this Watch) Close() {
	close(this.done)
}

type Connection interface {
	Append(stream string, messages []MessageInput) (int64, error)
	Watch(stream string, from int64) Watch
	Read(stream string, from int64, count int) (Slice, error)
	Close() error
}

func MustOpenDefault() Connection {
	conn, err := OpenDefault()
	if err != nil {
		panic(err)
	}

	return conn
}

func OpenDefault() (Connection, error) {
	conn, err := grpc.Dial("localhost:5000", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := api.NewStreamsClient(conn)
	return &grpcConnection{conn, client}, nil
}

func (this *grpcConnection) Append(stream string, messages []MessageInput) (int64, error) {
	inputs := make([]*api.MessageInput, len(messages), len(messages))

	for i, m := range messages {
		inputs[i] = &api.MessageInput{Header: m.Header, Value: m.Value}
	}

	result, err := this.client.Append(context.Background(), &api.AppendRequest{
		Stream:   stream,
		Messages: inputs,
	})
	if err != nil {
		return 0, err
	}
	return result.From, nil
}

type Message struct {
	Header []byte
	Value  []byte
}

type Slice struct {
	Stream   string
	From     int64
	To       int64
	Count    int32
	Next     int64
	HasNext  bool
	Head     int64
	Messages []Message
}

func (this *grpcConnection) Read(stream string, from int64, count int) (Slice, error) {
	slice, err := this.client.Read(context.Background(), &api.ReadRequest{
		Stream:   stream,
		From:     from,
		MaxCount: uint32(count),
	})

	if err != nil {
		return Slice{}, err
	}

	messages := make([]Message, len(slice.Messages), len(slice.Messages))

	for i, m := range slice.Messages {
		messages[i] = Message{Header: m.Header, Value: m.Value}
	}

	return Slice{
		Stream:   slice.Stream,
		From:     slice.From,
		To:       slice.To,
		Count:    slice.Count,
		Next:     slice.Next,
		HasNext:  slice.HasNext,
		Head:     slice.Head,
		Messages: messages,
	}, nil
}

func (this *grpcConnection) Watch(stream string, from int64) Watch {
	done := make(chan struct{})
	messages := make(chan Message)

	go func() {
		defer close(messages)

		watch, err := this.client.Watch(context.Background(), &api.ReadRequest{Stream: stream, From: from, MaxCount: 50})
		if err != nil {
			return
		}

		for {
			slice, err := watch.Recv()
			if err != nil {
				return
			}
			for _, m := range slice.Messages {
				select {
				case messages <- Message{
					Header: m.Header,
					Value:  m.Value,
				}:
					continue
				case <-done:
					return
				}

			}
		}
	}()

	return Watch{done, messages}
}

func (this *grpcConnection) Close() error {
	return this.conn.Close()
}

type grpcConnection struct {
	conn   *grpc.ClientConn
	client api.StreamsClient
}
