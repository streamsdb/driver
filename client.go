package client

import (
	"context"
	"os"

	"github.com/pjvds/streamsdb/api"
	"google.golang.org/grpc"
)

type MessageInput struct {
	Header []byte
	Value  []byte
}

type Watch struct {
	cancel context.CancelFunc

	Slices <-chan Slice
}

func (this Watch) Close() {
	this.cancel()
}

type Collection interface {
	Append(stream string, messages []MessageInput) (int64, error)
	Watch(stream string, from int64, count int) Watch
	Read(stream string, from int64, count int) (Slice, error)
}

type Connection interface {
	EnableAcl(username string, password string) error
	Collection(name string) Collection
	Close() error
}

func (this *grpcConnection) EnableAcl(username string, password string) error {
	_, err := this.client.EnableAcl(context.Background(), &api.EnableAclRequest{
		Superuser: &api.SuperUser{
			Username:     username,
			PasswordHash: []byte(password), // TODO: hash password
		},
	})
	return err
}

func (this *grpcConnection) Collection(name string) Collection {
	return &collectionScope{this.client, name}
}

func MustOpenDefault() Connection {
	conn, err := OpenDefault()
	if err != nil {
		panic(err)
	}

	return conn
}

func OpenDefault() (Connection, error) {
	address := os.Getenv("SDB")
	if len(address) == 0 {
		address = "localhost:6000"
	}
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := api.NewStreamsClient(conn)
	return &grpcConnection{conn, client}, nil
}

func (this *collectionScope) Append(stream string, messages []MessageInput) (int64, error) {
	inputs := make([]*api.MessageInput, len(messages), len(messages))

	for i, m := range messages {
		inputs[i] = &api.MessageInput{Header: m.Header, Value: m.Value}
	}

	result, err := this.client.Append(context.Background(), &api.AppendRequest{
		Collection: this.collection,
		Stream:     stream,
		Messages:   inputs,
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

func (this *collectionScope) Read(stream string, from int64, count int) (Slice, error) {
	slice, err := this.client.Read(context.Background(), &api.ReadRequest{
		Collection: this.collection,
		Stream:     stream,
		From:       from,
		Count:      uint32(count),
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

func (this *collectionScope) Watch(stream string, from int64, count int) Watch {
	ctx, cancel := context.WithCancel(context.Background())
	slices := make(chan Slice)

	go func() {
		defer close(slices)
		defer cancel()

		watch, err := this.client.Watch(ctx, &api.ReadRequest{Collection: this.collection, Stream: stream, From: from, Count: uint32(count)})
		if err != nil {
			return
		}

		for {
			slice, err := watch.Recv()
			if err != nil {
				return
			}

			messages := make([]Message, len(slice.Messages), len(slice.Messages))

			for i, m := range slice.Messages {
				messages[i] = Message{Header: m.Header, Value: m.Value}
			}

			select {
			case slices <- Slice{
				Stream:   slice.Stream,
				From:     slice.From,
				To:       slice.To,
				Count:    slice.Count,
				Next:     slice.Next,
				HasNext:  slice.HasNext,
				Head:     slice.Head,
				Messages: messages,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return Watch{cancel, slices}
}

func (this *grpcConnection) Close() error {
	return this.conn.Close()
}

type collectionScope struct {
	client     api.StreamsClient
	collection string
}

type grpcConnection struct {
	conn   *grpc.ClientConn
	client api.StreamsClient
}
