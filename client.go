package client

import (
	"context"
	"os"

	"github.com/pjvds/streamsdb/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

type TokenAuth string

func (t TokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"token": string(t)}, nil
}
func (t TokenAuth) RequireTransportSecurity() bool {
	return false
}

type Collection interface {
	Append(stream string, messages []MessageInput) (int64, error)
	Watch(stream string, from int64, count int) Watch
	Read(stream string, from int64, count int) (Slice, error)
	ReadControl(stream string, from int64, count int) (Slice, error)
}

type Connection interface {
	SetToken(token string) error
	EnableAcl(username string, password string) error
	Login(username string, password string) (string, error)
	CreateUser(username string, password string) error
	CreateCollection(name string) (Collection, error)
	GrandUserToCollection(username string, collection string) error
	Collection(name string) Collection
	Close() error
}

func (this *grpcConnection) SetToken(token string) error {
	this.ctx = metadata.AppendToOutgoingContext(this.ctx, "token", token)
	return nil
}

func (this *grpcConnection) GrandUserToCollection(username string, collection string) error {
	_, err := this.client.GrandUserToCollection(context.Background(), &api.GrandUserToCollectionRequest{
		Username:   username,
		Collection: collection})
	return err
}

func (this *grpcConnection) Login(username string, password string) (string, error) {
	r, err := this.client.Login(context.Background(), &api.LoginRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return "", err
	}

	return r.Token, nil
}

func (this *grpcConnection) EnableAcl(username string, password string) error {
	_, err := this.client.EnableAcl(context.Background(), &api.EnableAclRequest{
		Username: username,
		Password: password,
	})
	return err
}
func (this *grpcConnection) CreateCollection(name string) (Collection, error) {
	_, err := this.client.CreateCollection(context.Background(), &api.CreateCollectionRequest{
		Name: name,
	})

	if err != nil {
		return nil, err
	}

	return this.Collection(name), nil
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
	return &grpcConnection{conn, client, context.Background()}, nil
}

// TODO: this is not an explicit admin check
func (this *grpcConnection) CreateUser(username string, password string) error {
	_, err := this.client.CreateUser(this.ctx, &api.CreateUserRequest{
		Username: username,
		Password: password,
	})
	return err
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

func (this *collectionScope) ReadControl(stream string, from int64, count int) (Slice, error) {
	slice, err := this.client.ReadControl(context.Background(), &api.ReadRequest{
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
	ctx    context.Context
}
