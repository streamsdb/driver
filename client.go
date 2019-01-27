package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"strings"

	"github.com/pjvds/streamsdb/api"
	"google.golang.org/grpc"

	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	_ "google.golang.org/grpc/encoding/gzip"
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
	IsTokenSet() bool
	SetToken(token string) error
	EnableAcl(username string, password string) error
	Login(username string, password string) (string, error)
	CreateUser(username string, password string) error
	CreateCollection(name string) (Collection, error)
	GrandUserToCollection(username string, collection string) error
	Collection(name string) (Collection, error)
	Close() error
}

func (this *grpcConnection) IsTokenSet() bool {
	md, ok := metadata.FromIncomingContext(this.ctx)
	if !ok {
		return false
	}

	return len(md.Get("token")) > 0
}

func (this *grpcConnection) SetToken(token string) error {
	this.ctx = metadata.AppendToOutgoingContext(this.ctx, "token", token)
	return nil
}

func (this *grpcConnection) GrandUserToCollection(username string, collection string) error {
	_, err := this.client.GrandUserToCollection(this.ctx, &api.GrandUserToCollectionRequest{
		Username:   username,
		Collection: collection})
	return err
}

func (this *grpcConnection) Login(username string, password string) (string, error) {
	r, err := this.client.Login(this.ctx, &api.LoginRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return "", err
	}

	return r.Token, nil
}

func (this *grpcConnection) EnableAcl(username string, password string) error {
	_, err := this.client.EnableAcl(this.ctx, &api.EnableAclRequest{
		Username: username,
		Password: password,
	})
	return err
}
func (this *grpcConnection) CreateCollection(name string) (Collection, error) {
	r, err := this.client.CreateCollection(this.ctx, &api.CreateCollectionRequest{
		Name: name,
	})

	if err != nil {
		return nil, err
	}

	return &collectionScope{
		client:         this.client,
		collectionId:   r.CollectionId,
		collectionName: name,
		ctx:            this.ctx,
	}, nil
}

func (this *grpcConnection) Collection(name string) (Collection, error) {
	r, err := this.client.GetCollection(this.ctx, &api.GetCollectionRequest{
		Name: name,
	})
	if err != nil {
		return nil, err
	}

	return &collectionScope{this.client, r.Id, r.Name, this.ctx}, nil
}

func MustOpenDefault() Connection {
	conn, err := OpenDefault("sdb://localhost:6000?insecure=1")
	if err != nil {
		panic(err)
	}

	return conn
}

func OpenDefault(address string) (Connection, error) {
	if !strings.HasPrefix(address, "sdb://") {
		return nil, errors.New("invalid sdb host address: not starting with 'sdb://'")
	}
	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	opts := make([]grpc.DialOption, 0)
	if u.Query().Get("insecure") == "1" {
		opts = append(opts, grpc.WithInsecure())
	}
	if u.Query().Get("gzip") == "1" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}
	if u.Query().Get("tls") == "1" {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if u.Query().Get("lbrr") == "1" {
		opts = append(opts, grpc.WithBalancerName(roundrobin.Name))
	}

	println(u.Host)
	conn, err := grpc.Dial(u.Host, opts...)
	if err != nil {
		return nil, err
	}

	client := api.NewStreamsClient(conn)
	grpcConn := &grpcConnection{conn, client, context.Background()}

	if user := u.User; user != nil {
		password, _ := user.Password()
		token, err := grpcConn.Login(user.Username(), password)
		if err != nil {
			return nil, err
		}
		grpcConn.SetToken(token)
	}

	return grpcConn, nil
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

	result, err := this.client.Append(this.ctx, &api.AppendRequest{
		CollectionId: this.collectionId,
		Stream:       stream,
		Messages:     inputs,
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
	slice, err := this.client.ReadControl(this.ctx, &api.ReadRequest{
		CollectionId: this.collectionId,
		Stream:       stream,
		From:         from,
		Count:        uint32(count),
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
	slice, err := this.client.Read(this.ctx, &api.ReadRequest{
		CollectionId: this.collectionId,
		Stream:       stream,
		From:         from,
		Count:        uint32(count),
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
	ctx, cancel := context.WithCancel(this.ctx)
	slices := make(chan Slice)

	go func() {
		defer close(slices)
		defer cancel()

		watch, err := this.client.Watch(ctx, &api.ReadRequest{CollectionId: this.collectionId, Stream: stream, From: from, Count: uint32(count)})
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
	client         api.StreamsClient
	collectionId   uint32
	collectionName string
	ctx            context.Context
}

type grpcConnection struct {
	conn   *grpc.ClientConn
	client api.StreamsClient
	ctx    context.Context
}
