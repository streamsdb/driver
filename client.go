package sdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/streamsdb/driver/internal/api"
	"google.golang.org/grpc"

	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

// MessageInput holds the data to be appended to a stream.
type MessageInput struct {
	// The name of the message type.
	Type string
	// The headers that will be included in the record.
	Headers []byte
	// The content of the message.
	Value []byte
}

// Watch follows a remote stream. Use the Slices channel to receive
// slices containing the messages from the remote stream.
// The Slices channel will be closed if an error occurs or if the watch
// was closed.
type Watch struct {
	cancel context.CancelFunc
	Slices <-chan Slice
	err    error
}

// Err returns the reason why Slices was closed, if so.
// If Slices is not closed, Err returns nil.
// If this watch is canceled, Err returns nil.
func (this *Watch) Err() error {
	return this.err
}

// Cancel cancels this watch. The Slices channel will be closed
// and Err() will return nil.
//
// Calling Cancel on an cancelled or errored watch will do nothing.
func (this *Watch) Cancel() {
	this.cancel()
}

type DB interface {
	Append(stream string, messages ...MessageInput) (int64, error)
	Watch(stream string, from int64, count int) *Watch
	Read(stream string, from int64, count int) (Slice, error)
}

type Connection interface {
	IsTokenSet() bool
	SetToken(token string) error
	Login(username string, password string) (string, error)

	// Collection gets a handle for a given collection.
	//
	// If a collection was specified in the connection string of
	// the client, passing an empty name (""), will returns
	// a handle to that collection.
	DB(name string) DB

	// Databases lists all available databases.
	Databases() ([]string, error)
	Close() error
	System() System
}

func (this *grpcConnection) Databases() ([]string, error) {
	r, err := this.client.GetDatabases(this.ctx, &api.GetDatabasesRequest{})
	if err != nil {
		return nil, err
	}

	return r.Databases, nil
}

// IsTokenSet determines whether a token is set of this connection or not.
func (this *grpcConnection) IsTokenSet() bool {
	md, ok := metadata.FromIncomingContext(this.ctx)
	if !ok {
		return false
	}

	return len(md.Get("token")) > 0
}

// SetToken set a token for this connection that will be included in every
// subsequent request.
func (this *grpcConnection) SetToken(token string) error {
	this.ctx = metadata.AppendToOutgoingContext(this.ctx, "token", token)
	return nil
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

func (this *grpcConnection) DB(name string) DB {
	if len(name) == 0 {
		name = strings.TrimPrefix(this.col, "/")
	}

	return &collectionScope{this.client, name, this.ctx}
}

func MustOpenDefault() Connection {
	conn, err := Open("sdb://localhost:6000?insecure=1")
	if err != nil {
		panic(err)
	}

	return conn
}

// See: Open
func MustOpen(cs string) Connection {
	conn, err := Open(cs)
	if err != nil {
		panic(fmt.Sprintf("streamsdb open error: %v", err.Error()))
	}
	return conn
}

// Open opens a connection to the specified streamsdb.
//
// The format of the url is:
// sdb://[username:password@]host[:port]/[collection]?[option_name]=[option_value]
func Open(cs string) (Connection, error) {
	if !strings.HasPrefix(cs, "sdb://") {
		return nil, fmt.Errorf("invalid streamsdb connection string '%v': not starting with 'sdb://'", cs)
	}
	u, err := url.Parse(cs)
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
	grpcConn := &grpcConnection{conn, client, context.Background(), u.Path}

	if user := u.User; user != nil {
		password, _ := user.Password()
		token, err := grpcConn.Login(user.Username(), password)
		if err != nil {
			return nil, errors.Wrap(err, "login error")
		}
		grpcConn.SetToken(token)
	}

	return grpcConn, nil
}

func (this *collectionScope) Append(stream string, messages ...MessageInput) (int64, error) {
	inputs := make([]*api.MessageInput, len(messages), len(messages))

	for i, m := range messages {
		inputs[i] = &api.MessageInput{Type: m.Type, Metadata: m.Headers, Value: m.Value}
	}

	result, err := this.client.Append(this.ctx, &api.AppendRequest{
		Database: this.db,
		Stream:   stream,
		Messages: inputs,
	})

	if err != nil {
		return 0, err
	}
	return result.From, nil
}

type Message struct {
	Type      string
	Timestamp time.Time
	Header    []byte
	Value     []byte
}

type Slice struct {
	Stream   string
	From     int64
	To       int64
	Next     int64
	HasNext  bool
	Head     int64
	Messages []Message
}

func (this *collectionScope) Read(stream string, from int64, count int) (Slice, error) {
	slice, err := this.client.Read(this.ctx, &api.ReadRequest{
		Database: this.db,
		Stream:   stream,
		From:     from,
		Count:    uint32(count),
	})

	if err != nil {
		return Slice{}, err
	}

	messages := make([]Message, len(slice.Messages), len(slice.Messages))

	for i, m := range slice.Messages {
		timestamp, _ := types.TimestampFromProto(m.Timestamp)

		messages[i] = Message{
			Type:      m.Type,
			Timestamp: timestamp,
			Header:    m.Metadata,
			Value:     m.Value}
	}

	return Slice{
		Stream:   stream,
		From:     slice.From,
		To:       slice.To,
		Next:     slice.Next,
		HasNext:  slice.HasNext,
		Head:     slice.Head,
		Messages: messages,
	}, nil
}

func (this *collectionScope) Watch(stream string, from int64, count int) *Watch {
	ctx, cancel := context.WithCancel(this.ctx)
	slices := make(chan Slice)
	result := &Watch{cancel, slices, nil}

	go func() {
		defer close(slices)
		defer cancel()

		watch, err := this.client.Watch(ctx, &api.ReadRequest{Database: this.db, Stream: stream, From: from, Count: uint32(count)})
		if err != nil {
			result.err = err
			return
		}

		for {
			slice, err := watch.Recv()
			if err != nil {
				result.err = err
				return
			}

			messages := make([]Message, len(slice.Messages))
			for i, m := range slice.Messages {
				timestamp, _ := types.TimestampFromProto(m.Timestamp)

				messages[i] = Message{
					Type:      m.Type,
					Timestamp: timestamp,
					Header:    m.Metadata,
					Value:     m.Value,
				}
			}

			s := Slice{
				Stream:   stream,
				From:     slice.From,
				To:       slice.To,
				Next:     slice.Next,
				HasNext:  slice.HasNext,
				Head:     slice.Head,
				Messages: messages,
			}

			select {
			case slices <- s:
			case <-ctx.Done():
				result.err = ctx.Err()
				return
			}
		}
	}()

	return result
}

func (this *grpcConnection) Close() error {
	return this.conn.Close()
}

type collectionScope struct {
	client api.StreamsClient
	db     string
	ctx    context.Context
}

type grpcConnection struct {
	conn   *grpc.ClientConn
	client api.StreamsClient
	ctx    context.Context
	col    string
}
