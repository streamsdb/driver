package sdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/streamsdb/driver/go/sdb/internal/api"
	"google.golang.org/grpc"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MessageInput holds the data to be appended to a stream.
type MessageInput struct {
	// The id of the message (not required).
	// If set, this is used for idempotentency. When appending
	// a message streamsdb checks if there is already a message
	// present with this Id. If so, the append is skipped.
	// This makes an append request with messages with id safe for
	// retrying.
	ID string
	// The name of the message type.
	Type string
	// The headers that will be included in the record.
	Header []byte
	// The content of the message.
	Value []byte
}

type StreamPage struct {
	Total int
	Names []string

	Filter string
	Limit  int

	First string
	Last  string

	HasNext bool
}

type DB interface {
	EnsureExists() error
	AppendStream(stream string, expectedVersion int64, messages ...MessageInput) (int64, error)
	DeleteMessage(stream string, at int64) error
	DeleteStream(stream string) error
	OpenStreamForward(stream string, options StreamReadOptions) (MessageIterator, error)
	ReadStreamForward(stream string, from int64, limit int) (Slice, error)
	ReadStreamBackward(stream string, from int64, limit int) (Slice, error)
	ReadGlobal(from []byte, limit int) (GlobalSlice, error)
	ListStreamsForward(filter string, after string, limit int) (StreamPage, error)
	ListStreamsBackward(filter string, after string, limit int) (StreamPage, error)
}

func (this *grpcClient) Clone(ctx context.Context) Client {
	return &grpcClient{
		conn:   this.conn,
		client: this.client,
		db:     this.db,
		ctx:    ctx,
	}
}

// ContextWithToken creates a new context with the token attached.
func ContextWithToken(ctx context.Context, token string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return metadata.AppendToOutgoingContext(ctx, "token", token)
	}

	md.Set("token", token)
	return metadata.NewOutgoingContext(ctx, md)
}

type Client interface {
	// DB gets a handle for a given database.
	//
	// If a database was specified in the connection string of
	// the client, passing an empty name (""), will returns
	// a handle to that collection.
	DB(name string) DB

	// Databases lists all available databases.
	Databases() ([]string, error)

	Close() error

	System() System

	// Clone clones the client with the provides context.
	Clone(ctx context.Context) Client

	Ping() error
}

func (this *grpcClient) Ping() error {
	_, err := this.client.Ping(this.ctx, &api.PingRequest{})

	return err
}

func (this *grpcClient) Databases() ([]string, error) {
	r, err := this.client.GetDatabases(this.ctx, &api.GetDatabasesRequest{})
	if err != nil {
		return nil, err
	}

	return r.Databases, nil
}

// IsTokenSet determines whether a token is set of this connection or not.
func (this *grpcClient) IsTokenSet() bool {
	md, ok := metadata.FromIncomingContext(this.ctx)
	if !ok {
		return false
	}

	return len(md.Get("token")) > 0
}

func (this *grpcClient) Authenticate(username string, password string) (string, error) {
	r, err := this.client.Login(this.ctx, &api.LoginRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return "", err
	}

	return r.Token, nil
}

func (this *grpcClient) DB(name string) DB {
	if len(name) == 0 {
		name = strings.TrimPrefix(this.db, "/")
	}

	return &collectionScope{this.client, name, this.ctx}
}

func MustOpenDefault() Client {
	conn, err := OpenDefault()
	if err != nil {
		panic(err)
	}

	return conn
}

func OpenDefault() (Client, error) {
	connString := "sdb://localhost:6000/default?insecure=1&block=1"
	if sdbHost := os.Getenv("SDB_HOST"); len(sdbHost) > 0 {
		connString = sdbHost
	}

	return Open(connString)
}

// See: Open
func MustOpen(connectionString string) Client {
	conn, err := Open(connectionString)
	if err != nil {
		panic(fmt.Sprintf("streamsdb open error: %v", err.Error()))
	}
	return conn
}

// Open opens a connection to the specified streamsdb.
//
// The format of the url is:
// sdb://[username:password@]host[:port]/[database]?[option_name]=[option_value]
//
// See "connection string documentation" for more information: https://streamsdb.io/docs/connection-string
func Open(connectionString string) (Client, error) {
	if !strings.HasPrefix(connectionString, "sdb://") {
		return nil, fmt.Errorf("invalid streamsdb connection string '%v': not starting with 'sdb://'", connectionString)
	}
	u, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}

	opts := make([]grpc.DialOption, 0)

	if u.Query().Get("block") == "1" {
		opts = append(opts, grpc.WithBlock())
	}

	if dt := u.Query().Get("dt"); len(dt) > 0 {
		d, err := time.ParseDuration(dt)
		if err != nil {
			return nil, errors.Wrap(err, "invalid value for connection string option 'dt'")
		}
		opts = append(opts, grpc.WithTimeout(d))
	}

	if u.Query().Get("insecure") == "1" {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if u.Query().Get("gzip") == "1" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	conn, err := grpc.Dial(u.Host, opts...)
	if err != nil {
		return nil, err
	}

	client := api.NewStreamsClient(conn)
	grpcConn := &grpcClient{conn, client, context.Background(), u.Path}

	if user := u.User; user != nil {
		password, _ := user.Password()
		token, err := grpcConn.Authenticate(user.Username(), password)
		if err != nil {
			return nil, errors.Wrap(err, "login error")
		}
		grpcConn.ctx = ContextWithToken(grpcConn.ctx, token)
	}

	return grpcConn, nil
}

func (this *collectionScope) DeleteStream(stream string) error {
	_, err := this.client.DeleteStream(this.ctx, &api.DeleteStreamRequest{
		Database: this.db,
		Stream:   stream,
	})
	return err
}

func (this *collectionScope) DeleteMessage(stream string, at int64) error {
	_, err := this.client.DeleteMessage(this.ctx, &api.DeleteMessageRequest{
		Database: this.db,
		Stream:   stream,
		Position: at,
	})
	return err
}

func (this *collectionScope) EnsureExists() error {
	_, err := this.client.CreateDatabase(this.ctx, &api.CreateDatabaseRequest{
		Name: this.db,
	})

	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			return nil
		}
		return err
	}

	return nil
}

func (this *collectionScope) AppendStream(stream string, expectedVersion int64, messages ...MessageInput) (int64, error) {
	inputs := make([]*api.MessageInput, len(messages))

	for i, m := range messages {
		inputs[i] = &api.MessageInput{
			Id: m.ID, Type: m.Type, Header: m.Header, Value: m.Value,
		}
	}

	result, err := this.client.AppendStream(this.ctx, &api.AppendStreamRequest{
		Database:        this.db,
		Stream:          stream,
		Messages:        inputs,
		ExpectedVersion: expectedVersion,
	})

	if err != nil {
		return 0, err
	}
	return result.Position, nil
}

type Message struct {
	ID        string
	Position  int64
	Type      string
	Timestamp time.Time
	Header    []byte
	Value     []byte
}

type Slice struct {
	Stream   string
	From     int64
	Next     int64
	HasNext  bool
	Head     int64
	Messages []Message
	Reverse  bool
}

type GlobalSlice struct {
	Database string
	From     []byte
	Next     []byte
	Messages []Message
}

func (this *collectionScope) ReadGlobal(cursor []byte, limit int) (GlobalSlice, error) {
	reply, err := this.client.ReadGlobal(this.ctx, &api.ReadGlobalRequest{
		Database: this.db,
		Cursor:   cursor,
		Limit:    int32(limit),
	})
	if err != nil {
		return GlobalSlice{}, err
	}

	messages := make([]Message, len(reply.Messages))

	for i, m := range reply.Messages {
		timestamp, _ := types.TimestampFromProto(m.Timestamp)

		messages[i] = Message{
			Position:  m.Position,
			Type:      m.Type,
			Timestamp: timestamp,
			Header:    m.Header,
			Value:     m.Value}
	}

	return GlobalSlice{
		From:     reply.Cursor,
		Next:     reply.Next,
		Messages: messages,
	}, nil
}

func (this *collectionScope) ListStreamsBackward(filter string, before string, limit int) (StreamPage, error) {
	result, err := this.client.GetStreams(this.ctx, &api.GetStreamsRequest{
		Database: this.db,
		Cursor:   before,
		Reverse:  true,
		Filter:   filter,
		Limit:    int32(limit),
	})
	if err != nil {
		return StreamPage{}, err
	}

	first := ""
	last := ""

	if len(result.Result) > 0 {
		first = result.Result[0]
		last = result.Result[len(result.Result)-1]
	}

	return StreamPage{
		Total:   int(result.Total),
		Names:   result.Result,
		Filter:  result.Filter,
		Limit:   int(result.Limit),
		HasNext: result.HasNext,
		First:   first,
		Last:    last,
	}, nil
}

func (this *collectionScope) ListStreamsForward(filter string, after string, limit int) (StreamPage, error) {
	result, err := this.client.GetStreams(this.ctx, &api.GetStreamsRequest{
		Database: this.db,
		Cursor:   after,
		Reverse:  false,
		Filter:   filter,
		Limit:    int32(limit),
	})
	if err != nil {
		return StreamPage{}, err
	}

	return StreamPage{
		Total:   int(result.Total),
		Names:   result.Result,
		Filter:  result.Filter,
		Limit:   int(result.Limit),
		HasNext: result.HasNext}, nil
}

type StreamReadOptions struct {
	From     int64
	Reverse  bool
	KeepOpen bool
}

func (this *collectionScope) OpenStreamForward(stream string, options StreamReadOptions) (MessageIterator, error) {
	ctx, cancel := context.WithCancel(this.ctx)

	subscription, err := this.client.IterateStream(ctx, &api.ReadStreamRequest{
		Database: this.db,
		Stream:   stream,
		From:     options.From,
	})

	if err != nil {
		cancel()
		return nil, err
	}

	return &messageIterator{cancel: cancel, subscription: subscription}, nil
}

func (this *collectionScope) ReadStreamForward(stream string, from int64, limit int) (Slice, error) {
	return this.read(stream, from, false, limit)
}
func (this *collectionScope) ReadStreamBackward(stream string, from int64, limit int) (Slice, error) {
	return this.read(stream, from, true, limit)
}

func (this *collectionScope) read(stream string, from int64, reverse bool, limit int) (Slice, error) {
	return Slice{}, errors.New("not implemented")

	/*
		slice, err := this.client.ReadStream(this.ctx, &api.ReadStreamRequest{
			Database: this.db,
			Stream:   stream,
			From:     from,
			Limit:    uint32(limit),
			Reverse:  reverse,
		})

		if err != nil {
			return Slice{}, err
		}

		messages := make([]Message, len(slice.Messages), len(slice.Messages))

		for i, m := range slice.Messages {
			timestamp, _ := types.TimestampFromProto(m.Timestamp)

			messages[i] = Message{
				Position:  m.Position,
				Type:      m.Type,
				Timestamp: timestamp,
				Header:    m.Header,
				Value:     m.Value}
		}

		return Slice{
			Stream:   stream,
			From:     slice.From,
			Next:     slice.Next,
			HasNext:  slice.HasNext,
			Head:     slice.Head,
			Reverse:  slice.Reverse,
			Messages: messages,
		}, nil
	*/
}

func (this *collectionScope) SubscribeStream(stream string, from int64, count int) SliceIterator {
	return nil
	/*ctx, cancel := context.WithCancel(this.ctx)
	slices := make(chan Slice)
	result := StreamSubscription{cancel, slices, nil}

	go func() {
		defer close(slices)
		defer cancel()

		subscription, err := this.client.SubscribeStream(ctx, &api.SubscribeStreamRequest{Database: this.db, Stream: stream, From: from, Count: uint32(count)})

		iterator := &sliceIterator{
		read: func() {

		}
	}
		if err != nil {
			result.err = err
			return
		}

		for {
			slice, err := subscription.Recv()
			if err != nil {
				result.err = err
				return
			}

			messages := make([]Message, len(slice.Messages))
			for i, m := range slice.Messages {
				timestamp, _ := types.TimestampFromProto(m.Timestamp)

				messages[i] = Message{
					Position:  m.Position,
					Type:      m.Type,
					Timestamp: timestamp,
					Header:    m.Header,
					Value:     m.Value,
				}
			}

			s := Slice{
				Stream:   stream,
				From:     slice.From,
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
	*/
}

func (this *grpcClient) Close() error {
	return this.conn.Close()
}

type collectionScope struct {
	client api.StreamsClient
	db     string
	ctx    context.Context
}

type grpcClient struct {
	conn   *grpc.ClientConn
	client api.StreamsClient
	ctx    context.Context
	db     string
}
