package sdb

import (
	"github.com/streamsdb/driver/go/sdb/internal/api"
)

type System interface {
	// Authenticate trades the provided username and password for a token.
	Authenticate(username string, password string) (string, error)
	EnableAcl(username string, password string) error
	CreateUser(username string, password string) error
	CreateDatabase(name string) (DB, error)
	GrandUserToDatabase(username string, database string) error
	ReadGlobal(from []byte, limit int) (GlobalSlice, error)
}

func (this *grpcClient) ReadGlobal(from []byte, limit int) (GlobalSlice, error) {
	reply, err := this.client.ReadGlobal(this.ctx, &api.ReadGlobalRequest{
		Database: this.db,
		From:     from,
		Limit:    int32(limit),
	})
	if err != nil {
		return GlobalSlice{}, err
	}

	return GlobalSlice{
		From:   reply.From,
		Next:   reply.Next,
		Values: reply.Values,
	}, nil
}

func (this *grpcClient) System() System {
	return this
}

func (this *grpcClient) EnableAcl(username string, password string) error {
	_, err := this.client.EnableAcl(this.ctx, &api.EnableAclRequest{
		Username: username,
		Password: password,
	})
	return err
}

func (this *grpcClient) CreateUser(username string, password string) error {
	_, err := this.client.CreateUser(this.ctx, &api.CreateUserRequest{
		Username: username,
		Password: password,
	})
	return err
}

func (this *grpcClient) CreateDatabase(name string) (DB, error) {
	_, err := this.client.CreateDatabase(this.ctx, &api.CreateDatabaseRequest{
		Name: name,
	})

	if err != nil {
		return nil, err
	}

	return &collectionScope{
		client: this.client,
		db:     name,
		ctx:    this.ctx,
	}, nil
}
func (this *grpcClient) GrandUserToDatabase(username string, database string) error {
	_, err := this.client.GrandUserToDatabase(this.ctx, &api.GrandUserToDatabaseRequest{
		Username: username,
		Database: database})
	return err
}
