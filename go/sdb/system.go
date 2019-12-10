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
	ChangePassword(username string, password string) error
	GrandUserToDatabase(username string, database string) error
}

func (this *grpcClient) ChangePassword(username string, password string) error {
	_, err := this.client.ChangePassword(this.ctx, &api.ChangePasswordRequest{
		Username: username,
		Password: password,
	})

	return err
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
