package sdb

import (
	"github.com/streamsdb/driver/internal/api"
)

type System interface {
	EnableAcl(username string, password string) error
	CreateUser(username string, password string) error
	CreateDatabase(name string) (DB, error)
	GrandUserToDatabase(username string, database string) error
}

func (this *grpcConnection) System() System {
	return this
}

func (this *grpcConnection) EnableAcl(username string, password string) error {
	_, err := this.client.EnableAcl(this.ctx, &api.EnableAclRequest{
		Username: username,
		Password: password,
	})
	return err
}

func (this *grpcConnection) CreateUser(username string, password string) error {
	_, err := this.client.CreateUser(this.ctx, &api.CreateUserRequest{
		Username: username,
		Password: password,
	})
	return err
}

func (this *grpcConnection) CreateDatabase(name string) (DB, error) {
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
func (this *grpcConnection) GrandUserToDatabase(username string, database string) error {
	_, err := this.client.GrandUserToDatabase(this.ctx, &api.GrandUserToDatabaseRequest{
		Username: username,
		Database: database})
	return err
}
