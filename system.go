package sdb

import "github.com/streamsdb/driver/internal/pb"

type System interface {
	EnableAcl(username string, password string) error
	CreateUser(username string, password string) error
	CreateCollection(name string) (Collection, error)
	GrandUserToCollection(username string, collection string) error
}

func (this *grpcConnection) System() System {
	return this
}

func (this *grpcConnection) EnableAcl(username string, password string) error {
	_, err := this.client.EnableAcl(this.ctx, &pb.EnableAclRequest{
		Username: username,
		Password: password,
	})
	return err
}

func (this *grpcConnection) CreateUser(username string, password string) error {
	_, err := this.client.CreateUser(this.ctx, &pb.CreateUserRequest{
		Username: username,
		Password: password,
	})
	return err
}

func (this *grpcConnection) CreateCollection(name string) (Collection, error) {
	r, err := this.client.CreateCollection(this.ctx, &pb.CreateCollectionRequest{
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
func (this *grpcConnection) GrandUserToCollection(username string, collection string) error {
	_, err := this.client.GrandUserToCollection(this.ctx, &pb.GrandUserToCollectionRequest{
		Username:   username,
		Collection: collection})
	return err
}
