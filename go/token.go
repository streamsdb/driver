package sdb

import "context"

// A authentication token that held a claim from a successful login.
type TokenAuth string

// GetRequestMetadata gets the request metadata as a map from the TokenAuth.
func (t TokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"token": string(t)}, nil
}

func (t TokenAuth) RequireTransportSecurity() bool {
	return false
}
