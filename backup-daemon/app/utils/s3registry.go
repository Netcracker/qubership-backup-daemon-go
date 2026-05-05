package utils

import "fmt"

// S3AliasRegistry provides access to pre-built S3 clients keyed by alias name.
type S3AliasRegistry interface {
	Get(aliasName string) (S3ClientRepository, error)
	Has(aliasName string) bool
}

type s3AliasRegistry struct {
	clients map[string]S3ClientRepository
}

func NewS3AliasRegistry(clients map[string]S3ClientRepository) S3AliasRegistry {
	if clients == nil {
		clients = make(map[string]S3ClientRepository)
	}
	return &s3AliasRegistry{clients: clients}
}

func (r *s3AliasRegistry) Get(aliasName string) (S3ClientRepository, error) {
	c, ok := r.clients[aliasName]
	if !ok {
		return nil, fmt.Errorf("s3 alias %q not found", aliasName)
	}
	return c, nil
}

func (r *s3AliasRegistry) Has(aliasName string) bool {
	_, ok := r.clients[aliasName]
	return ok
}
