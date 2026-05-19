package gitprovider

import (
	"context"
	"errors"
)

type Factory interface {
	ForTenant(ctx context.Context, tenantID int64, repoURL string) (GitProvider, error)
}

type StaticFactory struct {
	Provider GitProvider
}

func (s StaticFactory) ForTenant(_ context.Context, _ int64, _ string) (GitProvider, error) {
	if s.Provider == nil {
		return nil, errors.New("no provider configured")
	}
	return s.Provider, nil
}
