package gitprovider

import (
	"context"
	"sync"
)

// GitHubTokenFetcher retrieves a per-tenant GitHub PAT from an external source (titlis-api).
type GitHubTokenFetcher interface {
	GetGitHubToken(ctx context.Context, tenantID int64) (string, error)
}

// TokenProviderBuilder creates a GitProvider from a PAT. Injected from main to avoid
// an import cycle between the gitprovider package and gitprovider/github.
type TokenProviderBuilder func(token string) GitProvider

// PerTenantFactory resolves the right GitProvider for each tenant:
//  1. Tries to fetch a PAT for the tenant from the fetcher (cached).
//  2. Falls back to the global provider (GitHub App or memory) if no PAT is found.
type PerTenantFactory struct {
	fetcher  GitHubTokenFetcher
	builder  TokenProviderBuilder
	fallback GitProvider // GitHub App client or memory provider
	mu       sync.RWMutex
	cache    map[int64]GitProvider
}

func NewPerTenantFactory(fetcher GitHubTokenFetcher, builder TokenProviderBuilder, fallback GitProvider) *PerTenantFactory {
	return &PerTenantFactory{
		fetcher:  fetcher,
		builder:  builder,
		fallback: fallback,
		cache:    make(map[int64]GitProvider),
	}
}

func (f *PerTenantFactory) ForTenant(ctx context.Context, tenantID int64, _ string) (GitProvider, error) {
	f.mu.RLock()
	if p, ok := f.cache[tenantID]; ok {
		f.mu.RUnlock()
		return p, nil
	}
	f.mu.RUnlock()

	token, err := f.fetcher.GetGitHubToken(ctx, tenantID)
	if err == nil && token != "" {
		p := f.builder(token)
		f.mu.Lock()
		f.cache[tenantID] = p
		f.mu.Unlock()
		return p, nil
	}

	if f.fallback != nil {
		return f.fallback, nil
	}
	return nil, ErrUnsupported
}

// Invalidate clears the cached provider for a tenant, forcing a fresh token fetch
// on the next call. Call this when the tenant's GitHub token is updated.
func (f *PerTenantFactory) Invalidate(tenantID int64) {
	f.mu.Lock()
	delete(f.cache, tenantID)
	f.mu.Unlock()
}
