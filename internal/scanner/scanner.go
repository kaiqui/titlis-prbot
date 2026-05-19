package scanner

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/titlis/prbot/internal/gitprovider"
	"github.com/titlis/prbot/internal/model"
	"github.com/titlis/prbot/internal/repo"
)

const ServiceDefinitionPath = ".titlis/service.yaml"

type Scanner struct {
	mu       sync.Mutex
	factory  gitprovider.Factory
	mappings repo.MappingsRepo
	now      func() time.Time
	tenants  []int64
	repos    map[int64][]string
}

func NewScanner(factory gitprovider.Factory, mappings repo.MappingsRepo) *Scanner {
	return &Scanner{factory: factory, mappings: mappings, now: time.Now, repos: map[int64][]string{}}
}

func (s *Scanner) RegisterTenantRepos(tenantID int64, repos []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants = appendUnique(s.tenants, tenantID)
	s.repos[tenantID] = repos
}

func appendUnique(xs []int64, v int64) []int64 {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}

func (s *Scanner) RunOnce(ctx context.Context) (Summary, error) {
	s.mu.Lock()
	tenants := append([]int64(nil), s.tenants...)
	repos := make(map[int64][]string, len(s.repos))
	for k, v := range s.repos {
		repos[k] = append([]string(nil), v...)
	}
	s.mu.Unlock()

	sum := Summary{}
	for _, tid := range tenants {
		for _, repoURL := range repos[tid] {
			out, err := s.scanRepo(ctx, tid, repoURL)
			if err != nil {
				sum.Errors = append(sum.Errors, ScanError{TenantID: tid, RepoURL: repoURL, Err: err.Error()})
				continue
			}
			sum.Found += out
		}
		sum.RepoCount += len(repos[tid])
	}
	return sum, nil
}

func (s *Scanner) scanRepo(ctx context.Context, tenantID int64, repoURL string) (int, error) {
	gp, err := s.factory.ForTenant(ctx, tenantID, repoURL)
	if err != nil {
		return 0, err
	}
	file, err := gp.FetchFile(ctx, repoURL, "main", ServiceDefinitionPath)
	if err != nil {
		if errors.Is(err, gitprovider.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	def, err := ParseServiceDefinition(file.Content)
	if err != nil {
		return 0, err
	}
	mapping := model.WorkloadRepoMapping{
		TenantID:     tenantID,
		WorkloadName: def.Metadata.Name,
		RepoURL:      repoURL,
		Definition:   def,
		LastSeenSHA:  file.SHA,
		LastSyncedAt: s.now().UTC(),
	}
	if err := s.mappings.Upsert(ctx, mapping); err != nil {
		return 0, err
	}
	return 1, nil
}

type Summary struct {
	RepoCount int
	Found     int
	Errors    []ScanError
}

type ScanError struct {
	TenantID int64  `json:"tenant_id"`
	RepoURL  string `json:"repo_url"`
	Err      string `json:"error"`
}
