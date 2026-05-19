package repo

import (
	"context"
	"errors"

	"github.com/titlis/prbot/internal/model"
)

var ErrNotFound = errors.New("not found")

type MappingsRepo interface {
	Get(ctx context.Context, tenantID int64, workloadName string) (model.WorkloadRepoMapping, error)
	Upsert(ctx context.Context, m model.WorkloadRepoMapping) error
	List(ctx context.Context, tenantID int64) ([]model.WorkloadRepoMapping, error)
}

type GitOpsProfilesRepo interface {
	Get(ctx context.Context, tenantID int64, repoURL string) (model.GitOpsProfile, error)
	Upsert(ctx context.Context, p model.GitOpsProfile) error
	List(ctx context.Context, tenantID int64) ([]model.GitOpsProfile, error)
}

type PoliciesRepo interface {
	Get(ctx context.Context, tenantID int64, ruleID, environment string) (model.AutoRemediationPolicy, error)
	Upsert(ctx context.Context, p model.AutoRemediationPolicy) error
	ListEligibleTenants(ctx context.Context, ruleID string) ([]int64, error)
}
