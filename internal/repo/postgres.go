package repo

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/titlis/prbot/internal/model"
)

// PGMappings persists workload→repo mappings in titlis_prbot.workload_repo_mappings.
type PGMappings struct {
	db *pgxpool.Pool
}

func NewPGMappings(db *pgxpool.Pool) *PGMappings { return &PGMappings{db: db} }

func (r *PGMappings) Get(ctx context.Context, tenantID int64, workloadName string) (model.WorkloadRepoMapping, error) {
	row := r.db.QueryRow(ctx, `
		SELECT tenant_id, workload_name, repo_url, service_definition, last_seen_sha, last_synced_at
		FROM titlis_prbot.workload_repo_mappings
		WHERE tenant_id = $1 AND workload_name = $2`,
		tenantID, workloadName,
	)
	return scanMapping(row)
}

func (r *PGMappings) Upsert(ctx context.Context, m model.WorkloadRepoMapping) error {
	defJSON, err := json.Marshal(m.Definition)
	if err != nil {
		return err
	}
	if m.LastSyncedAt.IsZero() {
		m.LastSyncedAt = time.Now().UTC()
	}
	_, err = r.db.Exec(ctx, `
		INSERT INTO titlis_prbot.workload_repo_mappings
			(tenant_id, workload_name, repo_url, service_definition, last_seen_sha, last_synced_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, workload_name) DO UPDATE SET
			repo_url           = EXCLUDED.repo_url,
			service_definition = EXCLUDED.service_definition,
			last_seen_sha      = EXCLUDED.last_seen_sha,
			last_synced_at     = EXCLUDED.last_synced_at`,
		m.TenantID, m.WorkloadName, m.RepoURL, defJSON, m.LastSeenSHA, m.LastSyncedAt,
	)
	return err
}

func (r *PGMappings) List(ctx context.Context, tenantID int64) ([]model.WorkloadRepoMapping, error) {
	rows, err := r.db.Query(ctx, `
		SELECT tenant_id, workload_name, repo_url, service_definition, last_seen_sha, last_synced_at
		FROM titlis_prbot.workload_repo_mappings
		WHERE tenant_id = $1
		ORDER BY workload_name`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.WorkloadRepoMapping
	for rows.Next() {
		m, err := scanMapping(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func scanMapping(row pgx.Row) (model.WorkloadRepoMapping, error) {
	var m model.WorkloadRepoMapping
	var defRaw []byte
	err := row.Scan(&m.TenantID, &m.WorkloadName, &m.RepoURL, &defRaw, &m.LastSeenSHA, &m.LastSyncedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.WorkloadRepoMapping{}, ErrNotFound
	}
	if err != nil {
		return model.WorkloadRepoMapping{}, err
	}
	return m, json.Unmarshal(defRaw, &m.Definition)
}

// PGProfiles persists GitOps profiles in titlis_prbot.gitops_profiles.
type PGProfiles struct {
	db *pgxpool.Pool
}

func NewPGProfiles(db *pgxpool.Pool) *PGProfiles { return &PGProfiles{db: db} }

func (r *PGProfiles) Get(ctx context.Context, tenantID int64, repoURL string) (model.GitOpsProfile, error) {
	row := r.db.QueryRow(ctx, `
		SELECT tenant_id, repo_url, layout, base_branch, env_path_template,
		       pipeline_watcher, confirmed_by, confirmed_at, created_at
		FROM titlis_prbot.gitops_profiles
		WHERE tenant_id = $1 AND repo_url = $2`,
		tenantID, repoURL,
	)
	return scanProfile(row)
}

func (r *PGProfiles) Upsert(ctx context.Context, p model.GitOpsProfile) error {
	templateJSON, err := json.Marshal(p.EnvPathTemplate)
	if err != nil {
		return err
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now().UTC()
	}
	_, err = r.db.Exec(ctx, `
		INSERT INTO titlis_prbot.gitops_profiles
			(tenant_id, repo_url, layout, base_branch, env_path_template,
			 pipeline_watcher, confirmed_by, confirmed_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_id, repo_url) DO UPDATE SET
			layout             = EXCLUDED.layout,
			base_branch        = EXCLUDED.base_branch,
			env_path_template  = EXCLUDED.env_path_template,
			pipeline_watcher   = EXCLUDED.pipeline_watcher,
			confirmed_by       = EXCLUDED.confirmed_by,
			confirmed_at       = EXCLUDED.confirmed_at`,
		p.TenantID, p.RepoURL, p.Layout, p.BaseBranch, templateJSON,
		nullableString(p.PipelineWatcher), nullableString(p.ConfirmedBy),
		p.ConfirmedAt, p.CreatedAt,
	)
	return err
}

func (r *PGProfiles) List(ctx context.Context, tenantID int64) ([]model.GitOpsProfile, error) {
	rows, err := r.db.Query(ctx, `
		SELECT tenant_id, repo_url, layout, base_branch, env_path_template,
		       pipeline_watcher, confirmed_by, confirmed_at, created_at
		FROM titlis_prbot.gitops_profiles
		WHERE tenant_id = $1
		ORDER BY repo_url`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.GitOpsProfile
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func scanProfile(row pgx.Row) (model.GitOpsProfile, error) {
	var p model.GitOpsProfile
	var templateRaw []byte
	var pipelineWatcher, confirmedBy *string
	err := row.Scan(
		&p.TenantID, &p.RepoURL, &p.Layout, &p.BaseBranch, &templateRaw,
		&pipelineWatcher, &confirmedBy, &p.ConfirmedAt, &p.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.GitOpsProfile{}, ErrNotFound
	}
	if err != nil {
		return model.GitOpsProfile{}, err
	}
	if pipelineWatcher != nil {
		p.PipelineWatcher = *pipelineWatcher
	}
	if confirmedBy != nil {
		p.ConfirmedBy = *confirmedBy
	}
	return p, json.Unmarshal(templateRaw, &p.EnvPathTemplate)
}

// PGPolicies persists auto-remediation policies in titlis_prbot.auto_remediation_policies.
type PGPolicies struct {
	db *pgxpool.Pool
}

func NewPGPolicies(db *pgxpool.Pool) *PGPolicies { return &PGPolicies{db: db} }

func (r *PGPolicies) Get(ctx context.Context, tenantID int64, ruleID, environment string) (model.AutoRemediationPolicy, error) {
	// Priority: exact rule+env > exact rule (no env) > global(*)+env > global(*) no env.
	row := r.db.QueryRow(ctx, `
		SELECT tenant_id, rule_id, COALESCE(environment,''), mode, cascade_up_to,
		       COALESCE(auto_merge_max_delta_pct,20), require_pr_checks_green,
		       max_prs_per_day, updated_at
		FROM titlis_prbot.auto_remediation_policies
		WHERE tenant_id = $1 AND (rule_id = $2 OR rule_id = '*')
		  AND (environment = $3 OR environment IS NULL)
		ORDER BY CASE WHEN rule_id = $2 THEN 0 ELSE 1 END, environment NULLS LAST
		LIMIT 1`,
		tenantID, ruleID, environment,
	)
	return scanPolicy(row)
}

func (r *PGPolicies) Upsert(ctx context.Context, p model.AutoRemediationPolicy) error {
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = time.Now().UTC()
	}
	var envVal *string
	if p.Environment != "" {
		envVal = &p.Environment
	}
	_, err := r.db.Exec(ctx, `
		INSERT INTO titlis_prbot.auto_remediation_policies
			(tenant_id, rule_id, environment, mode, cascade_up_to,
			 auto_merge_max_delta_pct, require_pr_checks_green, max_prs_per_day, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_id, rule_id, COALESCE(environment,'')) DO UPDATE SET
			mode                     = EXCLUDED.mode,
			cascade_up_to            = EXCLUDED.cascade_up_to,
			auto_merge_max_delta_pct = EXCLUDED.auto_merge_max_delta_pct,
			require_pr_checks_green  = EXCLUDED.require_pr_checks_green,
			max_prs_per_day          = EXCLUDED.max_prs_per_day,
			updated_at               = EXCLUDED.updated_at`,
		p.TenantID, p.RuleID, envVal, string(p.Mode), string(p.CascadeUpTo),
		p.AutoMergeMaxDeltaPct, p.RequirePRChecksGreen, p.MaxPRsPerDay, p.UpdatedAt,
	)
	return err
}

func (r *PGPolicies) ListEligibleTenants(ctx context.Context, ruleID string) ([]int64, error) {
	rows, err := r.db.Query(ctx, `
		SELECT DISTINCT tenant_id
		FROM titlis_prbot.auto_remediation_policies
		WHERE (rule_id = $1 OR rule_id = '*') AND mode != 'disabled'
		ORDER BY tenant_id`,
		ruleID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func scanPolicy(row pgx.Row) (model.AutoRemediationPolicy, error) {
	var p model.AutoRemediationPolicy
	var mode, cascade string
	err := row.Scan(
		&p.TenantID, &p.RuleID, &p.Environment, &mode, &cascade,
		&p.AutoMergeMaxDeltaPct, &p.RequirePRChecksGreen, &p.MaxPRsPerDay, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.AutoRemediationPolicy{}, ErrNotFound
	}
	if err != nil {
		return model.AutoRemediationPolicy{}, err
	}
	p.Mode = model.PolicyMode(mode)
	p.CascadeUpTo = model.CascadeUpTo(cascade)
	return p, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
