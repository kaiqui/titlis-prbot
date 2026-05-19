package repo

import (
	"context"
	"sync"
	"time"

	"github.com/titlis/prbot/internal/model"
)

type MemoryMappings struct {
	mu      sync.RWMutex
	entries map[string]model.WorkloadRepoMapping
}

func NewMemoryMappings() *MemoryMappings {
	return &MemoryMappings{entries: map[string]model.WorkloadRepoMapping{}}
}

func mappingKey(tenantID int64, workloadName string) string {
	return workloadName + "|" + itoa(tenantID)
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func (m *MemoryMappings) Get(_ context.Context, tenantID int64, workloadName string) (model.WorkloadRepoMapping, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.entries[mappingKey(tenantID, workloadName)]; ok {
		return e, nil
	}
	return model.WorkloadRepoMapping{}, ErrNotFound
}

func (m *MemoryMappings) Upsert(_ context.Context, mp model.WorkloadRepoMapping) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mp.LastSyncedAt.IsZero() {
		mp.LastSyncedAt = time.Now().UTC()
	}
	m.entries[mappingKey(mp.TenantID, mp.WorkloadName)] = mp
	return nil
}

func (m *MemoryMappings) List(_ context.Context, tenantID int64) ([]model.WorkloadRepoMapping, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []model.WorkloadRepoMapping{}
	for _, e := range m.entries {
		if e.TenantID == tenantID {
			out = append(out, e)
		}
	}
	return out, nil
}

type MemoryProfiles struct {
	mu      sync.RWMutex
	entries map[string]model.GitOpsProfile
}

func NewMemoryProfiles() *MemoryProfiles {
	return &MemoryProfiles{entries: map[string]model.GitOpsProfile{}}
}

func profileKey(tenantID int64, repoURL string) string {
	return repoURL + "|" + itoa(tenantID)
}

func (m *MemoryProfiles) Get(_ context.Context, tenantID int64, repoURL string) (model.GitOpsProfile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.entries[profileKey(tenantID, repoURL)]; ok {
		return e, nil
	}
	return model.GitOpsProfile{}, ErrNotFound
}

func (m *MemoryProfiles) Upsert(_ context.Context, p model.GitOpsProfile) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now().UTC()
	}
	m.entries[profileKey(p.TenantID, p.RepoURL)] = p
	return nil
}

func (m *MemoryProfiles) List(_ context.Context, tenantID int64) ([]model.GitOpsProfile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []model.GitOpsProfile{}
	for _, e := range m.entries {
		if e.TenantID == tenantID {
			out = append(out, e)
		}
	}
	return out, nil
}

type MemoryPolicies struct {
	mu      sync.RWMutex
	entries map[string]model.AutoRemediationPolicy
}

func NewMemoryPolicies() *MemoryPolicies {
	return &MemoryPolicies{entries: map[string]model.AutoRemediationPolicy{}}
}

func policyKey(tenantID int64, ruleID, env string) string {
	return ruleID + "|" + env + "|" + itoa(tenantID)
}

func (m *MemoryPolicies) Get(_ context.Context, tenantID int64, ruleID, environment string) (model.AutoRemediationPolicy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.entries[policyKey(tenantID, ruleID, environment)]; ok {
		return e, nil
	}
	if e, ok := m.entries[policyKey(tenantID, ruleID, "")]; ok {
		return e, nil
	}
	// Fall back to global policy when no rule-specific entry exists.
	if ruleID != model.GlobalPolicyRuleID {
		if e, ok := m.entries[policyKey(tenantID, model.GlobalPolicyRuleID, environment)]; ok {
			return e, nil
		}
		if e, ok := m.entries[policyKey(tenantID, model.GlobalPolicyRuleID, "")]; ok {
			return e, nil
		}
	}
	return model.AutoRemediationPolicy{}, ErrNotFound
}

func (m *MemoryPolicies) Upsert(_ context.Context, p model.AutoRemediationPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = time.Now().UTC()
	}
	m.entries[policyKey(p.TenantID, p.RuleID, p.Environment)] = p
	return nil
}

func (m *MemoryPolicies) ListEligibleTenants(_ context.Context, ruleID string) ([]int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seen := map[int64]bool{}
	out := []int64{}
	for _, p := range m.entries {
		if (p.RuleID != ruleID && p.RuleID != model.GlobalPolicyRuleID) || p.Mode == model.PolicyDisabled {
			continue
		}
		if !seen[p.TenantID] {
			seen[p.TenantID] = true
			out = append(out, p.TenantID)
		}
	}
	return out, nil
}
