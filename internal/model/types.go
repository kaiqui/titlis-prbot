package model

import (
	"encoding/json"
	"time"

	"gopkg.in/yaml.v3"
)

type Environment string

const (
	EnvDev     Environment = "development"
	EnvStaging Environment = "staging"
	EnvProd    Environment = "production"
)

type EnvShort string

const (
	EnvShortDev EnvShort = "dev"
	EnvShortHml EnvShort = "hml"
	EnvShortPrd EnvShort = "prd"
)

func EnvShortOrder() []EnvShort { return []EnvShort{EnvShortDev, EnvShortHml, EnvShortPrd} }

type PolicyMode string

const (
	PolicyDisabled        PolicyMode = "disabled"
	PolicyDiscoveryOnly   PolicyMode = "discovery_only"
	PolicyOpenPR          PolicyMode = "open_pr"
	PolicyAutoMerge       PolicyMode = "auto_merge"
)

// GlobalPolicyRuleID is the sentinel used when the policy should apply to all rules.
// PoliciesRepo.Get falls back to this when no rule-specific policy exists.
const GlobalPolicyRuleID = "*"

// ManagedRuleIDs lists the schedule sentinels the prbot manages.
// "manifest" maps to DiscoverComprehensiveWorkflow (all remediable rules at once).
var ManagedRuleIDs = []string{"manifest"}

// ManifestRemediableRuleIDs lists all scorecard rules that can be fixed via
// a deployment manifest patch. SEC-001 (image tag) is intentionally excluded.
var ManifestRemediableRuleIDs = []string{
	"SEC-002", "SEC-003", "SEC-004",
	"RES-001", "RES-002", "RES-003", "RES-004", "RES-005", "RES-006",
	"RES-007", "RES-008", "RES-009", "RES-010", "RES-011", "RES-012",
	"RES-013", "RES-014", "RES-016", "RES-017", "RES-018", "RES-019",
	"PERF-001", "PERF-002", "PERF-003", "PERF-004",
	"OPS-001",
}

type TriggerSource string

const (
	TriggerManual   TriggerSource = "manual"
	TriggerSchedule TriggerSource = "schedule"
	TriggerReactive TriggerSource = "reactive"
	TriggerAIAgent  TriggerSource = "ai_agent"
)

type CascadeUpTo string

const (
	CascadeDev CascadeUpTo = "dev"
	CascadeHml CascadeUpTo = "hml"
	CascadePrd CascadeUpTo = "prd"
)

type HpaRecommendation struct {
	WorkloadUID  string    `json:"workload_uid"`
	MinReplicas  int       `json:"min_replicas"`
	MaxReplicas  int       `json:"max_replicas"`
	TargetCPUPct int       `json:"target_cpu_pct"`
	TargetMemPct int       `json:"target_mem_pct,omitempty"`
	Source       string    `json:"source"`
	Confidence   float64   `json:"confidence"`
	WindowDays   int       `json:"window_days,omitempty"`
	ComputedAt   time.Time `json:"computed_at"`
	Notes        string    `json:"notes,omitempty"`
}

type ServiceDefinition struct {
	APIVersion string                 `yaml:"apiVersion" json:"apiVersion"`
	Kind       string                 `yaml:"kind" json:"kind"`
	Metadata   ServiceDefinitionMeta  `yaml:"metadata" json:"metadata"`
	Spec       ServiceDefinitionSpec  `yaml:"spec" json:"spec"`
}

type ServiceDefinitionMeta struct {
	Name          string                 `yaml:"name" json:"name"`
	WorkloadMatch ServiceDefinitionMatch `yaml:"workload_match,omitempty" json:"workload_match,omitempty"`
}

type ServiceDefinitionMatch struct {
	Namespaces  []string `yaml:"namespaces,omitempty" json:"namespaces,omitempty"`
	NamePattern string   `yaml:"name_pattern,omitempty" json:"name_pattern,omitempty"`
}

type ServiceDefinitionSpec struct {
	Owner       ServiceDefinitionOwner       `yaml:"owner,omitempty" json:"owner,omitempty"`
	GitOps      ServiceDefinitionGitOps      `yaml:"gitops" json:"gitops"`
	Remediation ServiceDefinitionRemediation `yaml:"remediation,omitempty" json:"remediation,omitempty"`
}

type ServiceDefinitionOwner struct {
	Team     string                  `yaml:"team,omitempty" json:"team,omitempty"`
	Contacts []ServiceDefinitionContact `yaml:"contacts,omitempty" json:"contacts,omitempty"`
}

type ServiceDefinitionContact struct {
	Type  string `yaml:"type" json:"type"`
	Value string `yaml:"value" json:"value"`
}

// EnvPathSpec holds the manifest path and the base branch for a single environment.
// YAML accepts both the legacy scalar form ("k8s/dev/deploy.yaml") and the new object form.
type EnvPathSpec struct {
	Path       string `yaml:"path" json:"path"`
	BaseBranch string `yaml:"base_branch,omitempty" json:"base_branch,omitempty"`
}

// UnmarshalYAML accepts both the legacy scalar ("k8s/dev/deploy.yaml")
// and the new object ({path: ..., base_branch: ...}) forms.
func (e *EnvPathSpec) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		e.Path = value.Value
		return nil
	}
	type plain EnvPathSpec
	return value.Decode((*plain)(e))
}

// UnmarshalJSON accepts both the legacy string form and the new object form
// so that existing database rows written before this change can still be read.
func (e *EnvPathSpec) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		e.Path = s
		return nil
	}
	type plain EnvPathSpec
	return json.Unmarshal(data, (*plain)(e))
}

type ServiceDefinitionGitOps struct {
	Layout          string                 `yaml:"layout" json:"layout"`
	BaseBranch      string                 `yaml:"base_branch,omitempty" json:"base_branch,omitempty"`
	Paths           map[string]EnvPathSpec `yaml:"paths" json:"paths"`
	PipelineWatcher string                 `yaml:"pipeline_watcher,omitempty" json:"pipeline_watcher,omitempty"`
}

type ServiceDefinitionRemediation struct {
	Enabled   bool                   `yaml:"enabled" json:"enabled"`
	Overrides map[string]interface{} `yaml:"overrides,omitempty" json:"overrides,omitempty"`
}

type WorkloadRepoMapping struct {
	TenantID     int64             `json:"tenant_id"`
	WorkloadName string            `json:"workload_name"`
	RepoURL      string            `json:"repo_url"`
	Definition   ServiceDefinition `json:"definition"`
	LastSeenSHA  string            `json:"last_seen_sha"`
	LastSyncedAt time.Time         `json:"last_synced_at"`
}

type GitOpsProfile struct {
	TenantID         int64             `json:"tenant_id"`
	RepoURL          string            `json:"repo_url"`
	Layout           string            `json:"layout"`
	BaseBranch       string            `json:"base_branch"`
	EnvPathTemplate  map[string]string `json:"env_path_template"`
	PipelineWatcher  string            `json:"pipeline_watcher,omitempty"`
	ConfirmedBy      string            `json:"confirmed_by,omitempty"`
	ConfirmedAt      *time.Time        `json:"confirmed_at,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
}

type AutoRemediationPolicy struct {
	TenantID              int64      `json:"tenant_id"`
	RuleID                string     `json:"rule_id"`
	Environment           string     `json:"environment,omitempty"`
	Mode                  PolicyMode `json:"mode"`
	CascadeUpTo           CascadeUpTo `json:"cascade_up_to"`
	AutoMergeMaxDeltaPct  int        `json:"auto_merge_max_delta_pct,omitempty"`
	RequirePRChecksGreen  bool       `json:"require_pr_checks_green"`
	MaxPRsPerDay          int        `json:"max_prs_per_day"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

// WorkloadFinding represents a single failing rule for a workload.
type WorkloadFinding struct {
	RuleID      string `json:"rule_id"`
	Severity    string `json:"severity"`
}

// ManifestPatchRequest is sent to titlis-api which proxies it to titlis-ai.
type ManifestPatchRequest struct {
	TenantID          int64              `json:"tenant_id"`
	Manifest          string             `json:"manifest"`
	Findings          []WorkloadFinding  `json:"findings"`
	HpaRecommendation *HpaRecommendation `json:"hpa_recommendation,omitempty"`
	WorkloadName      string             `json:"workload_name"`
	Namespace         string             `json:"namespace"`
	ClusterName       string             `json:"cluster_name"`
	Environment       string             `json:"environment"`
	Criticality       string             `json:"criticality"`
}

// ManifestPatchResponse is returned by titlis-ai after generating the corrected manifest.
type ManifestPatchResponse struct {
	CorrectedManifest string       `json:"corrected_manifest"`
	Applied           []AppliedFix `json:"applied"`
	Skipped           []SkippedFix `json:"skipped"`
}

type AppliedFix struct {
	RuleID  string `json:"rule_id"`
	Summary string `json:"summary"`
}

type SkippedFix struct {
	RuleID string `json:"rule_id"`
	Reason string `json:"reason"`
}

type CampaignItem struct {
	ItemID         string                 `json:"item_id"`
	WorkloadUID    string                 `json:"workload_uid"`
	DeploymentName string                 `json:"deployment_name"`
	Namespace      string                 `json:"namespace"`
	ClusterName    string                 `json:"cluster_name"`
	Environment    string                 `json:"environment"`
	Criticality    string                 `json:"criticality"`
	RepoURL        string                 `json:"repo_url"`
	Paths          map[string]EnvPathSpec `json:"paths"`
	Recommendation HpaRecommendation      `json:"recommendation"`
	Findings       []WorkloadFinding      `json:"findings,omitempty"`
}

type CampaignSpec struct {
	CampaignID      string         `json:"campaign_id"`
	TenantID        int64          `json:"tenant_id"`
	ActorEmail      string         `json:"actor_email"`
	TriggerSource   TriggerSource  `json:"trigger_source"`
	RuleID          string         `json:"rule_id"`
	Title           string         `json:"title"`
	Description     string         `json:"description"`
	PolicyMode      PolicyMode     `json:"policy_mode"`
	CascadeUpTo     CascadeUpTo    `json:"cascade_up_to"`
	MaxDeltaPct     int            `json:"max_delta_pct"`
	Items           []CampaignItem `json:"items"`
	IdempotencyKey  string         `json:"idempotency_key"`
}

type ItemStatus string

const (
	ItemStatusPending        ItemStatus = "pending"
	ItemStatusRunning        ItemStatus = "running"
	ItemStatusPROpen         ItemStatus = "pr_open"
	ItemStatusPRMerged       ItemStatus = "pr_merged"
	ItemStatusPRClosed       ItemStatus = "pr_closed"
	ItemStatusAwaitingHuman  ItemStatus = "awaiting_human"
	ItemStatusFailed         ItemStatus = "failed"
	ItemStatusSkipped        ItemStatus = "skipped"
)

type EnvStepStatus string

const (
	EnvStepPending       EnvStepStatus = "pending"
	EnvStepPROpen        EnvStepStatus = "pr_open"
	EnvStepChecksGreen   EnvStepStatus = "checks_green"
	EnvStepMerged        EnvStepStatus = "merged"
	EnvStepClosed        EnvStepStatus = "closed"
	EnvStepFailed        EnvStepStatus = "failed"
	EnvStepAwaitingHuman EnvStepStatus = "awaiting_human"
)
