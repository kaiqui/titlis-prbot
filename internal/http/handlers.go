package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/titlis/prbot/internal/gitprovider"
	"github.com/titlis/prbot/internal/model"
	"github.com/titlis/prbot/internal/observability"
	"github.com/titlis/prbot/internal/repo"
	"github.com/titlis/prbot/internal/scanner"
)

const discoveryWorkers = 10 // parallel goroutines for .titlis/service.yaml checks

type CampaignStarter interface {
	Start(ctx context.Context, spec model.CampaignSpec) (workflowID, runID string, err error)
	Status(ctx context.Context, campaignID string) (CampaignStatusSnapshot, error)
	Cancel(ctx context.Context, campaignID string) error
}

// Scheduler manages Temporal schedules for the discover-drift workflow.
type Scheduler interface {
	EnsureSchedule(ctx context.Context, tenantID int64, ruleID string) error
	TriggerNow(ctx context.Context, tenantID int64, ruleID string) error
	SyncPolicies(ctx context.Context, policies []model.AutoRemediationPolicy) error
}

// FactoryInvalidator allows clearing the cached GitHub provider for a tenant
// so the next call fetches a fresh token.
type FactoryInvalidator interface {
	Invalidate(tenantID int64)
}

type CampaignStatusSnapshot struct {
	CampaignID string                 `json:"campaign_id"`
	State      map[string]interface{} `json:"state"`
}

type Handlers struct {
	Mappings   repo.MappingsRepo
	Profiles   repo.GitOpsProfilesRepo
	Policies   repo.PoliciesRepo
	Starter    CampaignStarter
	Scanner    *scanner.Scanner
	Provider   gitprovider.GitProvider // used for webhook parse on incoming GitHub events
	Factory    gitprovider.Factory     // used to resolve per-tenant provider for discovery
	Sched      Scheduler               // may be nil (Noop) when Temporal is disabled
	Invalidate FactoryInvalidator      // may be nil when using StaticFactory
	Log        *observability.Logger   // may be nil

	mu       sync.Mutex
	statuses map[string]CampaignStatusSnapshot
}

func NewHandlers(m repo.MappingsRepo, p repo.GitOpsProfilesRepo, pol repo.PoliciesRepo, starter CampaignStarter, sc *scanner.Scanner, provider gitprovider.GitProvider) *Handlers {
	return &Handlers{
		Mappings: m, Profiles: p, Policies: pol, Starter: starter, Scanner: sc, Provider: provider,
		statuses: map[string]CampaignStatusSnapshot{},
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (h *Handlers) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "titlis-prbot"})
}

type createCampaignRequest struct {
	IdempotencyKey string               `json:"idempotency_key"`
	TenantID       int64                `json:"tenant_id"`
	ActorEmail     string               `json:"actor_email"`
	TriggerSource  string               `json:"trigger_source"`
	RuleID         string               `json:"rule_id"`
	Title          string               `json:"title"`
	Description    string               `json:"description"`
	PolicyMode     string               `json:"policy_mode"`
	CascadeUpTo    string               `json:"cascade_up_to"`
	MaxDeltaPct    int                  `json:"max_delta_pct"`
	Items          []model.CampaignItem `json:"items"`
}

func (h *Handlers) CreateCampaign(w http.ResponseWriter, r *http.Request) {
	var req createCampaignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.TenantID == 0 || len(req.Items) == 0 {
		writeError(w, http.StatusBadRequest, "tenant_id and items required")
		return
	}
	if req.IdempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key required")
		return
	}
	mode := model.PolicyMode(req.PolicyMode)
	switch mode {
	case model.PolicyOpenPR, model.PolicyAutoMerge, model.PolicyDiscoveryOnly, model.PolicyDisabled, "":
	default:
		writeError(w, http.StatusBadRequest, "invalid policy_mode")
		return
	}
	if mode == "" {
		mode = model.PolicyOpenPR
	}
	cascade := model.CascadeUpTo(req.CascadeUpTo)
	switch cascade {
	case model.CascadeDev, model.CascadeHml, model.CascadePrd, "":
	default:
		writeError(w, http.StatusBadRequest, "invalid cascade_up_to")
		return
	}
	if cascade == "" {
		cascade = model.CascadeDev
	}
	trigger := model.TriggerSource(req.TriggerSource)
	if trigger == "" {
		trigger = model.TriggerManual
	}
	spec := model.CampaignSpec{
		CampaignID:     "cmp_" + req.IdempotencyKey,
		TenantID:       req.TenantID,
		ActorEmail:     req.ActorEmail,
		TriggerSource:  trigger,
		RuleID:         req.RuleID,
		Title:          req.Title,
		Description:    req.Description,
		PolicyMode:     mode,
		CascadeUpTo:    cascade,
		MaxDeltaPct:    req.MaxDeltaPct,
		Items:          req.Items,
		IdempotencyKey: req.IdempotencyKey,
	}
	wfID, runID, err := h.Starter.Start(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.mu.Lock()
	h.statuses[spec.CampaignID] = CampaignStatusSnapshot{
		CampaignID: spec.CampaignID,
		State: map[string]interface{}{
			"workflow_id":     wfID,
			"run_id":          runID,
			"status":          "queued",
			"total_items":     len(spec.Items),
			"created_at":      time.Now().UTC(),
		},
	}
	h.mu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"campaign_id":     spec.CampaignID,
		"workflow_id":     wfID,
		"run_id":          runID,
		"accepted_items":  len(spec.Items),
	})
}

func (h *Handlers) GetCampaign(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "campaignID")
	snap, err := h.Starter.Status(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (h *Handlers) CancelCampaign(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "campaignID")
	if err := h.Starter.Cancel(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"campaign_id": id, "status": "cancel_signal_sent"})
}

func (h *Handlers) ListMappings(w http.ResponseWriter, r *http.Request) {
	tenantID, err := strconv.ParseInt(r.URL.Query().Get("tenant_id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}
	out, err := h.Mappings.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if out == nil {
		out = []model.WorkloadRepoMapping{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (h *Handlers) RefreshMappings(w http.ResponseWriter, r *http.Request) {
	if h.Scanner == nil {
		writeError(w, http.StatusServiceUnavailable, "scanner not configured")
		return
	}
	sum, err := h.Scanner.RunOnce(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

type policyUpsertRequest struct {
	RuleID               string `json:"rule_id"`
	Environment          string `json:"environment,omitempty"`
	Mode                 string `json:"mode"`
	CascadeUpTo          string `json:"cascade_up_to"`
	AutoMergeMaxDeltaPct int    `json:"auto_merge_max_delta_pct"`
	RequireChecksGreen   bool   `json:"require_pr_checks_green"`
	MaxPRsPerDay         int    `json:"max_prs_per_day"`
}

func (h *Handlers) UpsertPolicy(w http.ResponseWriter, r *http.Request) {
	tenantID, err := strconv.ParseInt(chi.URLParam(r, "tenantID"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant id")
		return
	}
	var req policyUpsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.RuleID == "" {
		writeError(w, http.StatusBadRequest, "rule_id required")
		return
	}
	mode := model.PolicyMode(req.Mode)
	switch mode {
	case model.PolicyDisabled, model.PolicyDiscoveryOnly, model.PolicyOpenPR, model.PolicyAutoMerge:
	default:
		writeError(w, http.StatusBadRequest, "invalid mode")
		return
	}
	cascade := model.CascadeUpTo(req.CascadeUpTo)
	if cascade == "" {
		cascade = model.CascadeDev
	}
	maxPRs := req.MaxPRsPerDay
	if maxPRs <= 0 {
		maxPRs = 10
	}
	p := model.AutoRemediationPolicy{
		TenantID:             tenantID,
		RuleID:               req.RuleID,
		Environment:          req.Environment,
		Mode:                 mode,
		CascadeUpTo:          cascade,
		AutoMergeMaxDeltaPct: req.AutoMergeMaxDeltaPct,
		RequirePRChecksGreen: req.RequireChecksGreen || mode == model.PolicyAutoMerge,
		MaxPRsPerDay:         maxPRs,
	}
	if err := h.Policies.Upsert(r.Context(), p); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if h.Sched != nil && p.Mode != model.PolicyDisabled {
		_ = h.Sched.EnsureSchedule(r.Context(), tenantID, p.RuleID)
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handlers) GetPolicy(w http.ResponseWriter, r *http.Request) {
	tenantID, err := strconv.ParseInt(chi.URLParam(r, "tenantID"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant id")
		return
	}
	q := r.URL.Query()
	p, err := h.Policies.Get(r.Context(), tenantID, q.Get("rule_id"), q.Get("environment"))
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handlers) Webhook(w http.ResponseWriter, r *http.Request) {
	if h.Provider == nil {
		writeError(w, http.StatusServiceUnavailable, "no provider configured")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body")
		return
	}
	headers := map[string]string{}
	for k := range r.Header {
		headers[k] = r.Header.Get(k)
	}
	ev, err := h.Provider.ParseWebhook(headers, body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// In a real implementation we'd correlate ev.PRNumber → workflow signal.
	// Skeleton: ack receipt.
	writeJSON(w, http.StatusOK, map[string]any{
		"type":      ev.Type,
		"pr_number": ev.PRNumber,
		"state":     ev.State,
		"merged":    ev.Merged,
	})
}

// --- GitOps Profiles ---

func (h *Handlers) ListGitOpsProfiles(w http.ResponseWriter, r *http.Request) {
	tenantID, err := strconv.ParseInt(r.URL.Query().Get("tenant_id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}
	out, err := h.Profiles.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if out == nil {
		out = []model.GitOpsProfile{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

type upsertProfileRequest struct {
	TenantID        int64             `json:"tenant_id"`
	RepoURL         string            `json:"repo_url"`
	Layout          string            `json:"layout"`
	BaseBranch      string            `json:"base_branch"`
	EnvPathTemplate map[string]string `json:"env_path_template"`
	PipelineWatcher string            `json:"pipeline_watcher,omitempty"`
}

func (h *Handlers) UpsertGitOpsProfile(w http.ResponseWriter, r *http.Request) {
	var req upsertProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.TenantID == 0 || req.RepoURL == "" {
		writeError(w, http.StatusBadRequest, "tenant_id and repo_url required")
		return
	}
	if req.BaseBranch == "" {
		req.BaseBranch = "main"
	}
	now := time.Now().UTC()
	p := model.GitOpsProfile{
		TenantID:        req.TenantID,
		RepoURL:         req.RepoURL,
		Layout:          req.Layout,
		BaseBranch:      req.BaseBranch,
		EnvPathTemplate: req.EnvPathTemplate,
		PipelineWatcher: req.PipelineWatcher,
		CreatedAt:       now,
	}
	if err := h.Profiles.Upsert(r.Context(), p); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Register the new repo with the scanner so it is included in the next refresh.
	if h.Scanner != nil {
		h.Scanner.RegisterTenantRepos(req.TenantID, []string{req.RepoURL})
	}
	writeJSON(w, http.StatusOK, p)
}

// --- OnGitHubConfigured ---

// OnGitHubConfigured is called by titlis-api when a tenant saves a GitHub token.
// Returns 202 immediately; discovery runs in a background goroutine so the caller's
// HTTP timeout does not interrupt the scan (which may take 30-60s for large GitHub accounts).
func (h *Handlers) OnGitHubConfigured(w http.ResponseWriter, r *http.Request) {
	tenantID, err := strconv.ParseInt(chi.URLParam(r, "tenantID"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant id")
		return
	}

	// Evict cached provider synchronously so the background goroutine gets a fresh token.
	if h.Invalidate != nil {
		h.Invalidate.Invalidate(tenantID)
	}

	// Respond immediately so the caller's timeout never cancels the discovery.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"tenant_id": tenantID,
		"status":    "accepted",
	})

	// Run the rest in a detached goroutine with a fresh context (5-minute budget).
	go h.runDiscovery(tenantID)
}

// runDiscovery is the background half of OnGitHubConfigured.
func (h *Handlers) runDiscovery(tenantID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log := h.Log
	logf := func(msg string, kv ...any) {
		if log != nil {
			log.Info(msg, kv...)
		}
	}
	logw := func(msg string, kv ...any) {
		if log != nil {
			log.Warn(msg, kv...)
		}
	}

	logf("github-configured: start", "tenant_id", tenantID)

	var repoURLs []string
	if h.Factory != nil {
		provider, fErr := h.Factory.ForTenant(ctx, tenantID, "")
		if fErr != nil {
			logw("github-configured: factory.ForTenant failed", "tenant_id", tenantID, "error", fErr.Error())
		} else {
			logf("github-configured: got provider", "tenant_id", tenantID, "provider", provider.Name())
			discovered, dErr := provider.ListAccessibleRepos(ctx)
			if dErr != nil {
				logw("github-configured: ListAccessibleRepos failed", "tenant_id", tenantID, "error", dErr.Error())
			} else {
				logf("github-configured: repos listed", "tenant_id", tenantID, "count", len(discovered))
				if len(discovered) > 0 {
					repoURLs = h.filterReposWithTitlisConfig(ctx, provider, discovered, log)
					logf("github-configured: repos with .titlis/service.yaml", "tenant_id", tenantID, "count", len(repoURLs))
					for _, u := range repoURLs {
						if uErr := h.Profiles.Upsert(ctx, model.GitOpsProfile{
							TenantID:   tenantID,
							RepoURL:    u,
							Layout:     "folder_per_env",
							BaseBranch: "main",
						}); uErr != nil {
							logw("github-configured: profile upsert failed", "repo", u, "error", uErr.Error())
						} else {
							logf("github-configured: profile upserted", "repo", u)
						}
					}
				}
			}
		}
	}

	// Merge pre-existing profiles not found by discovery.
	if existing, pErr := h.Profiles.List(ctx, tenantID); pErr == nil {
		seen := make(map[string]bool, len(repoURLs))
		for _, u := range repoURLs {
			seen[u] = true
		}
		for _, p := range existing {
			if !seen[p.RepoURL] {
				repoURLs = append(repoURLs, p.RepoURL)
			}
		}
	}

	if h.Scanner != nil && len(repoURLs) > 0 {
		h.Scanner.RegisterTenantRepos(tenantID, repoURLs)
		_, _ = h.Scanner.RunOnce(ctx)
	}

	if _, pErr := h.Policies.Get(ctx, tenantID, model.GlobalPolicyRuleID, ""); errors.Is(pErr, repo.ErrNotFound) {
		_ = h.Policies.Upsert(ctx, model.AutoRemediationPolicy{
			TenantID:     tenantID,
			RuleID:       model.GlobalPolicyRuleID,
			Mode:         model.PolicyOpenPR,
			CascadeUpTo:  model.CascadeDev,
			MaxPRsPerDay: 10,
			UpdatedAt:    time.Now().UTC(),
		})
	}

	if h.Sched != nil {
		for _, rid := range model.ManagedRuleIDs {
			_ = h.Sched.EnsureSchedule(ctx, tenantID, rid)
		}
	}

	logf("github-configured: done", "tenant_id", tenantID, "repos_discovered", len(repoURLs))
}

// filterReposWithTitlisConfig returns repos from the list that contain .titlis/service.yaml.
// Checks discoveryWorkers repos in parallel to keep wall-clock time under ~5s for 100 repos.
func (h *Handlers) filterReposWithTitlisConfig(ctx context.Context, provider gitprovider.GitProvider, repos []string, log *observability.Logger) []string {
	const configPath = ".titlis/service.yaml"

	type result struct {
		url   string
		found bool
	}

	work := make(chan string, len(repos))
	for _, r := range repos {
		work <- r
	}
	close(work)

	results := make(chan result, len(repos))
	var wg sync.WaitGroup
	workers := discoveryWorkers
	if workers > len(repos) {
		workers = len(repos)
	}
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repoURL := range work {
				if ctx.Err() != nil {
					return
				}
				_, err := provider.FetchFile(ctx, repoURL, "main", configPath)
				if err == nil {
					if log != nil {
						log.Info("github-configured: found .titlis/service.yaml", "repo", repoURL, "branch", "main")
					}
					results <- result{url: repoURL, found: true}
					continue
				}
				_, err2 := provider.FetchFile(ctx, repoURL, "master", configPath)
				if err2 == nil {
					if log != nil {
						log.Info("github-configured: found .titlis/service.yaml", "repo", repoURL, "branch", "master")
					}
					results <- result{url: repoURL, found: true}
					continue
				}
				results <- result{url: repoURL, found: false}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var found []string
	for r := range results {
		if r.found {
			found = append(found, r.url)
		}
	}
	return found
}
