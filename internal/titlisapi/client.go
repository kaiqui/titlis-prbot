package titlisapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// WorkloadFinding represents a workload with an open rule finding.
type WorkloadFinding struct {
	WorkloadUID    string `json:"workload_uid"`
	DeploymentName string `json:"deployment_name"`
	Namespace      string `json:"namespace"`
	ClusterName    string `json:"cluster_name"`
	RuleID         string `json:"rule_id"`
	Environment    string `json:"environment"`
	Criticality    string `json:"criticality"`
	HasDatadog     bool   `json:"has_datadog"`
}

type listFindingsResponse struct {
	Items []WorkloadFinding `json:"items"` // API wraps array in {"items":[...]}
}

// HTTPClient is an internal HTTP client for titlis-api.
type HTTPClient struct {
	baseURL string
	secret  string
	http    *http.Client
}

func NewHTTPClient(host string, port int, secret string) *HTTPClient {
	return &HTTPClient{
		baseURL: fmt.Sprintf("http://%s:%d", host, port),
		secret:  secret,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// ListWorkloadsWithFinding queries titlis-api for workloads that have an open
// finding for the given rule, scoped to the tenant. The API endpoint is
// GET /v1/internal/prbot/findings?rule_id=X (X-Internal-Secret auth).
func (c *HTTPClient) ListWorkloadsWithFinding(ctx context.Context, tenantID int64, ruleID string) ([]WorkloadFinding, error) {
	u := fmt.Sprintf("%s/v1/internal/prbot/findings?rule_id=%s&tenant_id=%d",
		c.baseURL, url.QueryEscape(ruleID), tenantID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Internal-Secret", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("titlis-api %d", resp.StatusCode)
	}
	var result listFindingsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return result.Items, nil
}

// ListWorkloadsWithFindings queries titlis-api for workloads with any open finding
// across the given list of ruleIDs. Returns deduplicated results grouped by workload.
func (c *HTTPClient) ListWorkloadsWithFindings(ctx context.Context, tenantID int64, ruleIDs []string) ([]WorkloadFinding, error) {
	if len(ruleIDs) == 0 {
		return nil, nil
	}
	ruleList := url.QueryEscape(strings.Join(ruleIDs, ","))
	u := fmt.Sprintf("%s/v1/internal/prbot/findings?rule_ids=%s&tenant_id=%d", c.baseURL, ruleList, tenantID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Internal-Secret", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("titlis-api %d", resp.StatusCode)
	}
	var result listFindingsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return result.Items, nil
}

// FindingsClient abstracts the titlis-api HTTP calls needed by the discover workflow.
type FindingsClient interface {
	ListWorkloadsWithFinding(ctx context.Context, tenantID int64, ruleID string) ([]WorkloadFinding, error)
	ListWorkloadsWithFindings(ctx context.Context, tenantID int64, ruleIDs []string) ([]WorkloadFinding, error)
}

// GitHubTokenFetcher retrieves the per-tenant GitHub PAT stored in titlis-api.
type GitHubTokenFetcher interface {
	GetGitHubToken(ctx context.Context, tenantID int64) (string, error)
}

// GetGitHubToken calls GET /v1/internal/prbot/github-token on titlis-api and returns
// the plaintext token. Returns ("", nil) when no token is configured for the tenant.
func (c *HTTPClient) GetGitHubToken(ctx context.Context, tenantID int64) (string, error) {
	u := fmt.Sprintf("%s/v1/internal/prbot/github-token?tenant_id=%d", c.baseURL, tenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Internal-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("titlis-api github-token: status %d", resp.StatusCode)
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	return result.Token, nil
}

// MemoryFindingsClient is an in-process implementation for tests and dev mode.
type MemoryFindingsClient struct {
	findings []WorkloadFinding
}

func NewMemoryFindingsClient(findings ...WorkloadFinding) *MemoryFindingsClient {
	return &MemoryFindingsClient{findings: findings}
}

func (m *MemoryFindingsClient) ListWorkloadsWithFinding(_ context.Context, tenantID int64, ruleID string) ([]WorkloadFinding, error) {
	var out []WorkloadFinding
	for _, f := range m.findings {
		if f.RuleID == ruleID {
			out = append(out, f)
		}
	}
	return out, nil
}

func (m *MemoryFindingsClient) ListWorkloadsWithFindings(_ context.Context, tenantID int64, ruleIDs []string) ([]WorkloadFinding, error) {
	set := make(map[string]bool, len(ruleIDs))
	for _, r := range ruleIDs {
		set[r] = true
	}
	var out []WorkloadFinding
	for _, f := range m.findings {
		if set[f.RuleID] {
			out = append(out, f)
		}
	}
	return out, nil
}
