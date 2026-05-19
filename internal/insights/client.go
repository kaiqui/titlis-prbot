package insights

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/titlis/prbot/internal/model"
)

type Client interface {
	GetHpaRecommendation(ctx context.Context, req RecommendationRequest) (model.HpaRecommendation, error)
}

type RecommendationRequest struct {
	TenantID       int64
	WorkloadUID    string
	DeploymentName string
	Namespace      string
	Cluster        string
	Environment    string
	Criticality    string
	HasDatadog     bool
}

type HTTPClient struct {
	baseURL string
	secret  string
	http    *http.Client
}

func NewHTTPClient(baseURL, secret string) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		secret:  secret,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *HTTPClient) GetHpaRecommendation(ctx context.Context, r RecommendationRequest) (model.HpaRecommendation, error) {
	q := url.Values{}
	q.Set("tenant_id", strconv.FormatInt(r.TenantID, 10))
	q.Set("workload_uid", r.WorkloadUID)
	q.Set("deployment_name", r.DeploymentName)
	q.Set("namespace", r.Namespace)
	q.Set("cluster", r.Cluster)
	q.Set("environment", r.Environment)
	q.Set("criticality", r.Criticality)
	if r.HasDatadog {
		q.Set("has_datadog", "true")
	}
	endpoint := c.baseURL + "/v1/recommendations/hpa?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return model.HpaRecommendation{}, err
	}
	req.Header.Set("X-Internal-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return model.HpaRecommendation{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return model.HpaRecommendation{}, fmt.Errorf("insights: status %d", resp.StatusCode)
	}
	var out model.HpaRecommendation
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return model.HpaRecommendation{}, err
	}
	return out, nil
}

// Static is a test/memory client that returns a fixed recommendation.
type Static struct {
	Map map[string]model.HpaRecommendation
}

func NewStatic() *Static { return &Static{Map: map[string]model.HpaRecommendation{}} }

func (s *Static) Set(workloadUID string, r model.HpaRecommendation) {
	s.Map[workloadUID] = r
}

func (s *Static) GetHpaRecommendation(_ context.Context, r RecommendationRequest) (model.HpaRecommendation, error) {
	if v, ok := s.Map[r.WorkloadUID]; ok {
		return v, nil
	}
	return model.HpaRecommendation{WorkloadUID: r.WorkloadUID, Source: "skipped", ComputedAt: time.Now().UTC()}, nil
}
