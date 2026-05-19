package titlisapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/titlis/prbot/internal/model"
)

// AIManifestClient generates corrected deployment manifests via titlis-ai (proxied by titlis-api).
type AIManifestClient interface {
	GenerateManifestPatch(ctx context.Context, req model.ManifestPatchRequest) (model.ManifestPatchResponse, error)
}

type HTTPAIManifestClient struct {
	baseURL string
	secret  string
	http    *http.Client
}

func NewHTTPAIManifestClient(host string, port int, secret string) *HTTPAIManifestClient {
	return &HTTPAIManifestClient{
		baseURL: fmt.Sprintf("http://%s:%d", host, port),
		secret:  secret,
		http:    &http.Client{Timeout: 3 * time.Minute},
	}
}

func (c *HTTPAIManifestClient) GenerateManifestPatch(ctx context.Context, req model.ManifestPatchRequest) (model.ManifestPatchResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return model.ManifestPatchResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/internal/prbot/generate-manifest-patch",
		bytes.NewReader(body))
	if err != nil {
		return model.ManifestPatchResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Internal-Secret", c.secret)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return model.ManifestPatchResponse{}, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return model.ManifestPatchResponse{}, fmt.Errorf("titlis-api generate-manifest-patch: status %d", resp.StatusCode)
	}
	var result model.ManifestPatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return model.ManifestPatchResponse{}, fmt.Errorf("decode: %w", err)
	}
	return result, nil
}

// NoopAIManifestClient is used when AI manifest generation is not configured.
type NoopAIManifestClient struct{}

func (NoopAIManifestClient) GenerateManifestPatch(_ context.Context, _ model.ManifestPatchRequest) (model.ManifestPatchResponse, error) {
	return model.ManifestPatchResponse{}, fmt.Errorf("ai manifest client not configured")
}
