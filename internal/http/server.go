package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/titlis/prbot/internal/observability"
)

func NewRouter(h *Handlers, internalSecret string, log *observability.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(RequestLogger(log))
	r.Get("/health", h.Health)
	// webhook validation is performed by provider (HMAC) — auth-free path.
	r.Post("/v1/webhook/github", h.Webhook)
	r.Group(func(r chi.Router) {
		r.Use(InternalSecretAuth(internalSecret))
		r.Post("/v1/campaigns", h.CreateCampaign)
		r.Get("/v1/campaigns/{campaignID}", h.GetCampaign)
		r.Post("/v1/campaigns/{campaignID}/cancel", h.CancelCampaign)
		r.Get("/v1/mappings", h.ListMappings)
		r.Post("/v1/mappings/refresh", h.RefreshMappings)
		r.Get("/v1/tenants/{tenantID}/policies", h.GetPolicy)
		r.Put("/v1/tenants/{tenantID}/policies", h.UpsertPolicy)
		r.Get("/v1/gitops-profiles", h.ListGitOpsProfiles)
		r.Put("/v1/gitops-profiles", h.UpsertGitOpsProfile)
		r.Post("/v1/tenants/{tenantID}/github-configured", h.OnGitHubConfigured)
	})
	return r
}
