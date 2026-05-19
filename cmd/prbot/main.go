package main

import (
	"context"
	"fmt"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/config"
	"github.com/titlis/prbot/internal/gitprovider"
	"github.com/titlis/prbot/internal/gitprovider/github"
	"github.com/titlis/prbot/internal/gitprovider/memory"
	thttp "github.com/titlis/prbot/internal/http"
	"github.com/titlis/prbot/internal/insights"
	"github.com/titlis/prbot/internal/observability"
	"github.com/titlis/prbot/internal/repo"
	"github.com/titlis/prbot/internal/scanner"
	"github.com/titlis/prbot/internal/temporal"
	"github.com/titlis/prbot/internal/titlisapi"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	log := observability.NewLogger(cfg.LogLevel, cfg.LogFormat)
	log.Info("starting", "service", "titlis-prbot", "env", cfg.AppEnv, "port", cfg.Port)

	// --- data repos (always postgres when DATABASE_URL is set) ---
	var (
		mappings repo.MappingsRepo       = repo.NewMemoryMappings()
		profiles repo.GitOpsProfilesRepo = repo.NewMemoryProfiles()
		policies repo.PoliciesRepo       = repo.NewMemoryPolicies()
	)
	if cfg.DatabaseURL != "" {
		db, dbErr := pgxpool.New(context.Background(), cfg.DatabaseURL)
		if dbErr != nil {
			log.Error("postgres connect failed; using memory repos", "error", dbErr.Error())
		} else {
			mappings = repo.NewPGMappings(db)
			profiles = repo.NewPGProfiles(db)
			policies = repo.NewPGPolicies(db)
			log.Info("using postgres repos")
		}
	}

	// --- titlis-api client (findings + github token) ---
	var apiClient *titlisapi.HTTPClient
	var findingsClient titlisapi.FindingsClient
	if cfg.AppEnv == "local" {
		findingsClient = titlisapi.NewMemoryFindingsClient()
		log.Info("using memory findings client")
	} else {
		apiClient = titlisapi.NewHTTPClient(cfg.TitlisAPIHost, cfg.TitlisAPIPort, cfg.InternalSecret)
		findingsClient = apiClient
		log.Info("using http findings client", "host", cfg.TitlisAPIHost, "port", cfg.TitlisAPIPort)
	}

	// --- git provider factory ---
	// Global fallback: GitHub App (env vars) or memory when UseMemoryProvider is true.
	var globalProvider gitprovider.GitProvider
	if cfg.UseMemoryProvider {
		globalProvider = memory.NewProvider()
		log.Info("using memory git provider as global fallback")
	} else {
		var privateKey []byte
		if cfg.GitHubAppPrivateKeyPath != "" {
			privateKey, err = os.ReadFile(cfg.GitHubAppPrivateKeyPath)
			if err != nil {
				log.Warn("could not read github app private key", "path", cfg.GitHubAppPrivateKeyPath, "error", err.Error())
			}
		}
		if cfg.GitHubAppID > 0 && cfg.GitHubAppInstallationID > 0 && len(privateKey) > 0 {
			globalProvider = github.NewClient(cfg.GitHubAppID, cfg.GitHubAppInstallationID, privateKey, cfg.GitHubWebhookSecret)
			log.Info("using github app provider", "app_id", cfg.GitHubAppID)
		} else {
			log.Info("no github app configured; per-tenant PAT only")
		}
	}

	// PerTenantFactory: PAT per tenant (fetched from titlis-api) with App/memory fallback.
	// The builder func breaks the import cycle: main imports both packages, neither imports the other.
	var factory gitprovider.Factory
	var perTenantFactory *gitprovider.PerTenantFactory
	if apiClient != nil {
		builder := func(token string) gitprovider.GitProvider {
			return github.NewTokenClient(token, "")
		}
		perTenantFactory = gitprovider.NewPerTenantFactory(apiClient, builder, globalProvider)
		factory = perTenantFactory
		log.Info("using per-tenant github factory")
	} else {
		factory = gitprovider.StaticFactory{Provider: globalProvider}
	}

	// --- insights client ---
	var insightsClient insights.Client
	if cfg.InsightsHost != "" && cfg.InsightsInternalSecret != "" {
		baseURL := fmt.Sprintf("http://%s:%d", cfg.InsightsHost, cfg.InsightsPort)
		insightsClient = insights.NewHTTPClient(baseURL, cfg.InsightsInternalSecret)
		log.Info("using http insights client", "host", cfg.InsightsHost, "port", cfg.InsightsPort)
	} else {
		insightsClient = insights.NewStatic()
		log.Warn("insights not configured; using static stub")
	}

	// Use HTTP event transport (authenticated) in non-local envs; fall back to UDP for local.
	var udp titlisapi.UDPClient
	if cfg.AppEnv == "local" || cfg.TitlisAPIHost == "" {
		udp = titlisapi.NewUDPClient(cfg.TitlisAPIUDPHost, cfg.TitlisAPIUDPPort)
		log.Info("using udp event transport")
	} else {
		udp = titlisapi.NewHTTPEventClient(cfg.TitlisAPIHost, cfg.TitlisAPIPort, cfg.InternalSecret)
		log.Info("using http event transport", "host", cfg.TitlisAPIHost, "port", cfg.TitlisAPIPort)
	}
	var aiManifestClient titlisapi.AIManifestClient
	if cfg.AppEnv == "local" || cfg.TitlisAPIHost == "" {
		aiManifestClient = titlisapi.NoopAIManifestClient{}
		log.Info("using noop ai manifest client")
	} else {
		aiManifestClient = titlisapi.NewHTTPAIManifestClient(cfg.TitlisAPIHost, cfg.TitlisAPIPort, cfg.InternalSecret)
		log.Info("using http ai manifest client", "host", cfg.TitlisAPIHost, "port", cfg.TitlisAPIPort)
	}
	a := activity.New(factory, insightsClient, mappings, profiles, policies, findingsClient, aiManifestClient, udp, log)
	sc := scanner.NewScanner(factory, mappings)

	// Pre-register repos from existing GitOps profiles at startup.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		allProfiles := loadAllProfiles(ctx, policies, profiles, log)
		for tenantID, repos := range allProfiles {
			sc.RegisterTenantRepos(tenantID, repos)
		}
		if len(allProfiles) > 0 {
			log.Info("pre-registered repos from profiles", "tenant_count", len(allProfiles))
		}
	}()

	// --- Temporal setup ---
	var (
		starter thttp.CampaignStarter
		tClient client.Client
		w       worker.Worker
		sched   thttp.Scheduler
	)

	if !cfg.DisableTemporal {
		c, dialErr := client.Dial(client.Options{HostPort: cfg.TemporalHost, Namespace: cfg.TemporalNamespace})
		if dialErr != nil {
			log.Warn("temporal not reachable; falling back to memory starter", "error", dialErr.Error())
			starter = temporal.NewMemoryStarter()
			sched = temporal.NoopScheduleManager{}
		} else {
			tClient = c
			w = temporal.NewWorker(c, cfg.TemporalTaskQueue, a, worker.Options{
				MaxConcurrentActivityExecutionSize:     cfg.WorkerConcurrentActivities,
				MaxConcurrentWorkflowTaskExecutionSize: cfg.WorkerConcurrentWorkflows,
			})
			go func() {
				if err := w.Run(worker.InterruptCh()); err != nil {
					log.Error("temporal worker stopped", "error", err.Error())
				}
			}()
			starter = temporal.NewStarter(c, cfg.TemporalTaskQueue)
			schedMgr := temporal.NewScheduleManager(c, cfg.TemporalTaskQueue)
			sched = schedMgr

			// Sync existing policies → Temporal schedules at startup.
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				syncStartupSchedules(ctx, policies, schedMgr, log)
			}()
		}
	} else {
		log.Info("temporal disabled by configuration")
		starter = temporal.NewMemoryStarter()
		sched = temporal.NoopScheduleManager{}
	}

	// --- HTTP handlers & router ---
	handlers := thttp.NewHandlers(mappings, profiles, policies, starter, sc, globalProvider)
	handlers.Factory = factory
	handlers.Sched = sched
	handlers.Log = log
	if perTenantFactory != nil {
		handlers.Invalidate = perTenantFactory
	}
	router := thttp.NewRouter(handlers, cfg.InternalSecret, log)

	// --- Periodic scanner tick ---
	if cfg.ScannerIntervalMinutes > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(cfg.ScannerIntervalMinutes) * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				sum, runErr := sc.RunOnce(ctx)
				cancel()
				if runErr != nil {
					log.Error("periodic scan failed", "error", runErr.Error())
				} else {
					log.Info("periodic scan complete", "repos", sum.RepoCount, "found", sum.Found)
				}
			}
		}()
	}

	srv := &nethttp.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case s := <-stop:
		log.Info("shutdown signal received", "signal", s.String())
	case err := <-errCh:
		log.Error("server error", "error", err.Error())
	}

	if w != nil {
		w.Stop()
	}
	if tClient != nil {
		tClient.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// loadAllProfiles returns a map of tenantID → repoURLs from all GitOps profiles.
// Uses ListEligibleTenants to discover which tenants have policies, then loads their profiles.
func loadAllProfiles(ctx context.Context, policies repo.PoliciesRepo, profiles repo.GitOpsProfilesRepo, log *observability.Logger) map[int64][]string {
	tenants, err := policies.ListEligibleTenants(ctx, "manifest")
	if err != nil {
		log.Warn("could not list eligible tenants for startup scan", "error", err.Error())
		return nil
	}
	result := make(map[int64][]string, len(tenants))
	for _, tid := range tenants {
		ps, pErr := profiles.List(ctx, tid)
		if pErr != nil || len(ps) == 0 {
			continue
		}
		repos := make([]string, 0, len(ps))
		for _, p := range ps {
			repos = append(repos, p.RepoURL)
		}
		result[tid] = repos
	}
	return result
}

// syncStartupSchedules loads all eligible policies from the DB and ensures
// a Temporal schedule exists for each active (tenant, rule) pair.
func syncStartupSchedules(ctx context.Context, policies repo.PoliciesRepo, sched *temporal.ScheduleManager, log *observability.Logger) {
	tenants, err := policies.ListEligibleTenants(ctx, "manifest")
	if err != nil {
		log.Warn("startup schedule sync: could not list tenants", "error", err.Error())
		return
	}
	synced := 0
	for _, tid := range tenants {
		p, pErr := policies.Get(ctx, tid, "manifest", "")
		if pErr != nil || p.Mode == "disabled" {
			continue
		}
		if sErr := sched.EnsureSchedule(ctx, tid, "manifest"); sErr != nil {
			log.Warn("startup schedule sync: ensure failed", "tenant", tid, "error", sErr.Error())
		} else {
			synced++
		}
	}
	log.Info("startup schedule sync complete", "synced", synced, "tenants", len(tenants))
}
