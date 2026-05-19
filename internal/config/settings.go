package config

import "github.com/kelseyhightower/envconfig"

type Settings struct {
	Port           int    `envconfig:"PORT" default:"8080"`
	AppEnv         string `envconfig:"TITLIS_APP_ENV" default:"local"`
	LogLevel       string `envconfig:"LOG_LEVEL" default:"info"`
	LogFormat      string `envconfig:"LOG_FORMAT" default:"json"`
	InternalSecret string `envconfig:"TITLIS_API_INTERNAL_SECRET" default:"dev-secret"`

	TitlisAPIHost string `envconfig:"TITLIS_API_HOST" default:"titlis-api"`
	TitlisAPIPort int    `envconfig:"TITLIS_API_PORT" default:"8080"`

	TitlisAPIUDPHost string `envconfig:"TITLIS_API_UDP_HOST" default:"titlis-api"`
	TitlisAPIUDPPort int    `envconfig:"TITLIS_API_UDP_PORT" default:"8125"`

	InsightsHost           string `envconfig:"TITLIS_INSIGHTS_HOST" default:"titlis-insights"`
	InsightsPort           int    `envconfig:"TITLIS_INSIGHTS_PORT" default:"8091"`
	InsightsInternalSecret string `envconfig:"TITLIS_INSIGHTS_INTERNAL_SECRET" default:"dev-secret"`

	TemporalHost      string `envconfig:"TEMPORAL_HOST" default:"temporal-frontend:7233"`
	TemporalNamespace string `envconfig:"TEMPORAL_NAMESPACE" default:"titlis"`
	TemporalTaskQueue string `envconfig:"TEMPORAL_TASK_QUEUE" default:"prbot-main"`

	GitHubAppID             int64  `envconfig:"GITHUB_APP_ID" default:"0"`
	GitHubAppInstallationID int64  `envconfig:"GITHUB_APP_INSTALLATION_ID" default:"0"`
	GitHubAppPrivateKeyPath string `envconfig:"GITHUB_APP_PRIVATE_KEY_PATH" default:""`
	GitHubWebhookSecret     string `envconfig:"GITHUB_WEBHOOK_SECRET" default:""`

	ScannerIntervalMinutes int `envconfig:"SCANNER_INTERVAL_MINUTES" default:"60"`

	WorkerConcurrentActivities int `envconfig:"PRBOT_WORKER_CONCURRENT_ACTIVITIES" default:"16"`
	WorkerConcurrentWorkflows  int `envconfig:"PRBOT_WORKER_CONCURRENT_WORKFLOWS" default:"64"`
	DefaultPRChecksTimeoutMin  int `envconfig:"PRBOT_DEFAULT_PR_CHECKS_TIMEOUT_MINUTES" default:"30"`

	DatabaseURL string `envconfig:"DATABASE_URL" default:""`

	UseMemoryProvider bool `envconfig:"PRBOT_USE_MEMORY_PROVIDER" default:"true"`
	DisableTemporal   bool `envconfig:"PRBOT_DISABLE_TEMPORAL" default:"false"`
}

func Load() (Settings, error) {
	var s Settings
	if err := envconfig.Process("", &s); err != nil {
		return Settings{}, err
	}
	return s, nil
}
