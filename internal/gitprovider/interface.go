package gitprovider

import (
	"context"
	"errors"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrUnsupported  = errors.New("unsupported")
	ErrRateLimited  = errors.New("rate limited")
)

type FileContent struct {
	Path    string
	SHA     string
	Content []byte
}

type PullRequest struct {
	Number    int
	URL       string
	Branch    string
	BaseRef   string
	State     string
	Merged    bool
	HeadSHA   string
}

type CheckStatus string

const (
	CheckPending CheckStatus = "pending"
	CheckSuccess CheckStatus = "success"
	CheckFailure CheckStatus = "failure"
)

type CheckResult struct {
	Status CheckStatus
	Reason string
}

type WebhookEvent struct {
	Type     string
	PRNumber int
	State    string
	Merged   bool
	Repo     string
}

type GitProvider interface {
	Name() string

	// ListAccessibleRepos returns HTTPS URLs of all repos visible to the credential.
	// For PAT-based providers this lists the authenticated user's repos (up to 200).
	// Providers that don't support listing return (nil, nil).
	ListAccessibleRepos(ctx context.Context) ([]string, error)

	FetchFile(ctx context.Context, repo, branch, path string) (FileContent, error)
	CreateBranch(ctx context.Context, repo, baseBranch, newBranch string) error
	CommitFile(ctx context.Context, repo, branch, path, message string, content []byte) error
	OpenPR(ctx context.Context, repo, baseBranch, headBranch, title, body string, labels []string) (PullRequest, error)
	FindOpenPR(ctx context.Context, repo, headBranch string) (PullRequest, error)
	MergePR(ctx context.Context, repo string, prNumber int, method string) error
	WaitChecks(ctx context.Context, repo string, prNumber int, timeoutCallback func() bool) (CheckResult, error)

	ParseWebhook(headers map[string]string, body []byte) (WebhookEvent, error)
}
