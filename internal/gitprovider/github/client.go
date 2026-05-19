package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	gogithub "github.com/google/go-github/v84/github"

	"github.com/titlis/prbot/internal/gitprovider"
)

type bearerTransport struct {
	token string
	base  http.RoundTripper
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "token "+t.token)
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

type Client struct {
	AppID         int64
	WebhookSecret string
	privateKey    []byte
	mu            sync.RWMutex
	transports    map[int64]*ghinstallation.Transport
	tokenClient   *http.Client // non-nil → PAT mode, skips App JWT auth
}

func NewClient(appID, installationID int64, privateKey []byte, webhookSecret string) *Client {
	c := &Client{
		AppID:         appID,
		WebhookSecret: webhookSecret,
		privateKey:    privateKey,
		transports:    make(map[int64]*ghinstallation.Transport),
	}
	if appID > 0 && installationID > 0 && len(privateKey) > 0 {
		// pre-warm transport for the default installation
		_, _ = c.transportFor(installationID)
	}
	return c
}

// NewTokenClient creates a Client that authenticates via a personal access token (PAT)
// instead of a GitHub App. webhookSecret may be empty if webhooks are not used.
func NewTokenClient(token, webhookSecret string) *Client {
	return &Client{
		WebhookSecret: webhookSecret,
		transports:    make(map[int64]*ghinstallation.Transport),
		tokenClient:   &http.Client{Transport: &bearerTransport{token: token}},
	}
}

func (c *Client) Name() string { return "github" }

func (c *Client) transportFor(installationID int64) (*ghinstallation.Transport, error) {
	c.mu.RLock()
	t, ok := c.transports[installationID]
	c.mu.RUnlock()
	if ok {
		return t, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok = c.transports[installationID]; ok {
		return t, nil
	}
	itr, err := ghinstallation.New(http.DefaultTransport, c.AppID, installationID, c.privateKey)
	if err != nil {
		return nil, fmt.Errorf("ghinstallation: %w", err)
	}
	c.transports[installationID] = itr
	return itr, nil
}

// defaultInstallation returns the first cached installation or an error if none.
func (c *Client) defaultInstallation() (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id := range c.transports {
		return id, nil
	}
	return 0, gitprovider.ErrUnsupported
}

func (c *Client) ghClient(ctx context.Context) (*gogithub.Client, error) {
	if c.tokenClient != nil {
		return gogithub.NewClient(c.tokenClient), nil
	}
	id, err := c.defaultInstallation()
	if err != nil {
		return nil, fmt.Errorf("no github installation configured: %w", err)
	}
	itr, err := c.transportFor(id)
	if err != nil {
		return nil, err
	}
	return gogithub.NewClient(&http.Client{Transport: itr}), nil
}

func splitRepo(repoURL string) (owner, name string, err error) {
	// Accepts "owner/repo", "https://github.com/owner/repo", "git@github.com:owner/repo.git"
	s := repoURL
	for _, prefix := range []string{"https://github.com/", "http://github.com/", "git@github.com:"} {
		if strings.HasPrefix(s, prefix) {
			s = strings.TrimPrefix(s, prefix)
			break
		}
	}
	s = strings.TrimSuffix(s, ".git")
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("cannot parse repo URL %q", repoURL)
	}
	return parts[0], parts[1], nil
}

// ListAccessibleRepos returns HTTPS URLs of repos visible to the credential.
// In PAT mode it lists the authenticated user's repos (up to 200).
// In GitHub App mode (no tokenClient) it returns nil — App auth uses
// installation-scoped access already enforced by ghinstallation.
func (c *Client) ListAccessibleRepos(ctx context.Context) ([]string, error) {
	if c.tokenClient == nil {
		return nil, nil
	}
	gh, err := c.ghClient(ctx)
	if err != nil {
		return nil, err
	}
	const maxRepos = 200
	var urls []string
	opts := &gogithub.RepositoryListOptions{ListOptions: gogithub.ListOptions{PerPage: 100}}
	for {
		repos, resp, err := gh.Repositories.List(ctx, "", opts)
		if err != nil {
			return urls, err
		}
		for _, r := range repos {
			if u := r.GetHTMLURL(); u != "" {
				urls = append(urls, u)
			}
		}
		if resp.NextPage == 0 || len(urls) >= maxRepos {
			break
		}
		opts.Page = resp.NextPage
	}
	return urls, nil
}

func (c *Client) FetchFile(ctx context.Context, repo, branch, path string) (gitprovider.FileContent, error) {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return gitprovider.FileContent{}, err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return gitprovider.FileContent{}, err
	}
	fc, _, resp, err := gh.Repositories.GetContents(ctx, owner, repoName, path, &gogithub.RepositoryContentGetOptions{Ref: branch})
	if err != nil {
		return gitprovider.FileContent{}, mapHTTPError(resp, err)
	}
	content, err := fc.GetContent()
	if err != nil {
		return gitprovider.FileContent{}, err
	}
	return gitprovider.FileContent{Path: path, SHA: fc.GetSHA(), Content: []byte(content)}, nil
}

func (c *Client) CreateBranch(ctx context.Context, repo, baseBranch, newBranch string) error {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return err
	}
	baseRef, _, err := gh.Git.GetRef(ctx, owner, repoName, "refs/heads/"+baseBranch)
	if err != nil {
		return fmt.Errorf("get base ref: %w", err)
	}
	_, resp, err := gh.Git.CreateRef(ctx, owner, repoName, gogithub.CreateRef{
		Ref: "refs/heads/" + newBranch,
		SHA: baseRef.Object.GetSHA(),
	})
	return mapHTTPError(resp, err)
}

func (c *Client) CommitFile(ctx context.Context, repo, branch, path, message string, content []byte) error {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return err
	}
	committer := &gogithub.CommitAuthor{
		Name:  gogithub.Ptr("titlis-prbot"),
		Email: gogithub.Ptr("prbot@titlis.io"),
	}
	fileOpts := &gogithub.RepositoryContentFileOptions{
		Message:   gogithub.Ptr(message),
		Content:   content,
		Branch:    gogithub.Ptr(branch),
		Committer: committer,
	}
	// Fetch current SHA so we can update (GitHub requires SHA for updates).
	existing, _, _, _ := gh.Repositories.GetContents(ctx, owner, repoName, path, &gogithub.RepositoryContentGetOptions{Ref: branch})
	if existing != nil {
		fileOpts.SHA = gogithub.Ptr(existing.GetSHA())
		_, _, err = gh.Repositories.UpdateFile(ctx, owner, repoName, path, fileOpts)
	} else {
		_, _, err = gh.Repositories.CreateFile(ctx, owner, repoName, path, fileOpts)
	}
	return err
}

func (c *Client) OpenPR(ctx context.Context, repo, baseBranch, headBranch, title, body string, labels []string) (gitprovider.PullRequest, error) {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return gitprovider.PullRequest{}, err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return gitprovider.PullRequest{}, err
	}
	pr, resp, err := gh.PullRequests.Create(ctx, owner, repoName, &gogithub.NewPullRequest{
		Title: gogithub.Ptr(title),
		Body:  gogithub.Ptr(body),
		Head:  gogithub.Ptr(headBranch),
		Base:  gogithub.Ptr(baseBranch),
	})
	if err != nil {
		return gitprovider.PullRequest{}, mapHTTPError(resp, err)
	}
	if len(labels) > 0 {
		// Best-effort label addition; ignore errors.
		_, _, _ = gh.Issues.AddLabelsToIssue(ctx, owner, repoName, pr.GetNumber(), labels)
	}
	return toPR(pr, headBranch), nil
}

func (c *Client) FindOpenPR(ctx context.Context, repo, headBranch string) (gitprovider.PullRequest, error) {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return gitprovider.PullRequest{}, err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return gitprovider.PullRequest{}, err
	}
	prs, _, err := gh.PullRequests.List(ctx, owner, repoName, &gogithub.PullRequestListOptions{
		State: "open",
		Head:  owner + ":" + headBranch,
	})
	if err != nil {
		return gitprovider.PullRequest{}, err
	}
	if len(prs) == 0 {
		return gitprovider.PullRequest{}, gitprovider.ErrNotFound
	}
	return toPR(prs[0], headBranch), nil
}

func (c *Client) MergePR(ctx context.Context, repo string, prNumber int, method string) error {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return err
	}
	if method == "" {
		method = "squash"
	}
	_, _, err = gh.PullRequests.Merge(ctx, owner, repoName, prNumber, "", &gogithub.PullRequestOptions{
		MergeMethod: method,
	})
	return err
}

func (c *Client) WaitChecks(ctx context.Context, repo string, prNumber int, timeoutCallback func() bool) (gitprovider.CheckResult, error) {
	gh, err := c.ghClient(ctx)
	if err != nil {
		return gitprovider.CheckResult{}, err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return gitprovider.CheckResult{}, err
	}
	pr, _, err := gh.PullRequests.Get(ctx, owner, repoName, prNumber)
	if err != nil {
		return gitprovider.CheckResult{}, err
	}
	headSHA := pr.GetHead().GetSHA()

	for {
		if timeoutCallback() {
			return gitprovider.CheckResult{Status: gitprovider.CheckPending, Reason: "timeout"}, nil
		}
		checks, _, err := gh.Checks.ListCheckRunsForRef(ctx, owner, repoName, headSHA, &gogithub.ListCheckRunsOptions{})
		if err != nil {
			return gitprovider.CheckResult{}, err
		}
		if checks.GetTotal() == 0 {
			return gitprovider.CheckResult{Status: gitprovider.CheckSuccess, Reason: "no checks"}, nil
		}
		allDone, anyFailed := true, false
		for _, run := range checks.CheckRuns {
			if run.GetStatus() != "completed" {
				allDone = false
				break
			}
			switch run.GetConclusion() {
			case "failure", "cancelled", "timed_out", "action_required":
				anyFailed = true
			}
		}
		if allDone {
			if anyFailed {
				return gitprovider.CheckResult{Status: gitprovider.CheckFailure, Reason: "checks failed"}, nil
			}
			return gitprovider.CheckResult{Status: gitprovider.CheckSuccess}, nil
		}
		select {
		case <-ctx.Done():
			return gitprovider.CheckResult{}, ctx.Err()
		case <-time.After(15 * time.Second):
		}
	}
}

func (c *Client) ParseWebhook(headers map[string]string, body []byte) (gitprovider.WebhookEvent, error) {
	if c.WebhookSecret != "" {
		if !verifyHMAC(c.WebhookSecret, headers["X-Hub-Signature-256"], body) {
			return gitprovider.WebhookEvent{}, errors.New("invalid webhook signature")
		}
	}
	eventType := headers["X-GitHub-Event"]
	if eventType != "pull_request" {
		return gitprovider.WebhookEvent{Type: eventType}, nil
	}
	var payload struct {
		Action      string `json:"action"`
		PullRequest struct {
			Number int    `json:"number"`
			State  string `json:"state"`
			Merged bool   `json:"merged"`
		} `json:"pull_request"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return gitprovider.WebhookEvent{}, fmt.Errorf("parse: %w", err)
	}
	return gitprovider.WebhookEvent{
		Type:     "pull_request." + payload.Action,
		PRNumber: payload.PullRequest.Number,
		State:    payload.PullRequest.State,
		Merged:   payload.PullRequest.Merged,
		Repo:     payload.Repository.FullName,
	}, nil
}

func toPR(pr *gogithub.PullRequest, headBranch string) gitprovider.PullRequest {
	return gitprovider.PullRequest{
		Number:  pr.GetNumber(),
		URL:     pr.GetHTMLURL(),
		Branch:  headBranch,
		BaseRef: pr.GetBase().GetRef(),
		State:   pr.GetState(),
		Merged:  pr.GetMerged(),
		HeadSHA: pr.GetHead().GetSHA(),
	}
}

func mapHTTPError(resp *gogithub.Response, err error) error {
	if err == nil {
		return nil
	}
	if resp != nil {
		switch resp.StatusCode {
		case http.StatusNotFound:
			return gitprovider.ErrNotFound
		case http.StatusConflict, http.StatusUnprocessableEntity:
			return gitprovider.ErrConflict
		case http.StatusTooManyRequests, 403: // 403 can be rate limit from GitHub
			return gitprovider.ErrRateLimited
		}
	}
	return err
}

func verifyHMAC(secret, signature string, body []byte) bool {
	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}
	expected := strings.TrimPrefix(signature, "sha256=")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	got := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(got))
}
