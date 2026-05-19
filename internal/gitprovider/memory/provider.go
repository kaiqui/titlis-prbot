package memory

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/titlis/prbot/internal/gitprovider"
)

type repoState struct {
	files   map[string]map[string][]byte // branch -> path -> content
	prs     map[int]*gitprovider.PullRequest
	checks  map[int]gitprovider.CheckStatus
}

type Provider struct {
	mu      sync.Mutex
	repos   map[string]*repoState
	nextPR  int64
}

func NewProvider() *Provider {
	return &Provider{repos: map[string]*repoState{}}
}

func (p *Provider) Name() string { return "memory" }

func (p *Provider) ListAccessibleRepos(_ context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	repos := make([]string, 0, len(p.repos))
	for r := range p.repos {
		repos = append(repos, r)
	}
	return repos, nil
}

func (p *Provider) ensure(repo string) *repoState {
	if r, ok := p.repos[repo]; ok {
		return r
	}
	r := &repoState{
		files:  map[string]map[string][]byte{"main": {}},
		prs:    map[int]*gitprovider.PullRequest{},
		checks: map[int]gitprovider.CheckStatus{},
	}
	p.repos[repo] = r
	return r
}

func (p *Provider) Seed(repo, branch, path string, content []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.ensure(repo)
	if _, ok := r.files[branch]; !ok {
		r.files[branch] = map[string][]byte{}
	}
	r.files[branch][path] = append([]byte(nil), content...)
}

func sha(b []byte) string {
	h := sha1.Sum(b)
	return hex.EncodeToString(h[:])
}

func (p *Provider) FetchFile(_ context.Context, repo, branch, path string) (gitprovider.FileContent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.repos[repo]
	if !ok {
		return gitprovider.FileContent{}, gitprovider.ErrNotFound
	}
	br, ok := r.files[branch]
	if !ok {
		return gitprovider.FileContent{}, gitprovider.ErrNotFound
	}
	content, ok := br[path]
	if !ok {
		return gitprovider.FileContent{}, gitprovider.ErrNotFound
	}
	return gitprovider.FileContent{Path: path, SHA: sha(content), Content: append([]byte(nil), content...)}, nil
}

func (p *Provider) CreateBranch(_ context.Context, repo, baseBranch, newBranch string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.ensure(repo)
	base, ok := r.files[baseBranch]
	if !ok {
		return gitprovider.ErrNotFound
	}
	if _, exists := r.files[newBranch]; exists {
		return gitprovider.ErrConflict
	}
	clone := make(map[string][]byte, len(base))
	for k, v := range base {
		clone[k] = append([]byte(nil), v...)
	}
	r.files[newBranch] = clone
	return nil
}

func (p *Provider) CommitFile(_ context.Context, repo, branch, path, _ string, content []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.ensure(repo)
	br, ok := r.files[branch]
	if !ok {
		return gitprovider.ErrNotFound
	}
	br[path] = append([]byte(nil), content...)
	return nil
}

func (p *Provider) OpenPR(_ context.Context, repo, baseBranch, headBranch, title, body string, _ []string) (gitprovider.PullRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.ensure(repo)
	for _, existing := range r.prs {
		if existing.Branch == headBranch && existing.State == "open" {
			return *existing, gitprovider.ErrConflict
		}
	}
	num := int(atomic.AddInt64(&p.nextPR, 1))
	pr := &gitprovider.PullRequest{
		Number:  num,
		URL:     fmt.Sprintf("memory://%s/pull/%d", repo, num),
		Branch:  headBranch,
		BaseRef: baseBranch,
		State:   "open",
		HeadSHA: sha([]byte(fmt.Sprintf("%s|%s|%s", repo, headBranch, title))),
	}
	r.prs[num] = pr
	r.checks[num] = gitprovider.CheckPending
	_ = body
	return *pr, nil
}

func (p *Provider) FindOpenPR(_ context.Context, repo, headBranch string) (gitprovider.PullRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.repos[repo]
	if !ok {
		return gitprovider.PullRequest{}, gitprovider.ErrNotFound
	}
	for _, pr := range r.prs {
		if pr.Branch == headBranch && pr.State == "open" {
			return *pr, nil
		}
	}
	return gitprovider.PullRequest{}, gitprovider.ErrNotFound
}

func (p *Provider) MergePR(_ context.Context, repo string, prNumber int, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.repos[repo]
	if !ok {
		return gitprovider.ErrNotFound
	}
	pr, ok := r.prs[prNumber]
	if !ok {
		return gitprovider.ErrNotFound
	}
	if pr.State != "open" {
		return gitprovider.ErrConflict
	}
	pr.State = "merged"
	pr.Merged = true
	// fast-forward main with branch contents
	if branchFiles, ok := r.files[pr.Branch]; ok {
		if _, ok := r.files[pr.BaseRef]; !ok {
			r.files[pr.BaseRef] = map[string][]byte{}
		}
		for k, v := range branchFiles {
			r.files[pr.BaseRef][k] = append([]byte(nil), v...)
		}
	}
	return nil
}

func (p *Provider) SetChecks(repo string, prNumber int, status gitprovider.CheckStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.ensure(repo)
	r.checks[prNumber] = status
}

func (p *Provider) WaitChecks(_ context.Context, repo string, prNumber int, _ func() bool) (gitprovider.CheckResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.repos[repo]
	if !ok {
		return gitprovider.CheckResult{}, gitprovider.ErrNotFound
	}
	status, ok := r.checks[prNumber]
	if !ok {
		return gitprovider.CheckResult{Status: gitprovider.CheckPending}, nil
	}
	return gitprovider.CheckResult{Status: status}, nil
}

func (p *Provider) ParseWebhook(headers map[string]string, body []byte) (gitprovider.WebhookEvent, error) {
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
		return gitprovider.WebhookEvent{}, err
	}
	_ = headers
	return gitprovider.WebhookEvent{
		Type:     "pull_request." + payload.Action,
		PRNumber: payload.PullRequest.Number,
		State:    payload.PullRequest.State,
		Merged:   payload.PullRequest.Merged,
		Repo:     payload.Repository.FullName,
	}, nil
}
