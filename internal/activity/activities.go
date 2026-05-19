package activity

import (
	"context"
	"errors"
	"fmt"

	"github.com/titlis/prbot/internal/gitprovider"
	"github.com/titlis/prbot/internal/insights"
	"github.com/titlis/prbot/internal/model"
	"github.com/titlis/prbot/internal/observability"
	"github.com/titlis/prbot/internal/patch"
	"github.com/titlis/prbot/internal/repo"
	"github.com/titlis/prbot/internal/scanner"
	"github.com/titlis/prbot/internal/titlisapi"
)


type Activities struct {
	Factory    gitprovider.Factory
	Insights   insights.Client
	Mappings   repo.MappingsRepo
	Profiles   repo.GitOpsProfilesRepo
	Policies   repo.PoliciesRepo
	Findings   titlisapi.FindingsClient
	AIManifest titlisapi.AIManifestClient
	UDP        titlisapi.UDPClient
	Logger     *observability.Logger
}

func New(factory gitprovider.Factory, ins insights.Client, m repo.MappingsRepo, p repo.GitOpsProfilesRepo, pol repo.PoliciesRepo, findings titlisapi.FindingsClient, aiManifest titlisapi.AIManifestClient, udp titlisapi.UDPClient, log *observability.Logger) *Activities {
	return &Activities{Factory: factory, Insights: ins, Mappings: m, Profiles: p, Policies: pol, Findings: findings, AIManifest: aiManifest, UDP: udp, Logger: log}
}

type ResolveMappingInput struct {
	TenantID     int64
	WorkloadName string
	Namespace    string
}

type ResolveMappingOutput struct {
	Found   bool
	Mapping model.WorkloadRepoMapping
}

func (a *Activities) ResolveMapping(ctx context.Context, in ResolveMappingInput) (ResolveMappingOutput, error) {
	mp, err := a.Mappings.Get(ctx, in.TenantID, in.WorkloadName)
	if errors.Is(err, repo.ErrNotFound) {
		return ResolveMappingOutput{Found: false}, nil
	}
	if err != nil {
		return ResolveMappingOutput{}, err
	}
	if !scanner.MatchesWorkload(mp.Definition, in.WorkloadName, in.Namespace) {
		// strict match failed but the cache may still be valid; treat as found.
	}
	return ResolveMappingOutput{Found: true, Mapping: mp}, nil
}

type GetRecommendationInput struct {
	TenantID       int64
	WorkloadUID    string
	DeploymentName string
	Namespace      string
	Cluster        string
	Environment    string
	Criticality    string
	HasDatadog     bool
}

func (a *Activities) GetRecommendation(ctx context.Context, in GetRecommendationInput) (model.HpaRecommendation, error) {
	return a.Insights.GetHpaRecommendation(ctx, insights.RecommendationRequest{
		TenantID:       in.TenantID,
		WorkloadUID:    in.WorkloadUID,
		DeploymentName: in.DeploymentName,
		Namespace:      in.Namespace,
		Cluster:        in.Cluster,
		Environment:    in.Environment,
		Criticality:    in.Criticality,
		HasDatadog:     in.HasDatadog,
	})
}

type FetchManifestInput struct {
	TenantID int64
	RepoURL  string
	Branch   string
	Path     string
}

type FileContent struct {
	Path    string
	SHA     string
	Content []byte
}

func (a *Activities) FetchManifest(ctx context.Context, in FetchManifestInput) (FileContent, error) {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return FileContent{}, err
	}
	f, err := gp.FetchFile(ctx, in.RepoURL, in.Branch, in.Path)
	if err != nil {
		return FileContent{}, err
	}
	return FileContent{Path: f.Path, SHA: f.SHA, Content: f.Content}, nil
}

type ApplyPatchInput struct {
	Current        []byte
	Recommendation model.HpaRecommendation
}

func (a *Activities) ApplyPatch(_ context.Context, in ApplyPatchInput) ([]byte, error) {
	return patch.ApplyHpaRecommendation(in.Current, in.Recommendation)
}

type ValidatePatchInput struct {
	Current []byte
	New     []byte
}

func (a *Activities) ValidatePatch(_ context.Context, in ValidatePatchInput) (bool, error) {
	if err := patch.ValidateNeverReduce(in.Current, in.New); err != nil {
		return false, err
	}
	return true, nil
}

type BranchInput struct {
	TenantID   int64
	RepoURL    string
	BaseBranch string
	NewBranch  string
}

func (a *Activities) CreateBranch(ctx context.Context, in BranchInput) error {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return err
	}
	err = gp.CreateBranch(ctx, in.RepoURL, in.BaseBranch, in.NewBranch)
	if errors.Is(err, gitprovider.ErrConflict) {
		return nil
	}
	return err
}

type CommitInput struct {
	TenantID int64
	RepoURL  string
	Branch   string
	Path     string
	Message  string
	Content  []byte
}

func (a *Activities) CommitFile(ctx context.Context, in CommitInput) error {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return err
	}
	return gp.CommitFile(ctx, in.RepoURL, in.Branch, in.Path, in.Message, in.Content)
}

type OpenPRInput struct {
	TenantID   int64
	RepoURL    string
	BaseBranch string
	HeadBranch string
	Title      string
	Body       string
	Labels     []string
}

type PROutput struct {
	Number  int
	URL     string
	Branch  string
	BaseRef string
	State   string
	Merged  bool
}

func (a *Activities) OpenPR(ctx context.Context, in OpenPRInput) (PROutput, error) {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return PROutput{}, err
	}
	pr, err := gp.OpenPR(ctx, in.RepoURL, in.BaseBranch, in.HeadBranch, in.Title, in.Body, in.Labels)
	if errors.Is(err, gitprovider.ErrConflict) {
		existing, ferr := gp.FindOpenPR(ctx, in.RepoURL, in.HeadBranch)
		if ferr == nil {
			return prOut(existing), nil
		}
	}
	if err != nil {
		return PROutput{}, err
	}
	return prOut(pr), nil
}

func prOut(p gitprovider.PullRequest) PROutput {
	return PROutput{
		Number:  p.Number,
		URL:     p.URL,
		Branch:  p.Branch,
		BaseRef: p.BaseRef,
		State:   p.State,
		Merged:  p.Merged,
	}
}

type FindOpenPRInput struct {
	TenantID   int64
	RepoURL    string
	HeadBranch string
}

func (a *Activities) FindOpenPR(ctx context.Context, in FindOpenPRInput) (PROutput, error) {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return PROutput{}, err
	}
	pr, err := gp.FindOpenPR(ctx, in.RepoURL, in.HeadBranch)
	if errors.Is(err, gitprovider.ErrNotFound) {
		return PROutput{}, gitprovider.ErrNotFound
	}
	if err != nil {
		return PROutput{}, err
	}
	return prOut(pr), nil
}

type MergePRInput struct {
	TenantID int64
	RepoURL  string
	PRNumber int
	Method   string
}

func (a *Activities) MergePR(ctx context.Context, in MergePRInput) error {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return err
	}
	return gp.MergePR(ctx, in.RepoURL, in.PRNumber, in.Method)
}

type WaitChecksInput struct {
	TenantID int64
	RepoURL  string
	PRNumber int
}

type WaitChecksOutput struct {
	Status string
	Reason string
}

func (a *Activities) WaitChecks(ctx context.Context, in WaitChecksInput) (WaitChecksOutput, error) {
	gp, err := a.Factory.ForTenant(ctx, in.TenantID, in.RepoURL)
	if err != nil {
		return WaitChecksOutput{}, err
	}
	res, err := gp.WaitChecks(ctx, in.RepoURL, in.PRNumber, func() bool { return false })
	if err != nil {
		return WaitChecksOutput{}, err
	}
	return WaitChecksOutput{Status: string(res.Status), Reason: res.Reason}, nil
}

type NotifyInput struct {
	EventType string
	TenantID  int64
	Data      map[string]any
}

func (a *Activities) Notify(ctx context.Context, in NotifyInput) error {
	if a.UDP == nil {
		return fmt.Errorf("udp client not configured")
	}
	return a.UDP.Send(ctx, in.EventType, in.TenantID, in.Data)
}

// FindingItem is a workload with an open rule finding.
type FindingItem = titlisapi.WorkloadFinding

type ListFindingsInput struct {
	TenantID int64
	RuleID   string
}

type ListFindingsOutput struct {
	Items []FindingItem
}

func (a *Activities) ListWorkloadsWithFinding(ctx context.Context, in ListFindingsInput) (ListFindingsOutput, error) {
	if a.Findings == nil {
		return ListFindingsOutput{}, fmt.Errorf("findings client not configured")
	}
	items, err := a.Findings.ListWorkloadsWithFinding(ctx, in.TenantID, in.RuleID)
	if err != nil {
		return ListFindingsOutput{}, err
	}
	return ListFindingsOutput{Items: items}, nil
}

type GetPolicyInput struct {
	TenantID int64
	RuleID   string
}

type PolicyOutput struct {
	Mode                 string
	CascadeUpTo          string
	MaxPRsPerDay         int
	AutoMergeMaxDeltaPct int
	RequirePRChecksGreen bool
}

func (a *Activities) GetPolicy(ctx context.Context, in GetPolicyInput) (PolicyOutput, error) {
	p, err := a.Policies.Get(ctx, in.TenantID, in.RuleID, "")
	if errors.Is(err, repo.ErrNotFound) {
		return PolicyOutput{Mode: "disabled", CascadeUpTo: "dev", MaxPRsPerDay: 10}, nil
	}
	if err != nil {
		return PolicyOutput{}, err
	}
	return PolicyOutput{
		Mode:                 string(p.Mode),
		CascadeUpTo:          string(p.CascadeUpTo),
		MaxPRsPerDay:         p.MaxPRsPerDay,
		AutoMergeMaxDeltaPct: p.AutoMergeMaxDeltaPct,
		RequirePRChecksGreen: p.RequirePRChecksGreen,
	}, nil
}

type ListAllFindingsInput struct {
	TenantID int64
	RuleIDs  []string
}

type WorkloadFindings struct {
	WorkloadUID    string
	DeploymentName string
	Namespace      string
	ClusterName    string
	Environment    string
	Criticality    string
	HasDatadog     bool
	Findings       []FindingItem
}

type ListAllFindingsOutput struct {
	Workloads []WorkloadFindings
}

// ListAllFindings fetches open findings for all given ruleIDs and groups them by workload.
func (a *Activities) ListAllFindings(ctx context.Context, in ListAllFindingsInput) (ListAllFindingsOutput, error) {
	if a.Findings == nil {
		return ListAllFindingsOutput{}, fmt.Errorf("findings client not configured")
	}
	items, err := a.Findings.ListWorkloadsWithFindings(ctx, in.TenantID, in.RuleIDs)
	if err != nil {
		return ListAllFindingsOutput{}, err
	}

	// Group by workload UID.
	byUID := make(map[string]*WorkloadFindings)
	order := make([]string, 0)
	for _, f := range items {
		wf, exists := byUID[f.WorkloadUID]
		if !exists {
			wf = &WorkloadFindings{
				WorkloadUID:    f.WorkloadUID,
				DeploymentName: f.DeploymentName,
				Namespace:      f.Namespace,
				ClusterName:    f.ClusterName,
				Environment:    f.Environment,
				Criticality:    f.Criticality,
				HasDatadog:     f.HasDatadog,
			}
			byUID[f.WorkloadUID] = wf
			order = append(order, f.WorkloadUID)
		}
		wf.Findings = append(wf.Findings, f)
	}

	result := make([]WorkloadFindings, 0, len(order))
	for _, uid := range order {
		result = append(result, *byUID[uid])
	}
	return ListAllFindingsOutput{Workloads: result}, nil
}

type GenerateComprehensivePatchInput struct {
	TenantID    int64
	Manifest    string
	Findings    []FindingItem
	WorkloadName string
	Namespace   string
	ClusterName string
	Environment string
	Criticality string
}

type GenerateComprehensivePatchOutput struct {
	CorrectedManifest string
	Applied           []model.AppliedFix
	Skipped           []model.SkippedFix
}

func (a *Activities) GenerateComprehensivePatch(ctx context.Context, in GenerateComprehensivePatchInput) (GenerateComprehensivePatchOutput, error) {
	if a.AIManifest == nil {
		return GenerateComprehensivePatchOutput{}, fmt.Errorf("ai manifest client not configured")
	}

	findings := make([]model.WorkloadFinding, 0, len(in.Findings))
	for _, f := range in.Findings {
		findings = append(findings, model.WorkloadFinding{
			RuleID:   f.RuleID,
			Severity: f.Criticality,
		})
	}

	resp, err := a.AIManifest.GenerateManifestPatch(ctx, model.ManifestPatchRequest{
		TenantID:     in.TenantID,
		Manifest:     in.Manifest,
		Findings:     findings,
		WorkloadName: in.WorkloadName,
		Namespace:    in.Namespace,
		ClusterName:  in.ClusterName,
		Environment:  in.Environment,
		Criticality:  in.Criticality,
	})
	if err != nil {
		return GenerateComprehensivePatchOutput{}, err
	}
	return GenerateComprehensivePatchOutput{
		CorrectedManifest: resp.CorrectedManifest,
		Applied:           resp.Applied,
		Skipped:           resp.Skipped,
	}, nil
}
