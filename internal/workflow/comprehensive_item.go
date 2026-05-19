package workflow

import (
	"fmt"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/model"
)

type ComprehensiveItemSpec struct {
	CampaignID    string
	TenantID      int64
	Workload      activity.WorkloadFindings
	PolicyMode    model.PolicyMode
	CascadeUpTo   model.CascadeUpTo
	MaxDeltaPct   int
	TriggerSource model.TriggerSource
}

// ComprehensiveItemWorkflow processes a single workload:
// 1. Resolve repo mapping
// 2. Fetch manifest for dev (or first available env)
// 3. Generate comprehensive patch via titlis-ai
// 4. Run PromotionWorkflow with the pre-patched content
func ComprehensiveItemWorkflow(ctx workflow.Context, spec ComprehensiveItemSpec) (ItemResult, error) {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())
	var a *activity.Activities

	var mapping activity.ResolveMappingOutput
	if err := workflow.ExecuteActivity(ctx, a.ResolveMapping, activity.ResolveMappingInput{
		TenantID:     spec.TenantID,
		WorkloadName: spec.Workload.DeploymentName,
		Namespace:    spec.Workload.Namespace,
	}).Get(ctx, &mapping); err != nil {
		return ItemResult{Status: model.ItemStatusFailed, Reason: fmt.Sprintf("resolve_mapping: %v", err)}, nil
	}
	if !mapping.Found {
		_ = workflow.ExecuteActivity(ctx, a.Notify, activity.NotifyInput{
			EventType: "finding_opened",
			TenantID:  spec.TenantID,
			Data: map[string]any{
				"rule_id":     "PRBOT-001",
				"workload_id": spec.Workload.WorkloadUID,
				"reason":      "no_mapping",
			},
		}).Get(ctx, nil)
		return ItemResult{Status: model.ItemStatusSkipped, Reason: "no_mapping"}, nil
	}

	paths := mapping.Mapping.Definition.Spec.GitOps.Paths
	if len(paths) == 0 {
		return ItemResult{Status: model.ItemStatusSkipped, Reason: "no_paths"}, nil
	}

	// Fetch the dev manifest (or first available) to send to the AI.
	devPath, ok := paths[string(model.EnvShortDev)]
	if !ok {
		for _, p := range paths {
			devPath = p
			break
		}
	}

	var devManifest activity.FileContent
	if err := workflow.ExecuteActivity(ctx, a.FetchManifest, activity.FetchManifestInput{
		TenantID: spec.TenantID,
		RepoURL:  mapping.Mapping.RepoURL,
		Branch:   devPath.BaseBranch,
		Path:     devPath.Path,
	}).Get(ctx, &devManifest); err != nil {
		return ItemResult{Status: model.ItemStatusFailed, Reason: fmt.Sprintf("fetch_manifest: %v", err)}, nil
	}

	var patch activity.GenerateComprehensivePatchOutput
	if err := workflow.ExecuteActivity(ctx, a.GenerateComprehensivePatch, activity.GenerateComprehensivePatchInput{
		TenantID:     spec.TenantID,
		Manifest:     string(devManifest.Content),
		Findings:     spec.Workload.Findings,
		WorkloadName: spec.Workload.DeploymentName,
		Namespace:    spec.Workload.Namespace,
		ClusterName:  spec.Workload.ClusterName,
		Environment:  spec.Workload.Environment,
		Criticality:  spec.Workload.Criticality,
	}).Get(ctx, &patch); err != nil {
		return ItemResult{Status: model.ItemStatusFailed, Reason: fmt.Sprintf("generate_patch: %v", err)}, nil
	}

	if patch.CorrectedManifest == "" || len(patch.Applied) == 0 {
		return ItemResult{Status: model.ItemStatusSkipped, Reason: "nothing_to_fix"}, nil
	}

	appliedRules := make([]string, 0, len(patch.Applied))
	for _, f := range patch.Applied {
		appliedRules = append(appliedRules, f.RuleID)
	}
	prBody := fmt.Sprintf(
		"Corrige automaticamente os seguintes achados de compliance:\n%s\n\nGerado por titlis-prbot.",
		"- "+strings.Join(appliedRules, "\n- "),
	)

	promo := PromotionSpec{
		CampaignID:        spec.CampaignID,
		TenantID:          spec.TenantID,
		WorkloadUID:       spec.Workload.WorkloadUID,
		WorkloadName:      spec.Workload.DeploymentName,
		Namespace:         spec.Workload.Namespace,
		RepoURL:           mapping.Mapping.RepoURL,
		Paths:             paths,
		PolicyMode:        spec.PolicyMode,
		CascadeUpTo:       spec.CascadeUpTo,
		MaxDeltaPct:       spec.MaxDeltaPct,
		TriggerSource:     spec.TriggerSource,
		PrePatchedContent: []byte(patch.CorrectedManifest),
		CommitMessage:     fmt.Sprintf("titlis-prbot: compliance fix (%s) for %s", strings.Join(appliedRules, ", "), spec.Workload.DeploymentName),
		PRTitle:           fmt.Sprintf("Titlis: correção de compliance para %s", spec.Workload.DeploymentName),
		PRBody:            prBody,
	}
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-promo-%s", spec.CampaignID, spec.Workload.DeploymentName),
	}
	childCtx := workflow.WithChildOptions(ctx, cwo)
	var pr PromotionResult
	if err := workflow.ExecuteChildWorkflow(childCtx, PromotionWorkflow, promo).Get(ctx, &pr); err != nil {
		return ItemResult{Status: model.ItemStatusFailed, Reason: err.Error()}, nil
	}
	return ItemResult{Status: pr.FinalStatus, Promotion: pr, Reason: pr.StoppedReason}, nil
}
