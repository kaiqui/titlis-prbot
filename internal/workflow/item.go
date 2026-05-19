package workflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/model"
)

type ItemSpec struct {
	CampaignID     string
	TenantID       int64
	Item           model.CampaignItem
	PolicyMode     model.PolicyMode
	CascadeUpTo    model.CascadeUpTo
	MaxDeltaPct    int
	TriggerSource  model.TriggerSource
}

type ItemResult struct {
	Status      model.ItemStatus
	Reason      string
	Promotion   PromotionResult
}

func ItemWorkflow(ctx workflow.Context, spec ItemSpec) (ItemResult, error) {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())
	var a *activity.Activities

	var mapping activity.ResolveMappingOutput
	if err := workflow.ExecuteActivity(ctx, a.ResolveMapping, activity.ResolveMappingInput{
		TenantID: spec.TenantID, WorkloadName: spec.Item.DeploymentName, Namespace: spec.Item.Namespace,
	}).Get(ctx, &mapping); err != nil {
		return ItemResult{Status: model.ItemStatusFailed, Reason: fmt.Sprintf("resolve_mapping: %v", err)}, nil
	}
	if !mapping.Found {
		_ = workflow.ExecuteActivity(ctx, a.Notify, activity.NotifyInput{
			EventType: "finding_opened",
			TenantID:  spec.TenantID,
			Data: map[string]any{
				"rule_id":     "PRBOT-001",
				"workload_id": spec.Item.WorkloadUID,
				"reason":      "no_mapping",
			},
		}).Get(ctx, nil)
		return ItemResult{Status: model.ItemStatusSkipped, Reason: "no_mapping"}, nil
	}

	// Use Paths from the item (resolved by orchestrator) when present, else from definition.
	paths := spec.Item.Paths
	if len(paths) == 0 && mapping.Mapping.Definition.Spec.GitOps.Paths != nil {
		paths = mapping.Mapping.Definition.Spec.GitOps.Paths
	}
	if len(paths) == 0 {
		return ItemResult{Status: model.ItemStatusSkipped, Reason: "no_paths"}, nil
	}

	// Fetch HPA recommendation when not pre-populated (discovery path).
	if spec.Item.Recommendation.Source == "" {
		var rec model.HpaRecommendation
		_ = workflow.ExecuteActivity(ctx, a.GetRecommendation, activity.GetRecommendationInput{
			TenantID:       spec.TenantID,
			WorkloadUID:    spec.Item.WorkloadUID,
			DeploymentName: spec.Item.DeploymentName,
			Namespace:      spec.Item.Namespace,
			Cluster:        spec.Item.ClusterName,
		}).Get(ctx, &rec)
		if rec.Source != "" {
			spec.Item.Recommendation = rec
		}
	}

	if spec.Item.Recommendation.Source == "skipped" || spec.Item.Recommendation.Source == "" {
		return ItemResult{Status: model.ItemStatusSkipped, Reason: "no_recommendation"}, nil
	}

	repoURL := spec.Item.RepoURL
	if repoURL == "" {
		repoURL = mapping.Mapping.RepoURL
	}

	promo := PromotionSpec{
		CampaignID:     spec.CampaignID,
		TenantID:       spec.TenantID,
		WorkloadUID:    spec.Item.WorkloadUID,
		WorkloadName:   spec.Item.DeploymentName,
		Namespace:      spec.Item.Namespace,
		RepoURL:        repoURL,
		Paths:          paths,
		Recommendation: spec.Item.Recommendation,
		PolicyMode:     spec.PolicyMode,
		CascadeUpTo:    spec.CascadeUpTo,
		MaxDeltaPct:    spec.MaxDeltaPct,
		TriggerSource:  spec.TriggerSource,
	}
	cwo := workflow.ChildWorkflowOptions{WorkflowID: fmt.Sprintf("%s-promo-%s", spec.CampaignID, spec.Item.DeploymentName)}
	childCtx := workflow.WithChildOptions(ctx, cwo)
	var pr PromotionResult
	if err := workflow.ExecuteChildWorkflow(childCtx, PromotionWorkflow, promo).Get(ctx, &pr); err != nil {
		return ItemResult{Status: model.ItemStatusFailed, Reason: err.Error()}, nil
	}
	return ItemResult{Status: pr.FinalStatus, Promotion: pr, Reason: pr.StoppedReason}, nil
}
