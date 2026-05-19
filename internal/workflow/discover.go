package workflow

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/model"
)

type DiscoverSpec struct {
	TenantID int64
	RuleID   string
}

type DiscoverResult struct {
	Found     int
	Campaigns []string
}

// DiscoverDriftWorkflow lists workloads with open findings for the given rule,
// groups them by repository, and launches a CampaignWorkflow per group —
// respecting max_prs_per_day from the tenant's auto-remediation policy.
func DiscoverDriftWorkflow(ctx workflow.Context, spec DiscoverSpec) (DiscoverResult, error) {
	actCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions())
	var a *activity.Activities

	// 1. Fetch policy to check if discovery is enabled.
	var policy activity.PolicyOutput
	if err := workflow.ExecuteActivity(actCtx, a.GetPolicy, activity.GetPolicyInput{
		TenantID: spec.TenantID,
		RuleID:   spec.RuleID,
	}).Get(ctx, &policy); err != nil {
		return DiscoverResult{}, fmt.Errorf("get policy: %w", err)
	}
	if policy.Mode == string(model.PolicyDisabled) {
		return DiscoverResult{}, nil
	}

	// 2. List workloads with open findings from titlis-api.
	var findings activity.ListFindingsOutput
	if err := workflow.ExecuteActivity(actCtx, a.ListWorkloadsWithFinding, activity.ListFindingsInput{
		TenantID: spec.TenantID,
		RuleID:   spec.RuleID,
	}).Get(ctx, &findings); err != nil {
		return DiscoverResult{}, fmt.Errorf("list findings: %w", err)
	}
	if len(findings.Items) == 0 {
		_ = workflow.ExecuteActivity(actCtx, a.Notify, activity.NotifyInput{
			EventType: "discovery_completed",
			TenantID:  spec.TenantID,
			Data:      map[string]any{"rule_id": spec.RuleID, "found": 0},
		}).Get(ctx, nil)
		return DiscoverResult{}, nil
	}

	// 3. Resolve mappings once per workload; group resolved items by repository URL.
	repoGroups := make(map[string][]model.CampaignItem)
	for _, f := range findings.Items {
		var out activity.ResolveMappingOutput
		_ = workflow.ExecuteActivity(actCtx, a.ResolveMapping, activity.ResolveMappingInput{
			TenantID:     spec.TenantID,
			WorkloadName: f.DeploymentName,
			Namespace:    f.Namespace,
		}).Get(ctx, &out)
		if !out.Found {
			_ = workflow.ExecuteActivity(actCtx, a.Notify, activity.NotifyInput{
				EventType: "finding_opened",
				TenantID:  spec.TenantID,
				Data: map[string]any{
					"rule_id":   "PRBOT-001",
					"workload":  f.DeploymentName,
					"namespace": f.Namespace,
					"cluster":   f.ClusterName,
					"reason":    "no_mapping",
				},
			}).Get(ctx, nil)
			continue
		}
		rURL := out.Mapping.RepoURL
		repoGroups[rURL] = append(repoGroups[rURL], model.CampaignItem{
			ItemID:         fmt.Sprintf("disc-%s-%s", spec.RuleID, f.WorkloadUID),
			WorkloadUID:    f.WorkloadUID,
			DeploymentName: f.DeploymentName,
			Namespace:      f.Namespace,
			ClusterName:    f.ClusterName,
			RepoURL:        rURL,
			Paths:          out.Mapping.Definition.Spec.GitOps.Paths,
		})
	}

	// 4. Launch one CampaignWorkflow per repo group, honouring max_prs_per_day.
	maxPRs := policy.MaxPRsPerDay
	if maxPRs <= 0 {
		maxPRs = 10
	}
	launched := 0
	var campaignIDs []string
	runID := workflow.GetInfo(ctx).WorkflowExecution.RunID

	for repoURL, items := range repoGroups {
		if launched >= maxPRs {
			break
		}
		// Slice to respect the per-day cap.
		remaining := maxPRs - launched
		if len(items) > remaining {
			items = items[:remaining]
		}

		campaignID := fmt.Sprintf("disc-%d-%s-%s", spec.TenantID, spec.RuleID, runID[:8])
		campaignSpec := model.CampaignSpec{
			CampaignID:     campaignID,
			TenantID:       spec.TenantID,
			TriggerSource:  model.TriggerSchedule,
			RuleID:         spec.RuleID,
			Title:          fmt.Sprintf("Auto: HPA drift fix (%s) — %s", spec.RuleID, repoURL),
			PolicyMode:     model.PolicyMode(policy.Mode),
			CascadeUpTo:    model.CascadeUpTo(policy.CascadeUpTo),
			MaxDeltaPct:    policy.AutoMergeMaxDeltaPct,
			Items:          items,
			IdempotencyKey: campaignID,
		}
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:        campaignID,
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		}
		_ = workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), CampaignWorkflow, campaignSpec)
		campaignIDs = append(campaignIDs, campaignID)
		launched += len(items)
	}

	_ = workflow.ExecuteActivity(actCtx, a.Notify, activity.NotifyInput{
		EventType: "discovery_completed",
		TenantID:  spec.TenantID,
		Data: map[string]any{
			"rule_id":   spec.RuleID,
			"found":     len(findings.Items),
			"launched":  launched,
			"campaigns": len(campaignIDs),
		},
	}).Get(ctx, nil)

	return DiscoverResult{Found: len(findings.Items), Campaigns: campaignIDs}, nil
}
