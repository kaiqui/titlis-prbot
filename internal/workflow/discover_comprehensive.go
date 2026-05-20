package workflow

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/model"
)

// DiscoverComprehensiveWorkflow lists workloads with any open manifest-remediable finding,
// then launches one ComprehensiveItemWorkflow per workload (not per rule).
func DiscoverComprehensiveWorkflow(ctx workflow.Context, spec DiscoverSpec) (DiscoverResult, error) {
	actCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions())
	notifyCtx := workflow.WithActivityOptions(ctx, notifyActivityOptions())
	var a *activity.Activities

	var policy activity.PolicyOutput
	if err := workflow.ExecuteActivity(actCtx, a.GetPolicy, activity.GetPolicyInput{
		TenantID: spec.TenantID,
		RuleID:   model.GlobalPolicyRuleID,
	}).Get(ctx, &policy); err != nil {
		return DiscoverResult{}, fmt.Errorf("get policy: %w", err)
	}
	// Manual triggers (CampaignID set) always run — skip the disabled gate and use open_pr as default.
	if policy.Mode == string(model.PolicyDisabled) {
		if spec.CampaignID == "" {
			return DiscoverResult{}, nil
		}
		policy.Mode = string(model.PolicyOpenPR)
		if policy.MaxPRsPerDay <= 0 {
			policy.MaxPRsPerDay = 10
		}
	}

	var allFindings activity.ListAllFindingsOutput
	if err := workflow.ExecuteActivity(actCtx, a.ListAllFindings, activity.ListAllFindingsInput{
		TenantID: spec.TenantID,
		RuleIDs:  model.ManifestRemediableRuleIDs,
	}).Get(ctx, &allFindings); err != nil {
		return DiscoverResult{}, fmt.Errorf("list all findings: %w", err)
	}
	if len(allFindings.Workloads) == 0 {
		_ = workflow.ExecuteActivity(notifyCtx, a.Notify, activity.NotifyInput{
			EventType: "discovery_completed",
			TenantID:  spec.TenantID,
			Data:      map[string]any{"rule_id": "manifest", "total_findings": 0, "campaigns_started": 0},
		}).Get(ctx, nil)
		return DiscoverResult{}, nil
	}

	maxPRs := policy.MaxPRsPerDay
	if maxPRs <= 0 {
		maxPRs = 10
	}

	totalToLaunch := len(allFindings.Workloads)
	if totalToLaunch > maxPRs {
		totalToLaunch = maxPRs
	}

	if spec.CampaignID != "" {
		_ = workflow.ExecuteActivity(notifyCtx, a.Notify, activity.NotifyInput{
			EventType: "campaign_started",
			TenantID:  spec.TenantID,
			Data: map[string]any{
				"campaign_id":    spec.CampaignID,
				"workflow_id":    spec.CampaignID,
				"rule_id":        "manifest",
				"trigger_source": "manual",
				"total_items":    totalToLaunch,
				"title":          fmt.Sprintf("Compliance manifest fix — %d workloads", totalToLaunch),
			},
		}).Get(ctx, nil)
	}

	launched := 0
	var campaignIDs []string
	runID := workflow.GetInfo(ctx).WorkflowExecution.RunID

	for _, wf := range allFindings.Workloads {
		if launched >= maxPRs {
			break
		}
		itemID := fmt.Sprintf("comp-%d-%s-%s", spec.TenantID, wf.WorkloadUID, runID[:8])
		triggerSrc := model.TriggerSchedule
		if spec.CampaignID != "" {
			triggerSrc = model.TriggerManual
		}
		itemSpec := ComprehensiveItemSpec{
			CampaignID:       itemID,
			ParentCampaignID: spec.CampaignID,
			TenantID:         spec.TenantID,
			Workload:         wf,
			PolicyMode:       model.PolicyMode(policy.Mode),
			CascadeUpTo:      model.CascadeUpTo(policy.CascadeUpTo),
			MaxDeltaPct:      policy.AutoMergeMaxDeltaPct,
			TriggerSource:    triggerSrc,
		}
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:        itemID,
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		}
		_ = workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), ComprehensiveItemWorkflow, itemSpec)
		campaignIDs = append(campaignIDs, itemID)
		launched++
	}

	if spec.CampaignID != "" {
		_ = workflow.ExecuteActivity(notifyCtx, a.Notify, activity.NotifyInput{
			EventType: "campaign_completed",
			TenantID:  spec.TenantID,
			Data: map[string]any{
				"campaign_id": spec.CampaignID,
				"status":      "RUNNING",
			},
		}).Get(ctx, nil)
	}

	_ = workflow.ExecuteActivity(notifyCtx, a.Notify, activity.NotifyInput{
		EventType: "discovery_completed",
		TenantID:  spec.TenantID,
		Data: map[string]any{
			"rule_id":           "manifest",
			"total_findings":    len(allFindings.Workloads),
			"campaigns_started": launched,
		},
	}).Get(ctx, nil)

	return DiscoverResult{Found: len(allFindings.Workloads), Campaigns: campaignIDs}, nil
}
