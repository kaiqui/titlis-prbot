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
	var a *activity.Activities

	var policy activity.PolicyOutput
	if err := workflow.ExecuteActivity(actCtx, a.GetPolicy, activity.GetPolicyInput{
		TenantID: spec.TenantID,
		RuleID:   model.GlobalPolicyRuleID,
	}).Get(ctx, &policy); err != nil {
		return DiscoverResult{}, fmt.Errorf("get policy: %w", err)
	}
	if policy.Mode == string(model.PolicyDisabled) {
		return DiscoverResult{}, nil
	}

	var allFindings activity.ListAllFindingsOutput
	if err := workflow.ExecuteActivity(actCtx, a.ListAllFindings, activity.ListAllFindingsInput{
		TenantID: spec.TenantID,
		RuleIDs:  model.ManifestRemediableRuleIDs,
	}).Get(ctx, &allFindings); err != nil {
		return DiscoverResult{}, fmt.Errorf("list all findings: %w", err)
	}
	if len(allFindings.Workloads) == 0 {
		_ = workflow.ExecuteActivity(actCtx, a.Notify, activity.NotifyInput{
			EventType: "discovery_completed",
			TenantID:  spec.TenantID,
			Data:      map[string]any{"rule_id": "manifest", "found": 0},
		}).Get(ctx, nil)
		return DiscoverResult{}, nil
	}

	maxPRs := policy.MaxPRsPerDay
	if maxPRs <= 0 {
		maxPRs = 10
	}

	launched := 0
	var campaignIDs []string
	runID := workflow.GetInfo(ctx).WorkflowExecution.RunID

	for _, wf := range allFindings.Workloads {
		if launched >= maxPRs {
			break
		}
		itemID := fmt.Sprintf("comp-%d-%s-%s", spec.TenantID, wf.WorkloadUID, runID[:8])
		itemSpec := ComprehensiveItemSpec{
			CampaignID:    itemID,
			TenantID:      spec.TenantID,
			Workload:      wf,
			PolicyMode:    model.PolicyMode(policy.Mode),
			CascadeUpTo:   model.CascadeUpTo(policy.CascadeUpTo),
			MaxDeltaPct:   policy.AutoMergeMaxDeltaPct,
			TriggerSource: model.TriggerSchedule,
		}
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:        itemID,
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		}
		_ = workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, cwo), ComprehensiveItemWorkflow, itemSpec)
		campaignIDs = append(campaignIDs, itemID)
		launched++
	}

	_ = workflow.ExecuteActivity(actCtx, a.Notify, activity.NotifyInput{
		EventType: "discovery_completed",
		TenantID:  spec.TenantID,
		Data: map[string]any{
			"rule_id":   "manifest",
			"found":     len(allFindings.Workloads),
			"launched":  launched,
			"campaigns": len(campaignIDs),
		},
	}).Get(ctx, nil)

	return DiscoverResult{Found: len(allFindings.Workloads), Campaigns: campaignIDs}, nil
}
