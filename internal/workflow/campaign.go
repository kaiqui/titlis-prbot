package workflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/model"
)

const (
	SignalCancelCampaign = "CancelCampaign"
	SignalPRMerged       = "PRMergedExternally"
	SignalPRClosed       = "PRClosedExternally"
	QueryStatus          = "GetStatus"
)

type CampaignResult struct {
	Total     int
	Succeeded int
	Failed    int
	Skipped   int
	Awaiting  int
	Items     map[string]ItemResult
	Cancelled bool
}

func CampaignWorkflow(ctx workflow.Context, spec model.CampaignSpec) (CampaignResult, error) {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())
	var a *activity.Activities

	state := CampaignResult{
		Total: len(spec.Items),
		Items: map[string]ItemResult{},
	}

	// Register query handler for live status.
	_ = workflow.SetQueryHandler(ctx, QueryStatus, func() (CampaignResult, error) {
		return state, nil
	})

	// Notify titlis-api that the campaign started so it can be tracked in pr_campaigns.
	_ = workflow.ExecuteActivity(ctx, a.Notify, activity.NotifyInput{
		EventType: "campaign_started",
		TenantID:  spec.TenantID,
		Data: map[string]any{
			"campaign_id":    spec.CampaignID,
			"workflow_id":    spec.CampaignID,
			"trigger_source": string(spec.TriggerSource),
			"rule_id":        spec.RuleID,
			"title":          spec.Title,
			"description":    spec.Description,
			"total_items":    len(spec.Items),
			"actor_email":    spec.ActorEmail,
		},
	}).Get(ctx, nil)

	cancelled := false
	cancelChan := workflow.GetSignalChannel(ctx, SignalCancelCampaign)
	workflow.Go(ctx, func(ctx workflow.Context) {
		var v any
		cancelChan.Receive(ctx, &v)
		cancelled = true
	})

	for _, item := range spec.Items {
		if cancelled {
			state.Cancelled = true
			break
		}
		itemSpec := ItemSpec{
			CampaignID:    spec.CampaignID,
			TenantID:      spec.TenantID,
			Item:          item,
			PolicyMode:    spec.PolicyMode,
			CascadeUpTo:   spec.CascadeUpTo,
			MaxDeltaPct:   spec.MaxDeltaPct,
			TriggerSource: spec.TriggerSource,
		}
		cwo := workflow.ChildWorkflowOptions{WorkflowID: fmt.Sprintf("%s-item-%s", spec.CampaignID, item.DeploymentName)}
		childCtx := workflow.WithChildOptions(ctx, cwo)
		var res ItemResult
		if err := workflow.ExecuteChildWorkflow(childCtx, ItemWorkflow, itemSpec).Get(ctx, &res); err != nil {
			res = ItemResult{Status: model.ItemStatusFailed, Reason: err.Error()}
		}
		state.Items[item.DeploymentName] = res
		switch res.Status {
		case model.ItemStatusPRMerged, model.ItemStatusPROpen:
			state.Succeeded++
		case model.ItemStatusFailed:
			state.Failed++
		case model.ItemStatusSkipped:
			state.Skipped++
		case model.ItemStatusAwaitingHuman:
			state.Awaiting++
		}
	}

	finalStatus := "COMPLETED"
	if state.Cancelled {
		finalStatus = "CANCELLED"
	}
	_ = workflow.ExecuteActivity(ctx, a.Notify, activity.NotifyInput{
		EventType: "campaign_completed",
		TenantID:  spec.TenantID,
		Data: map[string]any{
			"campaign_id":     spec.CampaignID,
			"status":          finalStatus,
			"total_items":     state.Total,
			"succeeded_items": state.Succeeded,
			"failed_items":    state.Failed,
			"skipped_items":   state.Skipped,
		},
	}).Get(ctx, nil)

	return state, nil
}
