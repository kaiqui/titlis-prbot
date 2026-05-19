package workflow

import (
	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/model"
)

// ReactiveCampaignWorkflow runs a single-item campaign in reaction to a
// rule_failed event. It's a thin wrapper over CampaignWorkflow.
func ReactiveCampaignWorkflow(ctx workflow.Context, spec model.CampaignSpec) (CampaignResult, error) {
	return CampaignWorkflow(ctx, spec)
}
