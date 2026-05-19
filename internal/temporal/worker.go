package temporal

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/titlis/prbot/internal/activity"
	"github.com/titlis/prbot/internal/workflow"
)

func NewWorker(c client.Client, taskQueue string, a *activity.Activities, opts worker.Options) worker.Worker {
	w := worker.New(c, taskQueue, opts)
	w.RegisterWorkflow(workflow.CampaignWorkflow)
	w.RegisterWorkflow(workflow.ItemWorkflow)
	w.RegisterWorkflow(workflow.PromotionWorkflow)
	w.RegisterWorkflow(workflow.EnvDeployWorkflow)
	w.RegisterWorkflow(workflow.DiscoverDriftWorkflow)
	w.RegisterWorkflow(workflow.DiscoverComprehensiveWorkflow)
	w.RegisterWorkflow(workflow.ComprehensiveItemWorkflow)
	w.RegisterWorkflow(workflow.ReactiveCampaignWorkflow)
	w.RegisterActivity(a.ResolveMapping)
	w.RegisterActivity(a.GetRecommendation)
	w.RegisterActivity(a.FetchManifest)
	w.RegisterActivity(a.ApplyPatch)
	w.RegisterActivity(a.ValidatePatch)
	w.RegisterActivity(a.CreateBranch)
	w.RegisterActivity(a.CommitFile)
	w.RegisterActivity(a.OpenPR)
	w.RegisterActivity(a.MergePR)
	w.RegisterActivity(a.WaitChecks)
	w.RegisterActivity(a.Notify)
	w.RegisterActivity(a.ListWorkloadsWithFinding)
	w.RegisterActivity(a.ListAllFindings)
	w.RegisterActivity(a.GenerateComprehensivePatch)
	w.RegisterActivity(a.GetPolicy)
	w.RegisterActivity(a.FindOpenPR)
	return w
}
