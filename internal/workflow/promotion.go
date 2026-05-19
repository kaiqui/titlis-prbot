package workflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/titlis/prbot/internal/model"
)

type PromotionSpec struct {
	CampaignID        string
	TenantID          int64
	WorkloadUID       string
	WorkloadName      string
	Namespace         string
	RepoURL           string
	Paths             map[string]model.EnvPathSpec
	Recommendation    model.HpaRecommendation
	PolicyMode        model.PolicyMode
	CascadeUpTo       model.CascadeUpTo
	MaxDeltaPct       int
	TriggerSource     model.TriggerSource
	PrePatchedContent []byte
	CommitMessage     string
	PRTitle           string
	PRBody            string
}

type PromotionResult struct {
	Steps         map[string]EnvDeployResult
	FinalStatus   model.ItemStatus
	StoppedAt     string
	StoppedReason string
}

func envOrdinal(e model.EnvShort) int {
	switch e {
	case model.EnvShortDev:
		return 0
	case model.EnvShortHml:
		return 1
	case model.EnvShortPrd:
		return 2
	}
	return -1
}

func cascadeOrdinal(c model.CascadeUpTo) int {
	switch c {
	case model.CascadeDev:
		return 0
	case model.CascadeHml:
		return 1
	case model.CascadePrd:
		return 2
	}
	return 0
}

type envStepResult struct {
	key    string
	result EnvDeployResult
}

// PromotionWorkflow opens one PR per configured environment in parallel.
// Each PR targets its own base_branch (from EnvPathSpec). prd is never
// auto-merged when TriggerSource != manual.
func PromotionWorkflow(ctx workflow.Context, spec PromotionSpec) (PromotionResult, error) {
	out := PromotionResult{Steps: map[string]EnvDeployResult{}}
	maxOrd := cascadeOrdinal(spec.CascadeUpTo)

	// Collect envs to deploy in order, respecting CascadeUpTo.
	type envTask struct {
		env      model.EnvShort
		envSpec  EnvDeploySpec
	}
	var tasks []envTask
	for _, env := range model.EnvShortOrder() {
		if envOrdinal(env) > maxOrd {
			break
		}
		pathSpec, ok := spec.Paths[string(env)]
		if !ok || pathSpec.Path == "" {
			continue
		}

		effectiveMode := spec.PolicyMode
		if env == model.EnvShortPrd && spec.TriggerSource != model.TriggerManual {
			effectiveMode = model.PolicyOpenPR
		}

		tasks = append(tasks, envTask{
			env: env,
			envSpec: EnvDeploySpec{
				CampaignID:        spec.CampaignID,
				TenantID:          spec.TenantID,
				WorkloadUID:       spec.WorkloadUID,
				WorkloadName:      spec.WorkloadName,
				Namespace:         spec.Namespace,
				RepoURL:           spec.RepoURL,
				BaseBranch:        pathSpec.BaseBranch,
				Env:               env,
				ManifestPath:      pathSpec.Path,
				BranchName:        fmt.Sprintf("titlis/prbot/%s/%s/%s", spec.CampaignID, spec.WorkloadName, env),
				Recommendation:    spec.Recommendation,
				PolicyMode:        effectiveMode,
				MaxDeltaPct:       spec.MaxDeltaPct,
				TriggerSource:     spec.TriggerSource,
				PrePatchedContent: spec.PrePatchedContent,
				CommitMessage:     spec.CommitMessage,
				PRTitle:           spec.PRTitle,
				PRBody:            spec.PRBody,
			},
		})
	}

	if len(tasks) == 0 {
		out.FinalStatus = model.ItemStatusSkipped
		out.StoppedReason = "no_envs"
		return out, nil
	}

	// Fan-out: launch all env deployments in parallel using workflow.Go.
	// Use a buffered workflow channel to collect results safely (Temporal-deterministic).
	resultCh := workflow.NewBufferedChannel(ctx, len(tasks))

	for _, t := range tasks {
		t := t
		workflow.Go(ctx, func(ctx workflow.Context) {
			cwo := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("%s-%s-%s", spec.CampaignID, spec.WorkloadName, t.env),
			}
			childCtx := workflow.WithChildOptions(ctx, cwo)
			var r EnvDeployResult
			if err := workflow.ExecuteChildWorkflow(childCtx, EnvDeployWorkflow, t.envSpec).Get(ctx, &r); err != nil {
				r = EnvDeployResult{Status: model.EnvStepFailed, Reason: err.Error()}
			}
			resultCh.Send(ctx, envStepResult{key: string(t.env), result: r})
		})
	}

	// Collect all results before deriving final status.
	for i := 0; i < len(tasks); i++ {
		var entry envStepResult
		resultCh.Receive(ctx, &entry)
		out.Steps[entry.key] = entry.result
	}

	// Derive aggregate status.
	anyFailed, anyAwaiting, anyPROpen := false, false, false
	for _, r := range out.Steps {
		switch r.Status {
		case model.EnvStepFailed:
			anyFailed = true
		case model.EnvStepAwaitingHuman:
			anyAwaiting = true
		case model.EnvStepPROpen:
			anyPROpen = true
		}
	}
	switch {
	case anyFailed:
		out.FinalStatus = model.ItemStatusFailed
	case anyAwaiting:
		out.FinalStatus = model.ItemStatusAwaitingHuman
	case anyPROpen:
		out.FinalStatus = model.ItemStatusPROpen
	default:
		out.FinalStatus = model.ItemStatusPRMerged
	}
	return out, nil
}
