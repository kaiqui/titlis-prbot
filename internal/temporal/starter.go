package temporal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.temporal.io/sdk/client"

	thttp "github.com/titlis/prbot/internal/http"
	"github.com/titlis/prbot/internal/model"
	"github.com/titlis/prbot/internal/workflow"
)

// Starter is the production CampaignStarter — talks to Temporal.
type Starter struct {
	C         client.Client
	TaskQueue string
}

func NewStarter(c client.Client, taskQueue string) *Starter {
	return &Starter{C: c, TaskQueue: taskQueue}
}

func (s *Starter) Start(ctx context.Context, spec model.CampaignSpec) (string, string, error) {
	opts := client.StartWorkflowOptions{
		ID:        spec.CampaignID,
		TaskQueue: s.TaskQueue,
	}
	we, err := s.C.ExecuteWorkflow(ctx, opts, workflow.CampaignWorkflow, spec)
	if err != nil {
		return "", "", err
	}
	return we.GetID(), we.GetRunID(), nil
}

func (s *Starter) Status(ctx context.Context, campaignID string) (thttp.CampaignStatusSnapshot, error) {
	resp, err := s.C.QueryWorkflow(ctx, campaignID, "", workflow.QueryStatus)
	if err != nil {
		return thttp.CampaignStatusSnapshot{}, err
	}
	var state workflow.CampaignResult
	if err := resp.Get(&state); err != nil {
		return thttp.CampaignStatusSnapshot{}, err
	}
	return thttp.CampaignStatusSnapshot{
		CampaignID: campaignID,
		State: map[string]interface{}{
			"total":     state.Total,
			"succeeded": state.Succeeded,
			"failed":    state.Failed,
			"skipped":   state.Skipped,
			"awaiting":  state.Awaiting,
			"cancelled": state.Cancelled,
		},
	}, nil
}

func (s *Starter) Cancel(ctx context.Context, campaignID string) error {
	return s.C.SignalWorkflow(ctx, campaignID, "", workflow.SignalCancelCampaign, nil)
}

func (s *Starter) StartManifest(ctx context.Context, tenantID int64, campaignID string) (string, string, error) {
	opts := client.StartWorkflowOptions{
		ID:        campaignID,
		TaskQueue: s.TaskQueue,
	}
	spec := workflow.DiscoverSpec{
		TenantID:   tenantID,
		RuleID:     "manifest",
		CampaignID: campaignID,
	}
	we, err := s.C.ExecuteWorkflow(ctx, opts, workflow.DiscoverComprehensiveWorkflow, spec)
	if err != nil {
		return "", "", err
	}
	return we.GetID(), we.GetRunID(), nil
}

// MemoryStarter implements CampaignStarter without a Temporal cluster.
// It just records start calls — useful for local dev and tests.
type MemoryStarter struct {
	mu        sync.Mutex
	campaigns map[string]model.CampaignSpec
}

func NewMemoryStarter() *MemoryStarter {
	return &MemoryStarter{campaigns: map[string]model.CampaignSpec{}}
}

func (m *MemoryStarter) Start(_ context.Context, spec model.CampaignSpec) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.campaigns[spec.CampaignID] = spec
	return spec.CampaignID, "mem-run", nil
}

func (m *MemoryStarter) Status(_ context.Context, campaignID string) (thttp.CampaignStatusSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	spec, ok := m.campaigns[campaignID]
	if !ok {
		return thttp.CampaignStatusSnapshot{}, errors.New("not found")
	}
	return thttp.CampaignStatusSnapshot{
		CampaignID: campaignID,
		State: map[string]interface{}{
			"total":  len(spec.Items),
			"status": "memory_only",
			"trigger_source": fmt.Sprintf("%s", spec.TriggerSource),
		},
	}, nil
}

func (m *MemoryStarter) Cancel(_ context.Context, _ string) error {
	return nil
}

func (m *MemoryStarter) StartManifest(_ context.Context, tenantID int64, campaignID string) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.campaigns[campaignID] = model.CampaignSpec{
		CampaignID:    campaignID,
		TenantID:      tenantID,
		TriggerSource: model.TriggerManual,
		RuleID:        "manifest",
	}
	return campaignID, "mem-run", nil
}
