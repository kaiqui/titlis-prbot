package temporal

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/titlis/prbot/internal/model"
	"github.com/titlis/prbot/internal/workflow"
)

// ScheduleManager creates and manages Temporal Schedules for DiscoverDriftWorkflow.
// One schedule per (tenant, ruleID) pair runs the workflow daily at 03:00 UTC.
type ScheduleManager struct {
	C         client.Client
	TaskQueue string
}

func NewScheduleManager(c client.Client, taskQueue string) *ScheduleManager {
	return &ScheduleManager{C: c, TaskQueue: taskQueue}
}

func scheduleID(tenantID int64, ruleID string) string {
	return fmt.Sprintf("discover-drift-%d-%s", tenantID, ruleID)
}

// EnsureSchedule creates or updates the daily schedule for a tenant/rule pair.
// When ruleID is "manifest", it fires DiscoverComprehensiveWorkflow (all rules at once).
// The schedule fires at 03:00 UTC with a 30-minute random jitter to spread load.
func (s *ScheduleManager) EnsureSchedule(ctx context.Context, tenantID int64, ruleID string) error {
	sid := scheduleID(tenantID, ruleID)
	spec := workflow.DiscoverSpec{
		TenantID: tenantID,
		RuleID:   ruleID,
	}
	wfFn := interface{}(workflow.DiscoverDriftWorkflow)
	if ruleID == "manifest" {
		wfFn = workflow.DiscoverComprehensiveWorkflow
	}
	action := &client.ScheduleWorkflowAction{
		Workflow:  wfFn,
		Args:      []interface{}{spec},
		ID:        fmt.Sprintf("discover-%d-%s", tenantID, ruleID),
		TaskQueue: s.TaskQueue,
	}
	schedSpec := client.ScheduleSpec{
		CronExpressions: []string{"0 3 * * *"},
		Jitter:          30 * time.Minute,
	}

	handle := s.C.ScheduleClient().GetHandle(ctx, sid)
	_, err := handle.Describe(ctx)
	if err != nil {
		_, createErr := s.C.ScheduleClient().Create(ctx, client.ScheduleOptions{
			ID:     sid,
			Spec:   schedSpec,
			Action: action,
		})
		return createErr
	}
	return handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Spec = &schedSpec
			input.Description.Schedule.Action = action
			return &client.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
		},
	})
}

// DeleteSchedule removes the schedule for a tenant/rule. Safe to call if not found.
func (s *ScheduleManager) DeleteSchedule(ctx context.Context, tenantID int64, ruleID string) error {
	return s.C.ScheduleClient().GetHandle(ctx, scheduleID(tenantID, ruleID)).Delete(ctx)
}

// TriggerNow fires the schedule immediately (useful for admin "run now" action).
func (s *ScheduleManager) TriggerNow(ctx context.Context, tenantID int64, ruleID string) error {
	return s.C.ScheduleClient().GetHandle(ctx, scheduleID(tenantID, ruleID)).Trigger(
		ctx, client.ScheduleTriggerOptions{},
	)
}

// SyncPolicies upserts/removes schedules for all tenants that have an eligible policy.
// Call this on startup and after any policy change via the HTTP API.
// A global policy (rule_id="*") expands to all ManagedRuleIDs.
func (s *ScheduleManager) SyncPolicies(ctx context.Context, policies []model.AutoRemediationPolicy) error {
	for _, p := range policies {
		ruleIDs := []string{p.RuleID}
		if p.RuleID == model.GlobalPolicyRuleID {
			ruleIDs = model.ManagedRuleIDs
		}
		for _, rid := range ruleIDs {
			if p.Mode == model.PolicyDisabled {
				_ = s.DeleteSchedule(ctx, p.TenantID, rid)
				continue
			}
			if err := s.EnsureSchedule(ctx, p.TenantID, rid); err != nil {
				return fmt.Errorf("sync schedule tenant=%d rule=%s: %w", p.TenantID, rid, err)
			}
		}
	}
	return nil
}

// NoopScheduleManager is used when Temporal is disabled (local dev / tests).
type NoopScheduleManager struct{}

func (NoopScheduleManager) EnsureSchedule(_ context.Context, _ int64, _ string) error {
	return nil
}

func (NoopScheduleManager) DeleteSchedule(_ context.Context, _ int64, _ string) error {
	return nil
}

func (NoopScheduleManager) TriggerNow(_ context.Context, _ int64, _ string) error {
	return nil
}

func (NoopScheduleManager) SyncPolicies(_ context.Context, _ []model.AutoRemediationPolicy) error {
	return nil
}
