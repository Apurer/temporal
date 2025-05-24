package acceptance

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	. "go.temporal.io/server/common/testing/stamp"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkerVersioning(t *testing.T) {
	ts := NewTestSuite(t)

	// TODO: crossTq
	t.Run("Start child workflow", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			c, ns, tq, usr, wkr := ts.NewWorkflowStack(s)
			c.SetNamespaceDynamicConfig(ns, dynamicconfig.EnableDeploymentVersions, true)
			dpl := Act(c, CreateWorkerDeployment{Namespace: ns})
			dvrA := Act(c, CreateWorkerDeploymentVersion{Deployment: dpl, Name: GenJust[ID]("A")})
			dvrB := Act(c, CreateWorkerDeploymentVersion{Deployment: dpl, Name: GenJust[ID]("B")})

			// Make version "A" current
			wftA := ActAsync(wkr, PollWorkflowTaskQueue{TaskQueue: tq, Version: dvrA})
			s.Await(&dvrA.IsCurrent, OnRetry(func() {
				Act(c, SetWorkerDeploymentCurrentVersion{Version: dvrA}, IgnoreErr())
			}))

			// Start workflow
			Act(usr, StartWorkflowExecution{TaskQueue: tq})
			wftA.Await() // TODO: why does this time out?

			// Make version "B" current
			wftB := ActAsync(wkr, PollWorkflowTaskQueue{TaskQueue: tq, Version: dvrB})
			s.Await(&dvrB.IsCurrent, OnRetry(func() {
				Act(c, SetWorkerDeploymentCurrentVersion{Version: dvrB}, IgnoreErr())
			}))
			wftB.Cancel()

			// Start child workflow
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask:       wftA.Await(),
				Commands:           GenList(StartChildWorkflowExecutionCommand{TaskQueue: tq}),
				Version:            dvrA,
				VersioningBehavior: AnyVersioningBehavior,
			})

			// Complete child workflow
			cwft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq, Version: dvrA})
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask: cwft,
				Commands:     GenList(CompleteWorkflowExecutionCommand{}),
			})
		})
	})
}
