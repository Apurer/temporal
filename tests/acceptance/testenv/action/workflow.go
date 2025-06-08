package action

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	AnyWorkflowIdConflictPolicy = stamp.GenChoice("AnyWorkflowIdConflictPolicy",
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
	)
	AnyWorkflowIdReusePolicy = stamp.GenChoice("AnyWorkflowIdReusePolicy",
		enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	)
	AnyRequestID = stamp.GenChoice("AnyRequestID",
		"", // empty request ID
		"custom-request-id",
	)
)

type StartWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecution]
	TaskQueue                *model.TaskQueue `validate:"required"`
	Input                    Payloads
	Identity                 stamp.Gen[string] // defaults to actor's identity
	WorkflowID               stamp.Gen[stamp.ID]
	WorkflowType             stamp.Gen[stamp.ID]
	WorkflowExecutionTimeout stamp.Gen[time.Duration]
	WorkflowRunTimeout       stamp.Gen[time.Duration]
	WorkflowTaskTimeout      stamp.Gen[time.Duration]
	RequestId                stamp.Gen[string] // not an ID as it is empty by default
	WorkflowIdReusePolicy    stamp.Gen[enumspb.WorkflowIdReusePolicy]
	WorkflowIdConflictPolicy stamp.Gen[enumspb.WorkflowIdConflictPolicy]
	VersioningOverride       *VersioningOverride
}

func (w StartWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.StartWorkflowExecutionRequest {
	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  string(w.TaskQueue.GetNamespace().GetID()),
		WorkflowId: string(w.WorkflowID.Next(ctx.AllowRandom())),
		WorkflowType: &commonpb.WorkflowType{
			Name: string(w.WorkflowType.Next(ctx.AllowRandom())),
		},
		TaskQueue: &taskqueue.TaskQueue{
			Name: string(w.TaskQueue.GetID()),
		},
		Input:                    w.Input.Next(ctx),
		WorkflowExecutionTimeout: durationpb.New(w.WorkflowExecutionTimeout.Next(ctx)),
		WorkflowRunTimeout:       durationpb.New(w.WorkflowRunTimeout.Next(ctx)),
		WorkflowTaskTimeout:      durationpb.New(w.WorkflowTaskTimeout.Next(ctx)),
		Identity:                 w.Identity.NextOrDefault(ctx, string(w.GetActor().GetID())),
		RequestId:                w.RequestId.Next(ctx.AllowRandom()),
		WorkflowIdReusePolicy:    w.WorkflowIdReusePolicy.Next(ctx),
		WorkflowIdConflictPolicy: w.WorkflowIdConflictPolicy.Next(ctx),
	}
	if w.VersioningOverride != nil {
		req.VersioningOverride = w.VersioningOverride.Next(ctx)
	}
	return req
}

type GetWorkflowExecutionHistory struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecutionHistory]
	WorkflowExecution *model.WorkflowExecution `validate:"required"`
	// TODO ...
}

func (w GetWorkflowExecutionHistory) Next(ctx stamp.GenContext) *workflowservice.GetWorkflowExecutionHistoryRequest {
	return &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: string(w.WorkflowExecution.GetNamespace().GetID()),
		Execution: w.WorkflowExecution.Next(ctx),
	}
}

type TerminateWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecution]
	WorkflowExecution *model.WorkflowExecution `validate:"required"`
	Identity          stamp.Gen[string]        // defaults to actor's identity
	Reason            stamp.Gen[string]
	// TODO ...
}

func (w TerminateWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.TerminateWorkflowExecutionRequest {
	return &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         string(w.WorkflowExecution.GetNamespace().GetID()),
		WorkflowExecution: w.WorkflowExecution.Next(ctx),
		Identity:          w.Identity.NextOrDefault(ctx, string(w.GetActor().GetID())),
		Reason:            w.Reason.NextOrDefault(ctx, "<reason>"),
	}
}

type DescribeWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecution]
	WorkflowExecution *model.WorkflowExecution `validate:"required"`
}

func (w DescribeWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.DescribeWorkflowExecutionRequest {
	return &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: string(w.WorkflowExecution.GetNamespace().GetID()),
		Execution: w.WorkflowExecution.Next(ctx),
	}
}
