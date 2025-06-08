package action

import (
	"encoding/base64"

	commandpb "go.temporal.io/api/command/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type PollWorkflowTaskQueue struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	TaskQueue *model.TaskQueue `validate:"required"`
	Version   *model.WorkerDeploymentVersion
}

func (t PollWorkflowTaskQueue) Next(_ stamp.GenContext) *workflowservice.PollWorkflowTaskQueueRequest {
	var deploymentOptions *deploymentpb.WorkerDeploymentOptions
	if t.Version != nil {
		deploymentOptions = &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       string(t.Version.GetDeployment().GetID()),
			BuildId:              string(t.Version.GetID()),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED, // TODO?
		}
	}

	return &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: string(t.TaskQueue.GetNamespace().GetID()),
		TaskQueue: &taskqueue.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			Name: string(t.TaskQueue.GetID()),
		},
		Identity:          string(t.GetActor().GetID()),
		DeploymentOptions: deploymentOptions,
	}
}

type WorkflowTaskResponse interface {
	Complete() *RespondWorkflowTaskCompleted
	Fail() *FailWorkflowTask
}

type RespondWorkflowTaskCompleted struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	WorkflowTask       *model.WorkflowTask `validate:"required"`
	Commands           stamp.ListGen[*commandpb.Command]
	Messages           stamp.ListGen[*protocolpb.Message]
	Version            *model.WorkerDeploymentVersion
	VersioningBehavior stamp.Gen[enumspb.VersioningBehavior]
	// TODO ...
}

func (t RespondWorkflowTaskCompleted) Next(ctx stamp.GenContext) *workflowservice.RespondWorkflowTaskCompletedRequest {
	if t.WorkflowTask.Token == "" {
		panic("WorkflowTask Token is empty")
	}
	tokenBytes, err := base64.StdEncoding.DecodeString(t.WorkflowTask.Token)
	if err != nil {
		panic(err)
	}

	var commands []*commandpb.Command
	for _, cmd := range t.Commands {
		commands = append(commands, cmd.Next(ctx))
	}

	// TODO: randomize order
	var messages []*protocolpb.Message
	for _, msg := range t.Messages {
		m := msg.Next(ctx)
		messages = append(messages, m)

		// auto-generate a ProtocolCommand for each Message
		// TODO: allow to turn this off
		commands = append(commands, ProtocolCommand{MessageID: m.Id}.Next(ctx))
	}

	var deploymentOptions *deploymentpb.WorkerDeploymentOptions
	if t.Version != nil {
		deploymentOptions = &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       string(t.Version.GetDeployment().GetID()),
			BuildId:              string(t.Version.GetID()),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED, // TODO?
		}
	}

	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:          string(t.WorkflowTask.GetNamespace().GetID()),
		Identity:           string(t.GetActor().GetID()),
		TaskToken:          tokenBytes,
		Commands:           commands,
		Messages:           messages,
		VersioningBehavior: t.VersioningBehavior.Next(ctx),
		DeploymentOptions:  deploymentOptions,
	}
}

type FailWorkflowTask struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	WorkflowTask *model.WorkflowTask `validate:"required"`
	// TODO
}

func (t FailWorkflowTask) Next(ctx stamp.GenContext) *workflowservice.RespondWorkflowTaskFailedRequest {
	return &workflowservice.RespondWorkflowTaskFailedRequest{
		// TODO
	}
}
