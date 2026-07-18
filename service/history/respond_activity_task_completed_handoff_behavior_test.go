package history

import (
	"context"
	"testing"

	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
)

type activityCompletionHandoffTokenCaptureEngine struct {
	historyi.Engine

	request   *historyservice.RespondActivityTaskCompletedRequest
	taskToken *tokenspb.Task
}

func (e *activityCompletionHandoffTokenCaptureEngine) RespondActivityTaskCompletedWithTaskToken(
	_ context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	taskToken *tokenspb.Task,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	e.request = request
	e.taskToken = taskToken
	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

type activityCompletionHandoffRunIDConsistencyChecker struct {
	api.WorkflowConsistencyChecker

	workflowLease       api.WorkflowLease
	currentRunID        string
	currentRunIDCalls   int
	workflowKeyForLease definition.WorkflowKey
}

func (c *activityCompletionHandoffRunIDConsistencyChecker) GetWorkflowLease(
	_ context.Context,
	_ *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	_ locks.Priority,
) (api.WorkflowLease, error) {
	c.workflowKeyForLease = workflowKey
	return c.workflowLease, nil
}

func (c *activityCompletionHandoffRunIDConsistencyChecker) GetCurrentWorkflowRunID(
	context.Context,
	string,
	string,
	locks.Priority,
) (string, error) {
	c.currentRunIDCalls++
	return c.currentRunID, nil
}

func TestRespondActivityTaskCompletedPassesDecodedTokenToHistoryEngine(t *testing.T) {
	request, wantToken, err := newActivityCompletionHandoffRequest(0)
	if err != nil {
		t.Fatal(err)
	}
	engine := &activityCompletionHandoffTokenCaptureEngine{}
	shardContext := &activityCompletionHandoffShardContext{engine: engine}
	handler := &Handler{
		tokenSerializer: tasktoken.NewSerializer(),
		controller:      &activityCompletionHandoffController{shardContext: shardContext},
	}

	response, err := handler.RespondActivityTaskCompleted(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response == nil {
		t.Fatal("RespondActivityTaskCompleted returned a nil response")
	}
	if engine.request != request {
		t.Fatalf("engine request = %p, want %p", engine.request, request)
	}
	if engine.taskToken == wantToken {
		t.Fatal("engine received the token instance used to create the serialized request, not the handler-decoded token")
	}
	if !wantToken.Equal(engine.taskToken) {
		t.Fatalf("engine task token = %v, want %v", engine.taskToken, wantToken)
	}
}

func TestRespondActivityTaskCompletedResolvesMissingRunIDThroughProductionHandoff(t *testing.T) {
	handler, mutableState := newActivityCompletionHandoffHandler()
	controller := handler.controller.(*activityCompletionHandoffController)
	shardContext := controller.shardContext.(*activityCompletionHandoffShardContext)
	historyEngine := shardContext.engine.(*historyEngineImpl)
	workflowConsistencyChecker := &activityCompletionHandoffRunIDConsistencyChecker{
		workflowLease: api.NewWorkflowLease(
			&activityCompletionHandoffWorkflowContext{},
			func(error) {},
			mutableState,
		),
		currentRunID: tests.RunID,
	}
	historyEngine.workflowConsistencyChecker = workflowConsistencyChecker

	request, taskToken, err := newActivityCompletionHandoffRequest(0)
	if err != nil {
		t.Fatal(err)
	}
	taskToken.RunId = ""
	request.CompleteRequest.TaskToken, err = tasktoken.NewSerializer().Serialize(taskToken)
	if err != nil {
		t.Fatal(err)
	}

	response, err := handler.RespondActivityTaskCompleted(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response == nil {
		t.Fatal("RespondActivityTaskCompleted returned a nil response")
	}
	if workflowConsistencyChecker.currentRunIDCalls != 1 {
		t.Fatalf("GetCurrentWorkflowRunID calls = %d, want 1", workflowConsistencyChecker.currentRunIDCalls)
	}
	if workflowConsistencyChecker.workflowKeyForLease.RunID != tests.RunID {
		t.Fatalf("workflow lease run ID = %q, want %q", workflowConsistencyChecker.workflowKeyForLease.RunID, tests.RunID)
	}
	if mutableState.completedScheduledEventID != taskToken.GetScheduledEventId() {
		t.Fatalf("completed scheduled event ID = %d, want %d", mutableState.completedScheduledEventID, taskToken.GetScheduledEventId())
	}
}
