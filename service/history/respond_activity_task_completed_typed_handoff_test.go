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

type activityCompletionTypedHandoffCompatibilityEngine struct {
	historyi.Engine

	rawCalls   int
	typedCalls int
	request    *historyservice.RespondActivityTaskCompletedRequest
	taskToken  *tokenspb.Task
}

func (e *activityCompletionTypedHandoffCompatibilityEngine) RespondActivityTaskCompleted(
	_ context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	e.rawCalls++
	e.request = request
	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

func (e *activityCompletionTypedHandoffCompatibilityEngine) RespondActivityTaskCompletedWithTaskToken(
	_ context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	taskToken *tokenspb.Task,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	e.typedCalls++
	e.request = request
	e.taskToken = taskToken
	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

func TestRespondActivityTaskCompletedPassesDecodedTokenToHistoryEngine(t *testing.T) {
	request, wantToken, err := newActivityCompletionHandoffRequest(0)
	if err != nil {
		t.Fatal(err)
	}
	engine := &activityCompletionTypedHandoffCompatibilityEngine{}
	handler := &Handler{
		tokenSerializer: tasktoken.NewSerializer(),
		controller: &activityCompletionHandoffController{
			shardContext: &activityCompletionHandoffShardContext{engine: engine},
		},
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

	if engine.rawCalls != 0 || engine.typedCalls != 1 {
		t.Fatalf("typed Engine calls = raw %d, typed %d; want raw 0, typed 1", engine.rawCalls, engine.typedCalls)
	}
	if engine.taskToken == wantToken {
		t.Fatal("engine received the token instance used to create the serialized request, not the handler-decoded token")
	}
	if !wantToken.Equal(engine.taskToken) {
		t.Fatalf("engine task token = %v, want %v", engine.taskToken, wantToken)
	}
}

type activityCompletionTypedHandoffRunIDConsistencyChecker struct {
	api.WorkflowConsistencyChecker

	workflowLease       api.WorkflowLease
	currentRunID        string
	currentRunIDCalls   int
	workflowKeyForLease definition.WorkflowKey
}

func (c *activityCompletionTypedHandoffRunIDConsistencyChecker) GetWorkflowLease(
	_ context.Context,
	_ *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	_ locks.Priority,
) (api.WorkflowLease, error) {
	c.workflowKeyForLease = workflowKey
	return c.workflowLease, nil
}

func (c *activityCompletionTypedHandoffRunIDConsistencyChecker) GetCurrentWorkflowRunID(
	context.Context,
	string,
	string,
	locks.Priority,
) (string, error) {
	c.currentRunIDCalls++
	return c.currentRunID, nil
}

func TestRespondActivityTaskCompletedResolvesMissingRunIDThroughProductionHandoff(t *testing.T) {
	handler, mutableState := newActivityCompletionHandoffHandler()
	controller := handler.controller.(*activityCompletionHandoffController)
	shardContext := controller.shardContext.(*activityCompletionHandoffShardContext)
	historyEngine := shardContext.engine.(*historyEngineImpl)
	workflowConsistencyChecker := &activityCompletionTypedHandoffRunIDConsistencyChecker{
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
