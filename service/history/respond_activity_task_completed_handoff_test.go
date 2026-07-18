package history

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const activityCompletionHandoffWorkflowPrefix = "activity-completion-handoff"

type activityCompletionHandoffController struct {
	shard.Controller

	shardContext historyi.ShardContext
	calls        int
}

func (c *activityCompletionHandoffController) GetShardByNamespaceWorkflow(
	_ namespace.ID,
	_ string,
) (historyi.ShardContext, error) {
	c.calls++
	return c.shardContext, nil
}

type activityCompletionHandoffComponentEngine struct {
	chasm.Engine

	updateCalls int
}

func (e *activityCompletionHandoffComponentEngine) UpdateComponent(
	context.Context,
	chasm.ComponentRef,
	func(chasm.MutableContext, chasm.Component) error,
	...chasm.TransitionOption,
) ([]byte, error) {
	e.updateCalls++
	return nil, nil
}

// activityCompletionHandoffShardContext supplies only the external shard boundaries
// that the real completion path needs. The Handler, historyEngineImpl, and
// respondactivitytaskcompleted.Invoke implementations remain in the benchmarked path.
type activityCompletionHandoffShardContext struct {
	historyi.ShardContext

	engine            historyi.Engine
	namespaceRegistry namespace.Registry
	clusterMetadata   cluster.Metadata
	config            *configs.Config
	timeSource        clock.TimeSource
}

func (c *activityCompletionHandoffShardContext) GetEngine(context.Context) (historyi.Engine, error) {
	return c.engine, nil
}

func (c *activityCompletionHandoffShardContext) GetNamespaceRegistry() namespace.Registry {
	return c.namespaceRegistry
}

func (c *activityCompletionHandoffShardContext) GetClusterMetadata() cluster.Metadata {
	return c.clusterMetadata
}

func (*activityCompletionHandoffShardContext) GetMetricsHandler() metrics.Handler {
	return metrics.NoopMetricsHandler
}

func (c *activityCompletionHandoffShardContext) GetConfig() *configs.Config {
	return c.config
}

func (c *activityCompletionHandoffShardContext) GetTimeSource() clock.TimeSource {
	return c.timeSource
}

type activityCompletionHandoffNamespaceRegistry struct {
	namespace.Registry
}

func (*activityCompletionHandoffNamespaceRegistry) GetNamespaceByID(namespace.ID) (*namespace.Namespace, error) {
	return tests.LocalNamespaceEntry, nil
}

type activityCompletionHandoffClusterMetadata struct {
	cluster.Metadata
}

func (*activityCompletionHandoffClusterMetadata) GetCurrentClusterName() string {
	return cluster.TestCurrentClusterName
}

type activityCompletionHandoffWorkflowConsistencyChecker struct {
	api.WorkflowConsistencyChecker

	workflowLease api.WorkflowLease
}

func (c *activityCompletionHandoffWorkflowConsistencyChecker) GetWorkflowLease(
	context.Context,
	*clockspb.VectorClock,
	definition.WorkflowKey,
	locks.Priority,
) (api.WorkflowLease, error) {
	return c.workflowLease, nil
}

type activityCompletionHandoffWorkflowContext struct {
	historyi.WorkflowContext
}

func (*activityCompletionHandoffWorkflowContext) UpdateWorkflowExecutionAsActive(
	context.Context,
	historyi.ShardContext,
) error {
	return nil
}

type activityCompletionHandoffMutableState struct {
	historyi.MutableState

	completedRequest          *workflowservice.RespondActivityTaskCompletedRequest
	completedScheduledEventID int64
}

func (*activityCompletionHandoffMutableState) GetWorkflowType() *commonpb.WorkflowType {
	return &commonpb.WorkflowType{Name: "activity-completion-benchmark-workflow-type"}
}

func (*activityCompletionHandoffMutableState) IsWorkflowExecutionRunning() bool {
	return true
}

func (*activityCompletionHandoffMutableState) GetActivityInfo(
	scheduledEventID int64,
) (*persistencespb.ActivityInfo, bool) {
	return &persistencespb.ActivityInfo{
		ScheduledEventId:   scheduledEventID,
		StartedEventId:     200,
		Attempt:            1,
		StartedTime:        timestamppb.New(time.Unix(1_700_000_000, 0)),
		FirstScheduledTime: timestamppb.New(time.Unix(1_699_999_000, 0)),
		TaskQueue:          "activity-completion-benchmark-task-queue",
	}, true
}

func (s *activityCompletionHandoffMutableState) AddActivityTaskCompletedEvent(
	scheduledEventID int64,
	_ int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*historypb.HistoryEvent, error) {
	s.completedRequest = request
	s.completedScheduledEventID = scheduledEventID
	return &historypb.HistoryEvent{}, nil
}

func (*activityCompletionHandoffMutableState) GetEffectiveVersioningBehavior() enumspb.VersioningBehavior {
	return enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
}

func (*activityCompletionHandoffMutableState) HasPendingWorkflowTask() bool {
	return false
}

func (*activityCompletionHandoffMutableState) IsWorkflowExecutionStatusPaused() bool {
	return false
}

func (*activityCompletionHandoffMutableState) AddWorkflowTaskScheduledEvent(
	bool,
	enumsspb.WorkflowTaskType,
) (*historyi.WorkflowTaskInfo, error) {
	return nil, nil
}

func newActivityCompletionHandoffHandler() (*Handler, *activityCompletionHandoffMutableState) {
	serializer := tasktoken.NewSerializer()
	mutableState := &activityCompletionHandoffMutableState{}
	workflowLease := api.NewWorkflowLease(
		&activityCompletionHandoffWorkflowContext{},
		func(error) {},
		mutableState,
	)
	shardContext := &activityCompletionHandoffShardContext{
		namespaceRegistry: &activityCompletionHandoffNamespaceRegistry{},
		clusterMetadata:   &activityCompletionHandoffClusterMetadata{},
		config:            configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
		timeSource:        clock.NewRealTimeSource(),
	}
	engine := &historyEngineImpl{
		shardContext:               shardContext,
		tokenSerializer:            serializer,
		workflowConsistencyChecker: &activityCompletionHandoffWorkflowConsistencyChecker{workflowLease: workflowLease},
	}
	shardContext.engine = engine

	return &Handler{
		tokenSerializer: serializer,
		controller:      &activityCompletionHandoffController{shardContext: shardContext},
	}, mutableState
}

func newActivityCompletionHandoffRequest(
	sequence int,
) (*historyservice.RespondActivityTaskCompletedRequest, *tokenspb.Task, error) {
	taskToken := &tokenspb.Task{
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       fmt.Sprintf("%s-workflow-%d", activityCompletionHandoffWorkflowPrefix, sequence),
		RunId:            tests.RunID,
		ScheduledEventId: int64(100 + sequence),
		Attempt:          1,
		ActivityId:       fmt.Sprintf("%s-activity-%d", activityCompletionHandoffWorkflowPrefix, sequence),
		WorkflowType:     "activity-completion-benchmark-workflow-type",
		ActivityType:     "activity-completion-benchmark-activity-type",
		Clock: &clockspb.VectorClock{
			ShardId:   int32(sequence + 1),
			Clock:     int64(1000 + sequence),
			ClusterId: int64(2000 + sequence),
		},
		StartedEventId: int64(200 + sequence),
		Version:        0,
		StartedTime:    timestamppb.New(time.Unix(int64(1_700_000_000+sequence), 0)),
		StartVersion:   0,
	}

	serializedTaskToken, err := tasktoken.NewSerializer().Serialize(taskToken)
	if err != nil {
		return nil, nil, err
	}

	return &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: taskToken.GetNamespaceId(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Identity:  "activity-completion-handoff-benchmark-worker",
		},
	}, taskToken, nil
}

func TestRespondActivityTaskCompletedRunsProductionCompletionPath(t *testing.T) {
	handler, mutableState := newActivityCompletionHandoffHandler()
	request, wantToken, err := newActivityCompletionHandoffRequest(0)
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
	if mutableState.completedRequest != request.GetCompleteRequest() {
		t.Fatalf("completed request = %p, want %p", mutableState.completedRequest, request.GetCompleteRequest())
	}
	if mutableState.completedScheduledEventID != wantToken.GetScheduledEventId() {
		t.Fatalf("completed scheduled event ID = %d, want %d", mutableState.completedScheduledEventID, wantToken.GetScheduledEventId())
	}
}

func TestRespondActivityTaskCompletedRejectsInvalidTokenBeforeRouting(t *testing.T) {
	controller := &activityCompletionHandoffController{}
	handler := &Handler{
		tokenSerializer: tasktoken.NewSerializer(),
		controller:      controller,
	}

	_, err := handler.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: []byte("not a serialized activity task token"),
		},
	})
	if !errors.Is(err, consts.ErrDeserializingToken) {
		t.Fatalf("RespondActivityTaskCompleted error = %v, want %v", err, consts.ErrDeserializingToken)
	}
	if controller.calls != 0 {
		t.Fatalf("GetShardByNamespaceWorkflow calls = %d, want 0", controller.calls)
	}
}

func TestRespondActivityTaskCompletedKeepsComponentPathSeparate(t *testing.T) {
	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: tests.NamespaceID.String(),
		BusinessId:  "activity-completion-component",
		RunId:       tests.RunID,
		ArchetypeId: 1,
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	taskTokenBytes, err := tasktoken.NewSerializer().Serialize(&tokenspb.Task{
		NamespaceId:  tests.NamespaceID.String(),
		ComponentRef: componentRef,
	})
	if err != nil {
		t.Fatal(err)
	}

	controller := &activityCompletionHandoffController{}
	componentEngine := &activityCompletionHandoffComponentEngine{}
	handler := &Handler{
		tokenSerializer: tasktoken.NewSerializer(),
		controller:      controller,
	}
	_, err = handler.RespondActivityTaskCompleted(
		chasm.NewEngineContext(context.Background(), componentEngine),
		&historyservice.RespondActivityTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if componentEngine.updateCalls != 1 {
		t.Fatalf("component UpdateComponent calls = %d, want 1", componentEngine.updateCalls)
	}
	if controller.calls != 0 {
		t.Fatalf("GetShardByNamespaceWorkflow calls = %d, want 0", controller.calls)
	}
}
