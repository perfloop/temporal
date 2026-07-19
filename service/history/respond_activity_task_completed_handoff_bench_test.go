package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

func BenchmarkRespondActivityTaskCompletedTokenHandoff(b *testing.B) {
	fixture := newActivityCompletionHandoffFixture()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := fixture.handler.RespondActivityTaskCompleted(fixture.ctx, fixture.request); err != nil {
			b.Fatal(err)
		}
	}

	if fixture.mutableState.completedRequest != fixture.request.CompleteRequest ||
		fixture.mutableState.completedScheduledEventID != fixture.scheduledEventID {
		b.Fatal("activity completion did not reach mutable state")
	}
}

func TestRespondActivityTaskCompletedRunsProductionCompletionPath(t *testing.T) {
	fixture := newActivityCompletionHandoffFixture()

	_, err := fixture.handler.RespondActivityTaskCompleted(fixture.ctx, fixture.request)
	require.NoError(t, err)
	require.Same(t, fixture.request.CompleteRequest, fixture.mutableState.completedRequest)
	require.Equal(t, fixture.scheduledEventID, fixture.mutableState.completedScheduledEventID)
}

func TestRespondActivityTaskCompletedRejectsInvalidTokenBeforeRouting(t *testing.T) {
	invalidToken, err := (&tokenspb.Task{NamespaceId: tests.NamespaceID.String()}).Marshal()
	require.NoError(t, err)

	handler := &Handler{tokenSerializer: tasktoken.NewSerializer()}
	response, err := handler.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
		},
	})
	require.Error(t, err)
	require.Nil(t, response)
}

type activityCompletionHandoffFixture struct {
	ctx              context.Context
	handler          *Handler
	mutableState     *activityCompletionHandoffMutableState
	request          *historyservice.RespondActivityTaskCompletedRequest
	scheduledEventID int64
}

func newActivityCompletionHandoffFixture() *activityCompletionHandoffFixture {
	const scheduledEventID = int64(42)

	token := &tokenspb.Task{
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       "workflow-id",
		RunId:            "run-id",
		ScheduledEventId: scheduledEventID,
		Attempt:          2,
		ActivityId:       "activity-id",
		WorkflowType:     "workflow-type",
		ActivityType:     "activity-type",
		Clock: &clockspb.VectorClock{
			ShardId:   1,
			Clock:     2,
			ClusterId: 3,
		},
		StartedEventId: 43,
		Version:        5,
		StartVersion:   4,
	}
	taskToken, err := token.Marshal()
	if err != nil {
		panic(err)
	}

	request := &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  "worker",
			Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
				Data: []byte("activity-result"),
			}}},
		},
	}
	mutableState := &activityCompletionHandoffMutableState{
		activityInfo: &persistencespb.ActivityInfo{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   token.StartedEventId,
			Attempt:          token.Attempt,
			TaskQueue:        "activity-task-queue",
			Version:          token.Version,
			StartVersion:     token.StartVersion,
		},
		completedEvent: &historypb.HistoryEvent{},
		workflowType:   &commonpb.WorkflowType{Name: token.WorkflowType},
	}
	workflowContext := &activityCompletionHandoffWorkflowContext{}
	workflowLease := api.NewWorkflowLease(workflowContext, func(error) {}, mutableState)
	workflowConsistencyChecker := &activityCompletionHandoffWorkflowConsistencyChecker{
		workflowLease: workflowLease,
	}
	shardContext := &activityCompletionHandoffShardContext{
		clusterMetadata:   activityCompletionHandoffClusterMetadata{},
		config:            tests.NewDynamicConfig(),
		metricsHandler:    metrics.NoopMetricsHandler,
		namespaceRegistry: activityCompletionHandoffNamespaceRegistry{entry: tests.LocalNamespaceEntry},
		timeSource:        clock.NewRealTimeSource(),
	}
	engine := &historyEngineImpl{
		shardContext:               shardContext,
		workflowConsistencyChecker: workflowConsistencyChecker,
	}
	shardContext.engine = engine

	return &activityCompletionHandoffFixture{
		ctx: context.Background(),
		handler: &Handler{
			controller:      &activityCompletionHandoffController{shardContext: shardContext},
			tokenSerializer: tasktoken.NewSerializer(),
		},
		mutableState:     mutableState,
		request:          request,
		scheduledEventID: scheduledEventID,
	}
}

type activityCompletionHandoffController struct {
	shard.Controller
	shardContext historyi.ShardContext
}

func (c *activityCompletionHandoffController) GetShardByNamespaceWorkflow(namespace.ID, string) (historyi.ShardContext, error) {
	return c.shardContext, nil
}

type activityCompletionHandoffShardContext struct {
	historyi.ShardContext
	clusterMetadata   cluster.Metadata
	config            *configs.Config
	engine            historyi.Engine
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
	timeSource        clock.TimeSource
}

func (s *activityCompletionHandoffShardContext) GetClusterMetadata() cluster.Metadata {
	return s.clusterMetadata
}

func (s *activityCompletionHandoffShardContext) GetConfig() *configs.Config {
	return s.config
}

func (s *activityCompletionHandoffShardContext) GetEngine(context.Context) (historyi.Engine, error) {
	return s.engine, nil
}

func (s *activityCompletionHandoffShardContext) GetMetricsHandler() metrics.Handler {
	return s.metricsHandler
}

func (s *activityCompletionHandoffShardContext) GetNamespaceRegistry() namespace.Registry {
	return s.namespaceRegistry
}

func (s *activityCompletionHandoffShardContext) GetTimeSource() clock.TimeSource {
	return s.timeSource
}

type activityCompletionHandoffNamespaceRegistry struct {
	namespace.Registry
	entry *namespace.Namespace
}

func (r activityCompletionHandoffNamespaceRegistry) GetNamespaceByID(namespace.ID) (*namespace.Namespace, error) {
	return r.entry, nil
}

type activityCompletionHandoffClusterMetadata struct {
	cluster.Metadata
}

func (activityCompletionHandoffClusterMetadata) GetCurrentClusterName() string {
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

func (*activityCompletionHandoffWorkflowContext) UpdateWorkflowExecutionAsActive(context.Context, historyi.ShardContext) error {
	return nil
}

type activityCompletionHandoffMutableState struct {
	historyi.MutableState
	activityInfo              *persistencespb.ActivityInfo
	completedEvent            *historypb.HistoryEvent
	completedRequest          *workflowservice.RespondActivityTaskCompletedRequest
	completedScheduledEventID int64
	workflowType              *commonpb.WorkflowType
}

func (s *activityCompletionHandoffMutableState) AddActivityTaskCompletedEvent(
	scheduledEventID int64,
	_ int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*historypb.HistoryEvent, error) {
	s.completedRequest = request
	s.completedScheduledEventID = scheduledEventID
	return s.completedEvent, nil
}

func (s *activityCompletionHandoffMutableState) GetActivityInfo(scheduledEventID int64) (*persistencespb.ActivityInfo, bool) {
	return s.activityInfo, scheduledEventID == s.activityInfo.ScheduledEventId
}

func (*activityCompletionHandoffMutableState) GetEffectiveVersioningBehavior() enumspb.VersioningBehavior {
	return enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
}

func (*activityCompletionHandoffMutableState) HasPendingWorkflowTask() bool {
	return true
}

func (*activityCompletionHandoffMutableState) IsWorkflowExecutionRunning() bool {
	return true
}

func (s *activityCompletionHandoffMutableState) GetWorkflowType() *commonpb.WorkflowType {
	return s.workflowType
}
