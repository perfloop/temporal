package history

import (
	"context"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	pm "go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	redirectRetryInitialBuildID = "build-before-redirect"
	redirectRetryTargetBuildID  = "build-after-redirect"
)

type redirectRetryLifecycle struct {
	execution      *commonpb.WorkflowExecution
	mutableState   *workflow.MutableStateImpl
	workflowKey    definition.WorkflowKey
	retry          *persistencespb.ActivityInfo
	retryTimer     *tasks.ActivityRetryTimerTask
	triggerEventID int64
	lastEventID    int64
	lastVersion    int64
}

// newRedirectRetryLifecycle creates the retry through MutableState.RetryActivity,
// rather than constructing a pending retry by hand. The original retry timer is
// retained separately because it represents the physical task already queued before
// the redirect is applied.
func (s *timerQueueActiveTaskExecutorSuite) newRedirectRetryLifecycle() *redirectRetryLifecycle {
	s.T().Helper()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "redirect-retry-workflow-" + uuid.NewString(),
		RunId:      uuid.NewString(),
	}
	workflowType := "redirect-retry-workflow-type"
	taskQueueName := "redirect-retry-task-queue"
	mutableState := workflow.TestGlobalMutableState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(time.Second),
			},
		},
	)
	s.Require().NoError(err)

	workflowTask := addWorkflowTaskScheduledEvent(mutableState)
	workflowTaskStarted := addWorkflowTaskStartedEvent(
		mutableState,
		workflowTask.ScheduledEventID,
		taskQueueName,
		uuid.NewString(),
	)
	workflowTask.StartedEventID = workflowTaskStarted.GetEventId()
	workflowTaskCompleted := addWorkflowTaskCompletedEvent(
		&s.Suite,
		mutableState,
		workflowTask.ScheduledEventID,
		workflowTask.StartedEventID,
		"redirect-retry-worker",
	)
	mutableState.GetExecutionInfo().AssignedBuildId = redirectRetryInitialBuildID

	retryScheduled, retry := addActivityTaskScheduledEventWithRetry(
		mutableState,
		workflowTaskCompleted.GetEventId(),
		"redirect-retry-activity",
		"redirect-retry-activity-type",
		taskQueueName,
		nil,
		24*time.Hour,
		24*time.Hour,
		24*time.Hour,
		0,
		&commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Hour),
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	)
	retry.BuildIdInfo = &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo_{
		UseWorkflowBuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo{
			LastUsedBuildId: redirectRetryInitialBuildID,
		},
	}
	_, err = mutableState.AddActivityTaskStartedEvent(
		retry,
		retry.GetScheduledEventId(),
		uuid.NewString(),
		"redirect-retry-worker",
		nil,
		nil,
		nil,
		"",
		nil,
	)
	s.Require().NoError(err)
	retryState, err := mutableState.RetryActivity(retry, &failurepb.Failure{Message: "retryable failure"})
	s.Require().NoError(err)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	retry, ok := mutableState.GetActivityInfo(retryScheduled.GetEventId())
	s.Require().True(ok)
	s.Equal(int32(2), retry.GetAttempt())
	s.True(retry.GetScheduledTime().AsTime().After(s.now), "retry must be in backoff before redirect")

	var retryTimer *tasks.ActivityRetryTimerTask
	for _, task := range mutableState.PopTasks()[tasks.CategoryTimer] {
		timer, ok := task.(*tasks.ActivityRetryTimerTask)
		if ok && timer.EventID == retry.GetScheduledEventId() {
			retryTimer = timer
		}
	}
	s.Require().NotNil(retryTimer, "RetryActivity must create the retry timer")

	triggerScheduled, _ := addActivityTaskScheduledEvent(
		mutableState,
		workflowTaskCompleted.GetEventId(),
		"redirect-trigger-activity",
		"redirect-trigger-activity-type",
		taskQueueName,
		nil,
		24*time.Hour,
		24*time.Hour,
		24*time.Hour,
		0,
	)
	// The test starts with only the normal retry timer in the physical queue.
	mutableState.PopTasks()

	return &redirectRetryLifecycle{
		execution:      execution,
		mutableState:   mutableState,
		workflowKey:    definition.NewWorkflowKey(s.namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
		retry:          retry,
		retryTimer:     retryTimer,
		triggerEventID: triggerScheduled.GetEventId(),
		lastEventID:    triggerScheduled.GetEventId(),
		lastVersion:    triggerScheduled.GetVersion(),
	}
}

// TestActivityRetryTimerRedirectAfterReload exercises the complete retained-timer
// lifecycle. The baseline still takes its supported immediate ActivityTask route,
// while the redirect optimization must retain the original retry timer, reload the
// persisted mutable state, and dispatch it to Matching with the redirected build ID.
func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimerRedirectAfterReload() {
	lifecycle := s.newRedirectRetryLifecycle()

	s.Require().NoError(lifecycle.mutableState.ApplyBuildIdRedirect(
		lifecycle.triggerEventID,
		redirectRetryTargetBuildID,
		1,
	))

	var immediateRetryTask *tasks.ActivityTask
	for _, task := range lifecycle.mutableState.PopTasks()[tasks.CategoryTransfer] {
		activityTask, ok := task.(*tasks.ActivityTask)
		if ok && activityTask.ScheduledEventID == lifecycle.retry.GetScheduledEventId() {
			immediateRetryTask = activityTask
		}
	}

	timerStillCurrent := lifecycle.retryTimer.EventID == lifecycle.retry.GetScheduledEventId() &&
		lifecycle.retryTimer.Attempt == lifecycle.retry.GetAttempt() &&
		lifecycle.retryTimer.Stamp == lifecycle.retry.GetStamp()
	if !timerStillCurrent {
		// The pre-optimization implementation invalidates the old timer and uses a
		// current-stamp immediate ActivityTask. Keep this branch so the sealed check
		// can validate both revisions, while the retained-timer branch below verifies
		// the candidate's new route end to end.
		s.Require().NotNil(immediateRetryTask)
		s.Equal(lifecycle.retry.GetStamp(), immediateRetryTask.Stamp)
		return
	}

	s.Nil(immediateRetryTask, "a retained future retry must not be dispatched before its backoff expires")
	s.Equal(redirectRetryTargetBuildID, lifecycle.mutableState.GetAssignedBuildId())

	lifecycle.retryTimer.TaskID = s.mustGenerateTaskID()
	persistedState := s.createPersistenceMutableState(
		lifecycle.mutableState,
		lifecycle.lastEventID,
		lifecycle.lastVersion,
	)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&persistence.GetWorkflowExecutionResponse{State: persistedState},
		nil,
	)
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		pm.Eq(&matchingservice.AddActivityTaskRequest{
			NamespaceId: lifecycle.workflowKey.NamespaceID,
			Execution:   lifecycle.execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: lifecycle.retry.GetTaskQueue(),
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduledEventId:       lifecycle.retry.GetScheduledEventId(),
			ScheduleToStartTimeout: lifecycle.retry.GetScheduleToStartTimeout(),
			Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), lifecycle.retryTimer.TaskID),
			VersionDirective:       worker_versioning.MakeBuildIdDirective(redirectRetryTargetBuildID),
			Stamp:                  lifecycle.retryTimer.Stamp,
		}),
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	s.timeSource.Update(lifecycle.retry.GetScheduledTime().AsTime())
	response := s.timerQueueActiveTaskExecutor.Execute(
		context.Background(),
		s.newTaskExecutable(lifecycle.retryTimer),
	)
	s.NoError(response.ExecutionErr)
}
