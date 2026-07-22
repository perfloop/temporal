package workflow

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestPartialRefreshRestoresRedirectedRetryTimer models the replicated/recovered
// state shape: mutable state contains a retry that has already been redirected,
// but the derived physical timer task is absent. PartialRefresh is the supported
// task-recovery path used for replicated deltas, and must recreate the timer that
// will dispatch this activity at its scheduled time.
func (s *taskRefresherSuite) TestPartialRefreshRestoresRedirectedRetryTimer() {
	const (
		redirectedBuildID = "build-after-redirect"
		scheduledEventID  = int64(10)
		activityStamp     = int32(7)
	)
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	activityTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: common.EmptyVersion,
		TransitionCount:          2,
	}
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:            tests.NamespaceID.String(),
			WorkflowId:             tests.WorkflowID,
			AssignedBuildId:        redirectedBuildID,
			BuildIdRedirectCounter: 1,
			VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{
				BranchToken: []byte("redirected-retry-branch"),
				Items:       []*historyspb.VersionHistoryItem{{EventId: scheduledEventID, Version: common.EmptyVersion}},
			}}},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: common.EmptyVersion,
				TransitionCount:          1,
			},
		},
		NextEventId: scheduledEventID + 1,
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			scheduledEventID: {
				ActivityId:       "redirected-retry",
				ScheduledEventId: scheduledEventID,
				Version:          common.EmptyVersion,
				TaskQueue:        "redirected-retry-task-queue",
				ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
				StartedEventId:   common.EmptyEventID,
				HasRetryPolicy:   true,
				Attempt:          2,
				Stamp:            activityStamp,
				BuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo_{
					UseWorkflowBuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo{
						LastUsedBuildId: "build-before-redirect",
					},
				},
				LastUpdateVersionedTransition: activityTransition,
			},
		},
	}

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.GlobalNamespaceEntry,
		mutableStateRecord,
		scheduledEventID,
	)
	s.Require().NoError(err)

	// Use the real task generator. There is intentionally no pre-existing timer in
	// mutableStateRecord: physical tasks are derived state and are rebuilt here.
	refresher := NewTaskRefresher(s.mockShard)
	err = refresher.PartialRefresh(context.Background(), mutableState, activityTransition, nil, false)
	s.Require().NoError(err)

	var retryTimer *tasks.ActivityRetryTimerTask
	for _, task := range mutableState.PopTasks()[tasks.CategoryTimer] {
		if candidate, ok := task.(*tasks.ActivityRetryTimerTask); ok && candidate.EventID == scheduledEventID {
			retryTimer = candidate
		}
	}
	s.Require().NotNil(retryTimer, "partial refresh must restore the missing retry dispatch timer")
	s.Equal(now.Add(time.Hour), retryTimer.VisibilityTimestamp)
	s.Equal(int32(2), retryTimer.Attempt)
	s.Equal(activityStamp, retryTimer.Stamp)

	activity, ok := mutableState.GetActivityInfo(scheduledEventID)
	s.True(ok)
	s.NotNil(activity.GetUseWorkflowBuildIdInfo())
	s.Equal(redirectedBuildID, mutableState.GetAssignedBuildId())
}
