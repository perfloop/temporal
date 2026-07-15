package chasm

import (
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
)

func (s *nodeSuite) TestCloseTransaction_RetryPersistsDirtyTreeAfterLifecycleError() {
	const updatedRunID = "updated-after-retry"

	root := s.testComponentTree()
	initialMutation, err := root.CloseTransaction()
	s.NoError(err)
	persistedNodes := common.CloneProtoMap(initialMutation.UpdatedNodes)

	retryRoot, err := s.newTestTree(common.CloneProtoMap(persistedNodes))
	s.NoError(err)
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }

	mutableContext := NewMutableContext(s.T().Context(), retryRoot)
	component, err := retryRoot.Component(mutableContext, ComponentRef{})
	s.NoError(err)
	component.(*TestComponent).ComponentData.RunId = updatedRunID

	lifecycleErr := errors.New("transient lifecycle update failure")
	lifecycleCalls := 0
	s.nodeBackend.HandleUpdateWorkflowStateStatus = func(
		_ enumsspb.WorkflowExecutionState,
		_ enumspb.WorkflowExecutionStatus,
	) (bool, error) {
		lifecycleCalls++
		if lifecycleCalls == 1 {
			return false, lifecycleErr
		}
		return true, nil
	}

	_, err = retryRoot.CloseTransaction()
	s.ErrorIs(err, lifecycleErr)

	retryMutation, err := retryRoot.CloseTransaction()
	s.NoError(err)
	s.Contains(retryMutation.UpdatedNodes, "", "retry must persist the mutated root")
	s.Equal(2, lifecycleCalls)

	persistedNodes[""] = retryMutation.UpdatedNodes[""]
	reloadedRoot, err := s.newTestTree(common.CloneProtoMap(persistedNodes))
	s.NoError(err)
	component, err = reloadedRoot.Component(NewContext(s.T().Context(), reloadedRoot), ComponentRef{})
	s.NoError(err)
	s.Equal(updatedRunID, component.(*TestComponent).ComponentData.GetRunId())
}
