package chasm

import (
	"fmt"
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

const (
	cleanCloseMapEntries         = 64
	cleanClosePersistedNodeCount = 2 + cleanCloseMapEntries*3
)

func TestCloseTransactionCleanPersistedTree(t *testing.T) {
	root, backend := newCleanClosePersistedTree(t)

	mutation, err := root.CloseTransaction()
	if err != nil {
		t.Fatal(err)
	}
	assertCleanClose(t, root, backend, mutation)
}

func BenchmarkCloseTransactionCleanPersistedTree(b *testing.B) {
	root, backend := newCleanClosePersistedTree(b)
	b.ReportAllocs()

	var mutation NodesMutation
	for b.Loop() {
		var err error
		mutation, err = root.CloseTransaction()
		if err != nil {
			b.Fatal(err)
		}
	}

	assertCleanClose(b, root, backend, mutation)
}

func newCleanClosePersistedTree(t testing.TB) (*Node, *MockNodeBackend) {
	t.Helper()

	logger := log.NewNoopLogger()
	registry := NewRegistry(logger)
	controller := gomock.NewController(t)
	if err := registry.Register(newTestLibrary(controller)); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(&CoreLibrary{}); err != nil {
		t.Fatal(err)
	}

	timeSource := clock.NewEventTimeSource()
	initialBackend := cleanCloseBackend()
	initialTree := NewEmptyTree(
		registry,
		timeSource,
		initialBackend,
		DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	if err := initialTree.SetRootComponent(cleanCloseRootComponent(initialBackend)); err != nil {
		t.Fatal(err)
	}

	initialMutation, err := initialTree.CloseTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(initialMutation.UpdatedNodes); got != cleanClosePersistedNodeCount {
		t.Fatalf("persisted node count = %d, want %d", got, cleanClosePersistedNodeCount)
	}
	if len(initialMutation.DeletedNodes) != 0 {
		t.Fatalf("initial persisted tree has %d deleted nodes, want none", len(initialMutation.DeletedNodes))
	}

	loadedBackend := cleanCloseBackend()
	root, err := NewTreeFromDB(
		common.CloneProtoMap(initialMutation.UpdatedNodes),
		registry,
		timeSource,
		loadedBackend,
		DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatal(err)
	}
	if root.IsDirty() {
		t.Fatal("reloaded tree is dirty before clean close")
	}

	return root, loadedBackend
}

func cleanCloseRootComponent(backend *MockNodeBackend) *TestComponent {
	component := &TestComponent{
		ComponentData: &persistencespb.WorkflowExecutionState{RunId: "clean-close-root"},
		MSPointer:     NewMSPointer(backend),
		SubComponents: make(Map[string, *TestSubComponent1], cleanCloseMapEntries),
	}
	for i := range cleanCloseMapEntries {
		entryID := fmt.Sprintf("entry-%02d", i)
		component.SubComponents[entryID] = NewComponentField(nil, &TestSubComponent1{
			SubComponent1Data: &persistencespb.WorkflowExecutionState{RunId: entryID},
			SubComponent11: NewComponentField(nil, &TestSubComponent11{
				SubComponent11Data: &persistencespb.WorkflowExecutionState{RunId: entryID + "-child"},
			}),
			SubData11: NewDataField(nil, &persistencespb.WorkflowExecutionState{RunId: entryID + "-data"}),
		})
	}
	return component
}

func cleanCloseBackend() *MockNodeBackend {
	return &MockNodeBackend{
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
}

func assertCleanClose(t testing.TB, root *Node, backend *MockNodeBackend, mutation NodesMutation) {
	t.Helper()
	if !mutation.IsEmpty() {
		t.Fatalf("clean close produced mutation: %+v", mutation)
	}
	if root.IsDirty() {
		t.Fatal("tree remains dirty after clean close")
	}
	if got := backend.NumTasksAdded(); got != 0 {
		t.Fatalf("clean close emitted %d physical tasks, want none", got)
	}
}
