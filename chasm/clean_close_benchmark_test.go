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
	cleanCloseMapEntries = 64
	cleanCloseNodeCount  = 194
)

var cleanCloseMutationSink NodesMutation

func TestCloseTransactionCleanPersistedTree(t *testing.T) {
	root, backend := newCleanClosePersistedTree(t)

	assertCleanClose(t, root, backend)
	assertCleanClose(t, root, backend)
}

func BenchmarkCloseTransactionCleanPersistedTree(b *testing.B) {
	root, backend := newCleanClosePersistedTree(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mutation, err := root.CloseTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if !mutation.IsEmpty() {
			b.Fatal("clean close produced a node mutation")
		}
		cleanCloseMutationSink = mutation
	}
	b.StopTimer()

	if root.subtreeIsDirty {
		b.Fatal("clean close left the root dirty")
	}
	if backend.NumTasksAdded() != 0 {
		b.Fatal("clean close emitted physical tasks")
	}
}

func newCleanClosePersistedTree(tb testing.TB) (*Node, *MockNodeBackend) {
	tb.Helper()

	controller := gomock.NewController(tb)
	registry := NewRegistry(log.NewNoopLogger())
	if err := registry.Register(newTestLibrary(controller)); err != nil {
		tb.Fatalf("register test library: %v", err)
	}
	if err := registry.Register(&CoreLibrary{}); err != nil {
		tb.Fatalf("register core library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	initialBackend := newCleanCloseBackend()
	initialTree := NewEmptyTree(
		registry,
		timeSource,
		initialBackend,
		DefaultPathEncoder,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err := initialTree.SetRootComponent(newCleanCloseRootComponent(initialBackend)); err != nil {
		tb.Fatalf("set root component: %v", err)
	}

	initialMutation, err := initialTree.CloseTransaction()
	if err != nil {
		tb.Fatalf("persist initial tree: %v", err)
	}
	if got := len(initialMutation.UpdatedNodes); got != cleanCloseNodeCount {
		tb.Fatalf("persisted node count = %d, want %d", got, cleanCloseNodeCount)
	}
	if len(initialMutation.DeletedNodes) != 0 {
		tb.Fatalf("initial tree deleted %d nodes", len(initialMutation.DeletedNodes))
	}

	backend := newCleanCloseBackend()
	reloadedTree, err := NewTreeFromDB(
		common.CloneProtoMap(initialMutation.UpdatedNodes),
		registry,
		timeSource,
		backend,
		DefaultPathEncoder,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		tb.Fatalf("reload persisted tree: %v", err)
	}
	if reloadedTree.subtreeIsDirty {
		tb.Fatal("reloaded tree is unexpectedly dirty")
	}

	return reloadedTree, backend
}

func newCleanCloseBackend() *MockNodeBackend {
	return &MockNodeBackend{
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
}

func newCleanCloseRootComponent(backend NodeBackend) *TestComponent {
	root := &TestComponent{
		ComponentData: &persistencespb.WorkflowExecutionState{RunId: "root"},
		MSPointer:     NewMSPointer(backend),
		SubComponents: make(Map[string, *TestSubComponent1], cleanCloseMapEntries),
	}

	for i := range cleanCloseMapEntries {
		root.SubComponents[fmt.Sprintf("component-%02d", i)] = NewComponentField(nil, &TestSubComponent1{
			SubComponent1Data: &persistencespb.WorkflowExecutionState{RunId: fmt.Sprintf("component-%02d", i)},
			SubComponent11: NewComponentField(nil, &TestSubComponent11{
				SubComponent11Data: &persistencespb.WorkflowExecutionState{RunId: fmt.Sprintf("nested-%02d", i)},
			}),
			SubData11: NewDataField(nil, &persistencespb.WorkflowExecutionState{RunId: fmt.Sprintf("data-%02d", i)}),
		})
	}

	return root
}

func assertCleanClose(tb testing.TB, root *Node, backend *MockNodeBackend) {
	tb.Helper()

	if root.subtreeIsDirty {
		tb.Fatal("clean close started with a dirty root")
	}
	mutation, err := root.CloseTransaction()
	if err != nil {
		tb.Fatalf("close persisted tree: %v", err)
	}
	if !mutation.IsEmpty() {
		tb.Fatalf(
			"clean close produced %d updates and %d deletions",
			len(mutation.UpdatedNodes),
			len(mutation.DeletedNodes),
		)
	}
	if root.subtreeIsDirty {
		tb.Fatal("clean close left the root dirty")
	}
	if backend.NumTasksAdded() != 0 {
		tb.Fatal("clean close emitted physical tasks")
	}
}
