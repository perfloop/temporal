package chasm

import (
	"strconv"
	"testing"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

const (
	cleanPersistedTreeEntries = 64
	cleanPersistedTreeNodes   = 2 + cleanPersistedTreeEntries*3 // root, collection, and three nodes per entry
)

type cleanPersistedTreeFixture struct {
	root    *Node
	backend *MockNodeBackend
}

func TestCloseTransactionCleanPersistedTree(t *testing.T) {
	fixture := newCleanPersistedTree(t)
	assertCleanClose(t, fixture)
}

func BenchmarkCloseTransactionCleanPersistedTree(b *testing.B) {
	fixture := newCleanPersistedTree(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		assertCleanClose(b, fixture)
	}
}

func newCleanPersistedTree(tb testing.TB) *cleanPersistedTreeFixture {
	tb.Helper()

	backend := &MockNodeBackend{
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	logger := testlogger.NewTestLogger(tb, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	if err := registry.Register(newTestLibrary(gomock.NewController(tb))); err != nil {
		tb.Fatalf("register test library: %v", err)
	}
	if err := registry.Register(&CoreLibrary{}); err != nil {
		tb.Fatalf("register core library: %v", err)
	}

	rootComponent := &TestComponent{
		ComponentData: &protoMessageType{RunId: "root"},
		MSPointer:     NewMSPointer(backend),
		SubComponents: make(Map[string, *TestSubComponent1], cleanPersistedTreeEntries),
	}
	for i := 0; i < cleanPersistedTreeEntries; i++ {
		key := strconv.Itoa(i)
		rootComponent.SubComponents[key] = NewComponentField(nil, &TestSubComponent1{
			SubComponent1Data: &protoMessageType{RunId: key},
			SubComponent11: NewComponentField(nil, &TestSubComponent11{
				SubComponent11Data: &protoMessageType{RunId: key},
			}),
			SubData11: NewDataField(nil, &protoMessageType{RunId: key}),
		})
	}

	timeSource := clock.NewEventTimeSource()
	pathEncoder := &testNodePathEncoder{}
	root := NewEmptyTree(
		registry,
		timeSource,
		backend,
		pathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	if err := root.SetRootComponent(rootComponent); err != nil {
		tb.Fatalf("set root component: %v", err)
	}

	initialMutation, err := root.CloseTransaction()
	if err != nil {
		tb.Fatalf("persist tree: %v", err)
	}
	if got := len(initialMutation.UpdatedNodes); got != cleanPersistedTreeNodes {
		tb.Fatalf("persisted node count = %d, want %d", got, cleanPersistedTreeNodes)
	}
	if got := len(initialMutation.DeletedNodes); got != 0 {
		tb.Fatalf("persisted deleted node count = %d, want 0", got)
	}

	backend.HandleNextTransitionCount = func() int64 { return 2 }
	reloaded, err := NewTreeFromDB(
		common.CloneProtoMap(initialMutation.UpdatedNodes),
		registry,
		timeSource,
		backend,
		pathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		tb.Fatalf("reload persisted tree: %v", err)
	}
	if got := countNodes(reloaded); got != cleanPersistedTreeNodes {
		tb.Fatalf("reloaded node count = %d, want %d", got, cleanPersistedTreeNodes)
	}
	if reloaded.IsDirty() {
		tb.Fatal("reloaded tree is dirty before the clean close")
	}

	return &cleanPersistedTreeFixture{root: reloaded, backend: backend}
}

func countNodes(root *Node) int {
	count := 0
	for range root.andAllChildren() {
		count++
	}
	return count
}

func assertCleanClose(tb testing.TB, fixture *cleanPersistedTreeFixture) {
	tb.Helper()

	mutation, err := fixture.root.CloseTransaction()
	if err != nil {
		tb.Fatalf("close clean persisted tree: %v", err)
	}
	if !mutation.IsEmpty() {
		tb.Fatalf("clean close mutation = %+v, want empty", mutation)
	}
	if fixture.root.IsDirty() {
		tb.Fatal("tree is dirty after clean close")
	}
	if got := fixture.backend.NumTasksAdded(); got != 0 {
		tb.Fatalf("emitted task count = %d, want 0", got)
	}
}
