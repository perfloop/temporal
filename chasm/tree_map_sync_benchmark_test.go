package chasm

import (
	"strconv"
	"testing"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

var benchmarkMapSyncChildren int

func newMapSyncFixture(tb testing.TB, entries int) (*Node, *TestComponent) {
	tb.Helper()

	backend := &MockNodeBackend{
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	registry := NewRegistry(log.NewNoopLogger())
	if err := registry.Register(newTestLibrary(gomock.NewController(tb))); err != nil {
		tb.Fatalf("register test library: %v", err)
	}
	if err := registry.Register(&CoreLibrary{}); err != nil {
		tb.Fatalf("register core library: %v", err)
	}

	component := &TestComponent{
		ComponentData: &protoMessageType{},
		SubComponents: make(Map[string, *TestSubComponent1], entries),
	}
	for i := range entries {
		component.SubComponents[strconv.Itoa(i)] = NewComponentField(nil, &TestSubComponent1{
			SubComponent1Data: &protoMessageType{},
		})
	}

	root := NewEmptyTree(
		registry,
		clock.NewEventTimeSource(),
		backend,
		DefaultPathEncoder,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err := root.SetRootComponent(component); err != nil {
		tb.Fatalf("set root component: %v", err)
	}

	return root, component
}

func TestNodeSyncSubComponentsMapBehavior(t *testing.T) {
	root, component := newMapSyncFixture(t, 2)
	collection := root.children["SubComponents"]
	if collection == nil {
		t.Fatal("collection node was not created")
	}
	if len(collection.children) != len(component.SubComponents) {
		t.Fatalf("collection child count = %d, want %d", len(collection.children), len(component.SubComponents))
	}

	for key, field := range component.SubComponents {
		if field.Internal.node != collection.children[key] {
			t.Fatalf("map field %q was not rewritten with its collection node", key)
		}
	}

	item := component.SubComponents["0"].Internal.value().(*TestSubComponent1)
	item.SubComponent11 = NewComponentField(nil, &TestSubComponent11{
		SubComponent11Data: &protoMessageType{},
	})
	collection.children["0"].setValueState(valueStateNeedSyncStructure)
	root.setValueState(valueStateNeedSyncStructure)
	if err := root.syncSubComponents(); err != nil {
		t.Fatalf("sync nested component creation: %v", err)
	}
	if collection.children["0"].children["SubComponent11"] == nil {
		t.Fatal("nested component was not created")
	}

	item.SubComponent11 = NewEmptyField[*TestSubComponent11]()
	collection.children["0"].setValueState(valueStateNeedSyncStructure)
	root.setValueState(valueStateNeedSyncStructure)
	if err := root.syncSubComponents(); err != nil {
		t.Fatalf("sync nested component deletion: %v", err)
	}
	if collection.children["0"].children["SubComponent11"] != nil {
		t.Fatal("nested component was not deleted")
	}

	delete(component.SubComponents, "1")
	root.setValueState(valueStateNeedSyncStructure)
	if err := root.syncSubComponents(); err != nil {
		t.Fatalf("sync map item deletion: %v", err)
	}
	if collection.children["1"] != nil {
		t.Fatal("deleted map item remains in the collection node")
	}
	if component.SubComponents["0"].Internal.node != collection.children["0"] {
		t.Fatal("unchanged map item no longer points at its collection node")
	}

	if _, err := root.CloseTransaction(); err != nil {
		t.Fatalf("close transaction after map synchronization: %v", err)
	}
}

func BenchmarkNodeSyncSubComponentsMap(b *testing.B) {
	for _, entries := range []int{0, 1, 16, 128} {
		b.Run("entries="+strconv.Itoa(entries), func(b *testing.B) {
			root, component := newMapSyncFixture(b, entries)
			if entries > 0 && root.children["SubComponents"] == nil {
				b.Fatal("collection node was not created")
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// The caller marks the component before CloseTransaction. Restore that
				// precondition without timing unrelated mutation bookkeeping.
				root.valueState = valueStateNeedSyncStructure
				if err := root.syncSubComponents(); err != nil {
					b.Fatal(err)
				}
				benchmarkMapSyncChildren = len(component.SubComponents)
			}
		})
	}
}
