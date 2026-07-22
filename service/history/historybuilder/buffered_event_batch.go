package historybuilder

import (
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

// BufferedEventBatch owns the buffered events and their serialized byte total
// across HistoryBuilder instances in one mutable-state lifecycle.
type BufferedEventBatch struct {
	events []*historypb.HistoryEvent
	size   int
}

// NewBufferedEventBatch takes an isolated snapshot of persisted buffered
// events so changes to the source record cannot stale the cached total.
func NewBufferedEventBatch(events []*historypb.HistoryEvent) *BufferedEventBatch {
	clonedEvents := cloneBufferedEvents(events)
	return newBufferedEventBatchFromOwnedEvents(clonedEvents, bufferedEventBatchSize(clonedEvents))
}

func newBufferedEventBatchFromOwnedEvents(events []*historypb.HistoryEvent, size int) *BufferedEventBatch {
	return &BufferedEventBatch{events: events, size: size}
}

// newBufferedEventBatchWithAppendedEvents transfers the opaque persisted
// prefix and copies newly exposed output events into the next cache generation.
func newBufferedEventBatchWithAppendedEvents(
	persistedEvents []*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	size int,
) *BufferedEventBatch {
	events := append(persistedEvents, cloneBufferedEvents(newEvents)...)
	return newBufferedEventBatchFromOwnedEvents(events, size)
}

// CloneEvents returns an isolated copy suitable for persistence output.
func (b *BufferedEventBatch) CloneEvents() []*historypb.HistoryEvent {
	if b == nil {
		return nil
	}
	return cloneBufferedEvents(b.events)
}

// StampPrincipalOnLastEvents applies the active transaction's principal to
// newly buffered events while keeping the cache's serialized total exact.
func (b *BufferedEventBatch) StampPrincipalOnLastEvents(count int, principal *commonpb.Principal) {
	if b == nil || count == 0 {
		return
	}

	cachedPrincipal := principal
	if principal != nil {
		cachedPrincipal = proto.Clone(principal).(*commonpb.Principal)
	}
	for _, event := range b.events[len(b.events)-count:] {
		if event.Principal == nil && cachedPrincipal == nil {
			continue
		}
		oldSize := proto.Size(event)
		event.Principal = cachedPrincipal
		b.size += proto.Size(event) - oldSize
	}
}

func cloneBufferedEvents(events []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	if events == nil {
		return nil
	}
	clonedEvents := make([]*historypb.HistoryEvent, len(events))
	for i, event := range events {
		if event != nil {
			clonedEvents[i] = proto.Clone(event).(*historypb.HistoryEvent)
		}
	}
	return clonedEvents
}

func bufferedEventBatchSize(events []*historypb.HistoryEvent) int {
	size := 0
	for _, event := range events {
		size += proto.Size(event)
	}
	return size
}
