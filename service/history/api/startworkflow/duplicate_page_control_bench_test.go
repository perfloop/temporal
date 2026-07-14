package startworkflow

import (
	"errors"
	"testing"
)

func BenchmarkStarterGetWorkflowHistoryDuplicatePageRequestControl(b *testing.B) {
	duplicatePageRequests := 0
	for range b.N {
		control, ctrl := newDuplicatePageRequestControl(b)
		events, err := control.getWorkflowHistory()
		ctrl.Finish()
		if errors.Is(err, errDuplicatePageRequest) {
			if control.duplicatePageRequests != 1 {
				b.Fatalf("duplicate_page_requests/op = %d, want 1", control.duplicatePageRequests)
			}
			duplicatePageRequests++
			continue
		}
		if err != nil {
			b.Fatalf("getWorkflowHistory returned error: %v", err)
		}
		if control.duplicatePageRequests != 0 || len(events) != 2 || events[0].GetEventId() != 1 || events[1].GetEventId() != 2 {
			b.Fatalf("duplicate_page_requests/op = %d, events = %#v", control.duplicatePageRequests, events)
		}
	}
	b.ReportMetric(float64(duplicatePageRequests)/float64(b.N), "duplicate_page_requests/op")
}
