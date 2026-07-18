package history

import (
	"testing"

	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
)

func BenchmarkRespondActivityTaskCompletedTokenHandoff(b *testing.B) {
	handler, mutableState := newActivityCompletionHandoffHandler()
	requests := make([]*activityCompletionHandoffBenchmarkInput, 2)
	for i := range requests {
		request, taskToken, err := newActivityCompletionHandoffRequest(i)
		if err != nil {
			b.Fatal(err)
		}
		requests[i] = &activityCompletionHandoffBenchmarkInput{
			request:   request,
			taskToken: taskToken,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		input := requests[i%len(requests)]
		response, err := handler.RespondActivityTaskCompleted(b.Context(), input.request)
		if err != nil {
			b.Fatal(err)
		}
		if response == nil {
			b.Fatal("RespondActivityTaskCompleted returned a nil response")
		}
		if mutableState.completedRequest != input.request.GetCompleteRequest() ||
			mutableState.completedScheduledEventID != input.taskToken.GetScheduledEventId() {
			b.Fatalf(
				"completion received request %p with scheduled event ID %d, want request %p with scheduled event ID %d",
				mutableState.completedRequest,
				mutableState.completedScheduledEventID,
				input.request.GetCompleteRequest(),
				input.taskToken.GetScheduledEventId(),
			)
		}
	}
}

type activityCompletionHandoffBenchmarkInput struct {
	request   *historyservice.RespondActivityTaskCompletedRequest
	taskToken *tokenspb.Task
}
