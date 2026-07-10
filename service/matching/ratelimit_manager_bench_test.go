package matching

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tqid"
)

func BenchmarkInjectWorkerRPS(b *testing.B) {
	mockUserDataManager := &mockUserDataManager{}
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("test-namespace", "test-task-queue").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"test-namespace",
	)
	rateLimitManager := newRateLimitManager(mockUserDataManager, config, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	rateLimitManager.mu.Lock()
	rateLimitManager.fairnessKeyRateLimitDefault = nil
	rateLimitManager.mu.Unlock()

	meta := &pollMetadata{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rateLimitManager.InjectWorkerRPS(meta)
	}
}
