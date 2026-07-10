package matching

import (
	"strconv"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
)

func BenchmarkUpdatePollerInfo(b *testing.B) {
	history := newPollerHistory(5 * time.Minute)

	identities := make([]pollerIdentity, 100)
	for i := range 100 {
		identities[i] = pollerIdentity("worker-" + strconv.Itoa(i))
	}

	// Pre-allocated metadata outside the loop to isolate updatePollerInfo allocations
	metadata := &pollMetadata{
		workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId: "1.0",
		},
		deploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			DeploymentName: "test-deployment",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		history.updatePollerInfo(identities[i%100], metadata)
	}
}
