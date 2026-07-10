package matching

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	pollerHistoryInitMaxSize = 1000
)

type (
	pollerIdentity string

	pollerInfo struct {
		ratePerSecond             float64
		workerVersionCapabilities *commonpb.WorkerVersionCapabilities
		deploymentOptions         *deploymentpb.WorkerDeploymentOptions
	}
)

type pollerHistory struct {
	// poller ID -> pollerInfo
	// pollers map[pollerID]pollerInfo
	history cache.Cache
}

func newPollerHistory(pollerHistoryTTL time.Duration) *pollerHistory {
	opts := &cache.Options{
		TTL: pollerHistoryTTL,
		Pin: false,
	}

	return &pollerHistory{
		history: cache.New(pollerHistoryInitMaxSize, opts),
	}
}

func (pollers *pollerHistory) updatePollerInfo(id pollerIdentity, pollMetadata *pollMetadata) {
	if pollMetadata == nil {
		return
	}
	var ratePerSecond float64
	if pollMetadata.taskQueueMetadata != nil {
		ratePerSecond = defaultRPS(pollMetadata.taskQueueMetadata.GetMaxTasksPerSecond())
	} else {
		ratePerSecond = defaultTaskDispatchRPS
	}

	pollers.history.Put(id, &pollerInfo{
		ratePerSecond:             ratePerSecond,
		workerVersionCapabilities: pollMetadata.workerVersionCapabilities,
		deploymentOptions:         pollMetadata.deploymentOptions,
	})
}

func (pollers *pollerHistory) removePoller(id pollerIdentity) {
	pollers.history.Delete(id)
}

func (pollers *pollerHistory) getPollerInfo(earliestAccessTime time.Time) []*taskqueuepb.PollerInfo {
	var result []*taskqueuepb.PollerInfo

	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		key := entry.Key().(pollerIdentity)
		if value, ok := entry.Value().(*pollerInfo); ok {
			lastAccessTime := entry.CreateTime()
			if earliestAccessTime.Before(lastAccessTime) {
				result = append(result, &taskqueuepb.PollerInfo{
					Identity:                  string(key),
					LastAccessTime:            timestamppb.New(lastAccessTime),
					RatePerSecond:             value.ratePerSecond,
					WorkerVersionCapabilities: value.workerVersionCapabilities,
					DeploymentOptions:         value.deploymentOptions,
				})
			}
		}
	}

	return result
}

func defaultRPS(wrapper *wrapperspb.DoubleValue) float64 {
	if wrapper != nil {
		return wrapper.Value
	}
	return defaultTaskDispatchRPS
}
