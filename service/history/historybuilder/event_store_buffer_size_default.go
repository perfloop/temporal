//go:build !perfloop_buffer_size_counter

package historybuilder

import (
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

// bufferedEventProtoSize is kept as a small wrapper so the perfloop counter
// build can count exact protobuf sizing calls without changing the benchmark
// build. In ordinary builds this wrapper is inlined to proto.Size.
func bufferedEventProtoSize(event *historypb.HistoryEvent) int {
	return proto.Size(event)
}
