package temporaltest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/temporaltest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

func BenchmarkEagerStartConflict_RealDB(b *testing.B) {
	ts := temporaltest.NewServer()
	defer ts.Stop()

	// Register a simple workflow and start worker
	ts.NewWorker("benchmark-task-queue", func(registry worker.Registry) {
		registry.RegisterWorkflow(func(ctx workflow.Context) error {
			return nil
		})
	})

	// Get a gRPC connection to the server
	conn, err := grpc.Dial(ts.GetFrontendHostPort(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	client := workflowservice.NewWorkflowServiceClient(conn)
	namespace := ts.GetDefaultNamespace()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		workflowID := fmt.Sprintf("benchmark-workflow-%d", i)

		// 1. Start the first workflow
		_, err := client.StartWorkflowExecution(context.Background(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:             namespace,
			WorkflowId:            workflowID,
			WorkflowType:          &commonpb.WorkflowType{Name: "Workflow"},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: "benchmark-task-queue"},
			WorkflowRunTimeout:   durationpb.New(10 * time.Second),
			WorkflowExecutionTimeout: durationpb.New(10 * time.Second),
			Identity:              "benchmark-bot",
			RequestId:             uuid.NewString(),
		})
		if err != nil {
			b.Fatal(err)
		}

		// 2. Start the second workflow with same WorkflowId, ALLOW_DUPLICATE reuse policy, TERMINATE_EXISTING conflict policy, and eager execution true!
		_, err = client.StartWorkflowExecution(context.Background(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespace,
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: "Workflow"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "benchmark-task-queue"},
			WorkflowRunTimeout:      durationpb.New(10 * time.Second),
			WorkflowExecutionTimeout: durationpb.New(10 * time.Second),
			Identity:                 "benchmark-bot",
			RequestId:                uuid.NewString(),
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
			RequestEagerExecution:   true,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
