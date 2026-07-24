package frontend

import (
	"bytes"
	"context"
	"fmt"

	oteltrace "go.opentelemetry.io/otel/trace"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	visibilitymanager "go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/common/searchattribute"
	history "go.temporal.io/server/service/history"
	getreverse "go.temporal.io/server/service/history/api/getworkflowexecutionhistoryreverse"
	historyconfigs "go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/fx"
)

var reverseHistoryBranchToken = []byte("reverse-history-benchmark-branch")

type reverseHistoryStore struct {
	persistence.ExecutionManager
	source *pageSource
}

func (s *reverseHistoryStore) ReadHistoryBranchReverse(
	ctx context.Context,
	request *persistence.ReadHistoryBranchReverseRequest,
) (*persistence.ReadHistoryBranchReverseResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.PageSize != s.source.pageSize || !bytes.Equal(request.BranchToken, reverseHistoryBranchToken) {
		return nil, fmt.Errorf("reverse-history persistence request page_size=%d branch=%x", request.PageSize, request.BranchToken)
	}
	page := 0
	if len(request.NextPageToken) != 0 {
		page = int(request.NextPageToken[0])
	}
	if page < 0 || page >= len(s.source.pages) {
		return nil, fmt.Errorf("reverse-history persistence page %d", page)
	}
	if request.MaxEventID != int64(s.source.total()-page*s.source.pageSize) {
		return nil, fmt.Errorf("reverse-history max event id %d", request.MaxEventID)
	}
	var next []byte
	if page+1 < len(s.source.pages) {
		next = []byte{byte(page + 1)}
	}
	s.source.stats.reads.Add(1)
	return &persistence.ReadHistoryBranchReverseResponse{
		HistoryEvents: s.source.pages[page],
		NextPageToken: next,
		Size:          len(s.source.pages[page]),
	}, nil
}

type reverseHistorySearchAttributes struct{}

func (reverseHistorySearchAttributes) GetSearchAttributes(string, bool) (searchattribute.NameTypeMap, error) {
	return searchattribute.NewNameTypeMap(nil), nil
}

type reverseHistoryVisibilityManager struct {
	visibilitymanager.VisibilityManager
}

func (reverseHistoryVisibilityManager) GetIndexName() string { return "" }

type reverseHistoryShardContext struct {
	historyi.ShardContext
	engine historyi.Engine
	store  persistence.ExecutionManager
	config *historyconfigs.Config
}

func (s *reverseHistoryShardContext) GetEngine(context.Context) (historyi.Engine, error) {
	return s.engine, nil
}

func (s *reverseHistoryShardContext) GetNamespaceRegistry() namespace.Registry {
	return staticNamespaceRegistry{}
}

func (s *reverseHistoryShardContext) GetConfig() *historyconfigs.Config { return s.config }

func (s *reverseHistoryShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.store
}

func (s *reverseHistoryShardContext) GetLogger() log.Logger { return log.NewNoopLogger() }

func (s *reverseHistoryShardContext) GetSearchAttributesProvider() searchattribute.Provider {
	return reverseHistorySearchAttributes{}
}

func (*reverseHistoryShardContext) GetSearchAttributesMapperProvider() searchattribute.MapperProvider {
	return nil
}

type reverseHistoryEngine struct {
	historyi.Engine
	shardContext historyi.ShardContext
	visibility   visibilitymanager.VisibilityManager
}

func (e *reverseHistoryEngine) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	return getreverse.Invoke(ctx, e.shardContext, nil, nil, request, e.visibility)
}

type reverseHistoryController struct{ shard historyi.ShardContext }

func (c *reverseHistoryController) GetPingChecks() []pingable.Check { return nil }

func (c *reverseHistoryController) GetShardByID(int32) (historyi.ShardContext, error) {
	return c.shard, nil
}

func (c *reverseHistoryController) GetShardByNamespaceWorkflow(namespace.ID, string) (historyi.ShardContext, error) {
	return c.shard, nil
}

func (*reverseHistoryController) CloseShardByID(int32) {}

func (*reverseHistoryController) ShardIDs() []int32 { return []int32{1} }

func (*reverseHistoryController) Start() {}

func (*reverseHistoryController) Stop() {}

func (*reverseHistoryController) InitialShardsAcquired(context.Context) error { return nil }

type reverseHistoryLifecycle struct{}

func (reverseHistoryLifecycle) Append(fx.Hook) {}

func newReverseHistoryHandler(source *pageSource) (*history.Handler, error) {
	store := &reverseHistoryStore{source: source}
	shardContext := &reverseHistoryShardContext{
		store:  store,
		config: &historyconfigs.Config{NumberOfShards: 1},
	}
	visibility := reverseHistoryVisibilityManager{}
	shardContext.engine = &reverseHistoryEngine{shardContext: shardContext, visibility: visibility}
	return history.HandlerProvider(history.NewHandlerArgs{
		Config:          shardContext.config,
		Logger:          log.NewNoopLogger(),
		ThrottledLogger: log.NewNoopLogger(),
		ShardController: &reverseHistoryController{shard: shardContext},
		TracerProvider:  oteltrace.NewNoopTracerProvider(),
	}, reverseHistoryLifecycle{})
}
