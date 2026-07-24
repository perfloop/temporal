package frontend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	historyapi "go.temporal.io/server/service/history/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	reverseHistoryPageSize          = primitives.GetHistoryMaxPageSize
	reverseHistoryPageCount         = 8
	reverseHistoryBenchmarkPageSize = 16
	reverseHistoryNamespace         = "reverse-history-benchmark"
	reverseHistoryWorkflow          = "reverse-history-workflow"
	reverseHistoryRun               = "7c177db8-cb3b-4b8d-9385-1f0cfcd6e903"
	reverseHistoryStream            = "StreamWorkflowExecutionHistoryReverse"
)

var reverseHistoryNamespaceID = namespace.ID("5b4f313d-7158-4cc2-a60b-1dc0951f8a4d")

// staticNamespaceRegistry avoids mock overhead in the timed WorkflowHandler path.
type staticNamespaceRegistry struct {
	namespace.Registry
}

func (staticNamespaceRegistry) GetNamespaceID(namespace.Name) (namespace.ID, error) {
	return reverseHistoryNamespaceID, nil
}

func (staticNamespaceRegistry) GetNamespaceName(id namespace.ID) (namespace.Name, error) {
	if id != reverseHistoryNamespaceID {
		return "", fmt.Errorf("unexpected namespace id %q", id)
	}
	return namespace.Name(reverseHistoryNamespace), nil
}

type reverseHistoryStats struct {
	traceEnabled                                                 bool
	public, history, reads, serialized, deserialized, tokenBytes atomic.Int64
	mu                                                           sync.Mutex
	trace                                                        []string
}

func (s *reverseHistoryStats) hop(kind string) {
	if kind == "public" {
		s.public.Add(1)
	} else {
		s.history.Add(1)
	}
	if s.traceEnabled {
		s.mu.Lock()
		s.trace = append(s.trace, kind)
		s.mu.Unlock()
	}
}

func (s *reverseHistoryStats) reset() {
	s.public.Store(0)
	s.history.Store(0)
	s.reads.Store(0)
	s.serialized.Store(0)
	s.deserialized.Store(0)
	s.tokenBytes.Store(0)
	if s.traceEnabled {
		s.mu.Lock()
		s.trace = nil
		s.mu.Unlock()
	}
}

func (s *reverseHistoryStats) waterfall() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.trace, ">")
}

// pageSource is the bounded in-memory persistence double beneath the real
// generated frontend and History gRPC handlers, real History handler, and real
// reverse-history API invocation. The benchmark intentionally excludes storage
// latency while retaining production continuation serialization and pagination.
type pageSource struct {
	pages    [][]*historypb.HistoryEvent
	pageSize int
	stats    *reverseHistoryStats
}

func newPageSource(pages, pageSize int, stats *reverseHistoryStats) *pageSource {
	s := &pageSource{pages: make([][]*historypb.HistoryEvent, pages), pageSize: pageSize, stats: stats}
	for page := range s.pages {
		s.pages[page] = make([]*historypb.HistoryEvent, pageSize)
		for offset := range s.pages[page] {
			s.pages[page][offset] = &historypb.HistoryEvent{
				EventId:   int64(pages*pageSize - page*pageSize - offset),
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			}
		}
	}
	return s
}

func (s *pageSource) total() int { return len(s.pages) * s.pageSize }

func serverOptions(stats *reverseHistoryStats, kind string) []grpc.ServerOption {
	if stats == nil {
		return nil
	}
	hit := func() { stats.hop(kind) }
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			hit()
			return handler(ctx, request)
		}),
		grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			hit()
			return handler(server, stream)
		}),
	}
}

type reverseHistoryFixture struct {
	pages           int
	eventsPerPage   int
	maximumPageSize int32
	seedToken       []byte
	source          *pageSource
	stats           *reverseHistoryStats
	client          workflowservice.WorkflowServiceClient
	conn            *grpc.ClientConn
	stream          bool
}

func newReverseHistoryFixture(tb testing.TB, pages int, record bool) *reverseHistoryFixture {
	var stats *reverseHistoryStats
	if record {
		stats = &reverseHistoryStats{traceEnabled: true}
	}
	return newReverseHistoryFixtureWithStats(tb, pages, reverseHistoryPageSize, 1000, stats)
}

func newMeasuredReverseHistoryFixture(tb testing.TB, pages, eventsPerPage int) *reverseHistoryFixture {
	return newReverseHistoryFixtureWithStats(tb, pages, eventsPerPage, int32(eventsPerPage), &reverseHistoryStats{})
}

func newReverseHistoryFixtureWithStats(tb testing.TB, pages, eventsPerPage int, maximumPageSize int32, stats *reverseHistoryStats) *reverseHistoryFixture {
	tb.Helper()
	source := newPageSource(pages, eventsPerPage, stats)
	seedToken, err := historyapi.SerializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:            reverseHistoryRun,
		FirstEventId:     1,
		NextEventId:      int64(source.total()),
		BranchToken:      reverseHistoryBranchToken,
		PersistenceToken: []byte{0},
	})
	if err != nil {
		tb.Fatal(err)
	}
	historyHandler, err := newReverseHistoryHandler(source)
	if err != nil {
		tb.Fatal(err)
	}
	historyListener := bufconn.Listen(8 * 1024 * 1024)
	historyServer := grpc.NewServer(serverOptions(stats, "history")...)
	historyservice.RegisterHistoryServiceServer(historyServer, historyHandler)
	go func() { _ = historyServer.Serve(historyListener) }()
	historyConn := dial(tb, historyListener)

	handler := &WorkflowHandler{
		config:            NewConfig(dynamicconfig.NewNoopCollection(), 1),
		logger:            log.NewNoopLogger(),
		throttledLogger:   log.NewNoopLogger(),
		namespaceRegistry: staticNamespaceRegistry{},
		historyClient:     historyservice.NewHistoryServiceClient(historyConn),
	}
	publicListener := bufconn.Listen(8 * 1024 * 1024)
	publicServer := grpc.NewServer(serverOptions(stats, "public")...)
	workflowservice.RegisterWorkflowServiceServer(publicServer, handler)
	go func() { _ = publicServer.Serve(publicListener) }()
	publicConn := dial(tb, publicListener)

	fixture := &reverseHistoryFixture{
		pages:           pages,
		eventsPerPage:   eventsPerPage,
		maximumPageSize: maximumPageSize,
		seedToken:       seedToken,
		source:          source,
		stats:           stats,
		client:          workflowservice.NewWorkflowServiceClient(publicConn),
		conn:            publicConn,
		stream:          reflect.ValueOf(handler).MethodByName(reverseHistoryStream).IsValid(),
	}
	tb.Cleanup(func() {
		_ = publicConn.Close()
		publicServer.Stop()
		_ = publicListener.Close()
		_ = historyConn.Close()
		historyServer.Stop()
		_ = historyListener.Close()
	})
	return fixture
}

func dial(tb testing.TB, listener *bufconn.Listener) *grpc.ClientConn {
	tb.Helper()
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		tb.Fatal(err)
	}
	return conn
}

func (f *reverseHistoryFixture) scan(ctx context.Context, callback func(int) bool) ([]*historypb.HistoryEvent, error) {
	if f.stream {
		return f.scanStream(ctx, callback)
	}
	return f.scanUnary(ctx, callback)
}

func (f *reverseHistoryFixture) request() *workflowservice.GetWorkflowExecutionHistoryReverseRequest {
	return &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace:       reverseHistoryNamespace,
		Execution:       &commonpb.WorkflowExecution{WorkflowId: reverseHistoryWorkflow},
		MaximumPageSize: f.maximumPageSize,
		NextPageToken:   append([]byte(nil), f.seedToken...),
	}
}

func (f *reverseHistoryFixture) scanUnary(ctx context.Context, callback func(int) bool) ([]*historypb.HistoryEvent, error) {
	request := f.request()
	var events []*historypb.HistoryEvent
	for page := 0; ; page++ {
		if err := ctx.Err(); err != nil {
			return events, err
		}
		if page > 0 && f.stats != nil && f.stats.traceEnabled {
			continuation, err := historyapi.DeserializeHistoryToken(request.GetNextPageToken())
			if err != nil || continuation.GetRunId() != reverseHistoryRun || continuation.GetFirstEventId() != 1 {
				return events, errors.New("invalid unary continuation token")
			}
			f.stats.deserialized.Add(1)
			f.stats.tokenBytes.Add(int64(len(request.GetNextPageToken())))
		}
		response, err := f.client.GetWorkflowExecutionHistoryReverse(ctx, request)
		if err != nil {
			return events, err
		}
		events = append(events, response.GetHistory().GetEvents()...)
		if callback != nil && !callback(page) {
			return events, nil
		}
		if len(response.GetNextPageToken()) == 0 {
			return events, nil
		}
		if f.stats != nil && f.stats.traceEnabled {
			continuation, err := historyapi.DeserializeHistoryToken(response.GetNextPageToken())
			if err != nil || continuation.GetRunId() != reverseHistoryRun || continuation.GetFirstEventId() != 1 {
				return events, errors.New("invalid unary response continuation token")
			}
			f.stats.serialized.Add(1)
			f.stats.tokenBytes.Add(int64(len(response.GetNextPageToken())))
		}
		request.NextPageToken = response.GetNextPageToken()
	}
}

func (f *reverseHistoryFixture) scanStream(ctx context.Context, callback func(int) bool) ([]*historypb.HistoryEvent, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := f.conn.NewStream(ctx, &grpc.StreamDesc{StreamName: reverseHistoryStream, ServerStreams: true}, "/"+workflowservice.WorkflowService_ServiceDesc.ServiceName+"/"+reverseHistoryStream)
	if err != nil {
		return nil, err
	}
	if err = stream.SendMsg(f.request()); err != nil {
		return nil, err
	}
	if err = stream.CloseSend(); err != nil {
		return nil, err
	}
	var events []*historypb.HistoryEvent
	for page := 0; ; page++ {
		response := new(workflowservice.GetWorkflowExecutionHistoryReverseResponse)
		if err = stream.RecvMsg(response); errors.Is(err, io.EOF) {
			return events, nil
		} else if err != nil {
			return events, err
		}
		if len(response.GetNextPageToken()) != 0 {
			return events, errors.New("stream response carried a continuation token")
		}
		events = append(events, response.GetHistory().GetEvents()...)
		if callback != nil && !callback(page) {
			cancel()
			return events, nil
		}
		if err = ctx.Err(); err != nil {
			return events, err
		}
	}
}

func assertPrefix(t testing.TB, events []*historypb.HistoryEvent, count int, first int64) {
	t.Helper()
	if len(events) != count {
		t.Fatalf("events: got %d want %d", len(events), count)
	}
	for index, event := range events {
		if event.GetEventId() != first-int64(index) || event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			t.Fatalf("event %d is not descending reverse history", index)
		}
	}
}

func assertCalls(t testing.TB, stats *reverseHistoryStats, wantRPCs, wantReads int64) {
	t.Helper()
	if stats.public.Load() != wantRPCs || stats.history.Load() != wantRPCs || stats.reads.Load() != wantReads {
		t.Fatalf("calls public=%d history=%d reads=%d wantRPCs=%d wantReads=%d", stats.public.Load(), stats.history.Load(), stats.reads.Load(), wantRPCs, wantReads)
	}
}

func assertWaterfall(t testing.TB, stats *reverseHistoryStats, calls int) string {
	t.Helper()
	waterfall := stats.waterfall()
	parts := strings.Split(waterfall, ">")
	if len(parts) != calls*2 {
		t.Fatalf("waterfall %q has %d hops want %d", waterfall, len(parts), calls*2)
	}
	for index := 0; index < len(parts); index += 2 {
		if parts[index] != "public" || parts[index+1] != "history" {
			t.Fatalf("waterfall %q is not a serial public>history chain", waterfall)
		}
	}
	return waterfall
}

func canceled(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

func TestReverseHistoryTransportContract(t *testing.T) {
	fixture := newReverseHistoryFixture(t, reverseHistoryPageCount, true)
	events, err := fixture.scan(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefix(t, events, reverseHistoryPageCount*reverseHistoryPageSize, reverseHistoryPageCount*reverseHistoryPageSize)
	calls := int64(reverseHistoryPageCount)
	if fixture.stream {
		calls = 1
	}
	assertCalls(t, fixture.stats, calls, reverseHistoryPageCount)
	fullReads := fixture.stats.reads.Load()
	if fixture.stream {
		if fixture.stats.serialized.Load() != 0 || fixture.stats.deserialized.Load() != 0 || fixture.stats.tokenBytes.Load() != 0 {
			t.Fatal("stream transferred a continuation token")
		}
	} else if fixture.stats.serialized.Load() != reverseHistoryPageCount-1 || fixture.stats.deserialized.Load() != reverseHistoryPageCount-1 || fixture.stats.tokenBytes.Load() == 0 {
		t.Fatal("unary scan did not serialize and restore every continuation")
	}
	waterfall := assertWaterfall(t, fixture.stats, int(calls))

	fixture.stats.reset()
	events, err = fixture.scan(context.Background(), func(int) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	assertPrefix(t, events, reverseHistoryPageSize, reverseHistoryPageCount*reverseHistoryPageSize)
	assertCalls(t, fixture.stats, 1, 1)

	fixture.stats.reset()
	ctx, cancel := context.WithCancel(context.Background())
	events, err = fixture.scan(ctx, func(page int) bool {
		if page == 0 {
			cancel()
		}
		return true
	})
	if !canceled(err) {
		t.Fatalf("cancellation: %v", err)
	}
	assertPrefix(t, events, reverseHistoryPageSize, reverseHistoryPageCount*reverseHistoryPageSize)
	assertCalls(t, fixture.stats, 1, 1)

	t.Logf("reverse_history_transport_contract passed mode=%s pages=%d events=%d public_rpc_count=%d frontend_history_rpc_count=%d fixture_page_source_reads=%d waterfall=%s", map[bool]string{true: "stream", false: "unary"}[fixture.stream], reverseHistoryPageCount, reverseHistoryPageCount*reverseHistoryPageSize, calls, calls, fullReads, waterfall)
}

func TestReverseHistoryUnaryCompatibility(t *testing.T) {
	fixture := newReverseHistoryFixture(t, 2, true)
	events, err := fixture.scanUnary(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefix(t, events, 2*reverseHistoryPageSize, 2*reverseHistoryPageSize)
	assertCalls(t, fixture.stats, 2, 2)
	if fixture.stats.serialized.Load() != 1 || fixture.stats.deserialized.Load() != 1 {
		t.Fatal("unary continuation compatibility failed")
	}
	fixture.stats.reset()
	events, err = fixture.scanUnary(context.Background(), func(int) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	assertPrefix(t, events, reverseHistoryPageSize, 2*reverseHistoryPageSize)
	assertCalls(t, fixture.stats, 1, 1)
	t.Log("reverse_history_unary_compatibility passed pages=2 public_rpc_count=2 frontend_history_rpc_count=2")
}

// benchmarkReverseHistoryScan uses a client-selected, supported 16-event page
// size to isolate repeated public/frontend-to-History transport cost. The
// transport contract above separately drains eight production-cap (256-event) frames.
func benchmarkReverseHistoryScan(b *testing.B, pages int) {
	fixture := newMeasuredReverseHistoryFixture(b, pages, reverseHistoryBenchmarkPageSize)
	if events, err := fixture.scan(context.Background(), nil); err != nil {
		b.Fatal(err)
	} else {
		assertPrefix(b, events, pages*fixture.eventsPerPage, int64(pages*fixture.eventsPerPage))
	}

	var publicRPCs, historyRPCs int64
	b.ResetTimer()
	for b.Loop() {
		fixture.stats.reset()
		events, err := fixture.scan(context.Background(), nil)
		if err != nil || len(events) != pages*fixture.eventsPerPage {
			b.Fatalf("scan events=%d err=%v", len(events), err)
		}
		wantRPCs := int64(pages)
		if fixture.stream {
			wantRPCs = 1
		}
		if fixture.stats.public.Load() != wantRPCs || fixture.stats.history.Load() < wantRPCs || fixture.stats.reads.Load() < int64(pages) {
			b.Fatalf("observed public=%d history=%d reads=%d wantMinimumRPCs=%d wantMinimumReads=%d", fixture.stats.public.Load(), fixture.stats.history.Load(), fixture.stats.reads.Load(), wantRPCs, pages)
		}
		publicRPCs += fixture.stats.public.Load()
		historyRPCs += fixture.stats.history.Load()
	}
	b.StopTimer()
	b.ReportMetric(float64(publicRPCs)/float64(b.N), "public_rpcs/op")
	b.ReportMetric(float64(historyRPCs)/float64(b.N), "frontend_history_rpcs/op")
}

func BenchmarkReverseHistoryControlPlaneManyPages(b *testing.B) {
	benchmarkReverseHistoryScan(b, reverseHistoryPageCount)
}

func BenchmarkReverseHistoryControlPlaneOnePage(b *testing.B) {
	benchmarkReverseHistoryScan(b, 1)
}
