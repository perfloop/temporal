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
	reverseHistoryPageSize  = primitives.GetHistoryMaxPageSize
	reverseHistoryPageCount = 16
	reverseHistoryNamespace = "reverse-history-benchmark"
	reverseHistoryWorkflow  = "reverse-history-workflow"
	reverseHistoryRun       = "7c177db8-cb3b-4b8d-9385-1f0cfcd6e903"
	reverseHistoryStream    = "StreamWorkflowExecutionHistoryReverse"
)

var reverseHistoryNamespaceID = namespace.ID("5b4f313d-7158-4cc2-a60b-1dc0951f8a4d")

// staticNamespaceRegistry avoids mock overhead in the timed WorkflowHandler path.
type staticNamespaceRegistry struct {
	namespace.Registry
}

func (staticNamespaceRegistry) GetNamespaceID(namespace.Name) (namespace.ID, error) {
	return reverseHistoryNamespaceID, nil
}

type reverseHistoryStats struct {
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
	s.mu.Lock()
	s.trace = append(s.trace, kind)
	s.mu.Unlock()
}

func (s *reverseHistoryStats) reset() {
	s.public.Store(0)
	s.history.Store(0)
	s.reads.Store(0)
	s.serialized.Store(0)
	s.deserialized.Store(0)
	s.tokenBytes.Store(0)
	s.mu.Lock()
	s.trace = nil
	s.mu.Unlock()
}

func (s *reverseHistoryStats) waterfall() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.trace, ">")
}

// pageSource is deliberately an in-memory page source: this proof measures the
// public/frontend-to-History control plane, not persistence latency. It does use
// the production HistoryContinuation serializer on every unary continuation.
type pageSource struct {
	pages   [][]*historypb.HistoryEvent
	stats   *reverseHistoryStats
	permits chan struct{}
	done    chan error
}

func newPageSource(pages int, stats *reverseHistoryStats) *pageSource {
	s := &pageSource{pages: make([][]*historypb.HistoryEvent, pages), stats: stats}
	for page := range s.pages {
		s.pages[page] = make([]*historypb.HistoryEvent, reverseHistoryPageSize)
		for offset := range s.pages[page] {
			s.pages[page][offset] = &historypb.HistoryEvent{
				EventId:   int64(pages*reverseHistoryPageSize - page*reverseHistoryPageSize - offset),
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			}
		}
	}
	if stats != nil {
		s.permits = make(chan struct{}, 1)
		s.done = make(chan error, 3)
	}
	return s
}

func (s *pageSource) total() int { return len(s.pages) * reverseHistoryPageSize }

func (s *pageSource) validate(request *historyservice.GetWorkflowExecutionHistoryReverseRequest) error {
	if request.GetNamespaceId() != reverseHistoryNamespaceID.String() || request.GetRequest() == nil {
		return errors.New("unexpected reverse-history request")
	}
	if request.GetRequest().GetExecution().GetWorkflowId() != reverseHistoryWorkflow {
		return errors.New("unexpected reverse-history workflow")
	}
	if request.GetRequest().GetMaximumPageSize() != reverseHistoryPageSize {
		return fmt.Errorf("frontend page cap: got %d want %d", request.GetRequest().GetMaximumPageSize(), reverseHistoryPageSize)
	}
	return nil
}

func (s *pageSource) index(token []byte) (int, error) {
	if len(token) == 0 {
		return 0, nil
	}
	continuation, err := historyapi.DeserializeHistoryToken(token)
	if err != nil || continuation.GetRunId() != reverseHistoryRun || continuation.GetFirstEventId() != 1 {
		return 0, errors.New("invalid continuation token")
	}
	index := int((int64(s.total()) - continuation.GetNextEventId()) / reverseHistoryPageSize)
	if index <= 0 || index >= len(s.pages) {
		return 0, fmt.Errorf("invalid continuation page %d", index)
	}
	if s.stats != nil {
		s.stats.deserialized.Add(1)
		s.stats.tokenBytes.Add(int64(len(token)))
	}
	return index, nil
}

func (s *pageSource) token(next int) ([]byte, error) {
	if next == len(s.pages) {
		return nil, nil
	}
	token, err := historyapi.SerializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:            reverseHistoryRun,
		FirstEventId:     1,
		NextEventId:      int64(s.total() - next*reverseHistoryPageSize),
		BranchToken:      []byte("reverse-history-benchmark-branch"),
		PersistenceToken: []byte{byte(next), 1, 2, 3, 4, 5, 6, 7},
	})
	if err == nil && s.stats != nil {
		s.stats.serialized.Add(1)
		s.stats.tokenBytes.Add(int64(len(token)))
	}
	return token, err
}

func (s *pageSource) response(index int, continuation bool) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	if index < 0 || index >= len(s.pages) {
		return nil, fmt.Errorf("invalid page %d", index)
	}
	var token []byte
	var err error
	if continuation {
		token, err = s.token(index + 1)
		if err != nil {
			return nil, err
		}
	}
	if s.stats != nil {
		s.stats.reads.Add(1)
	}
	return &historyservice.GetWorkflowExecutionHistoryReverseResponse{Response: &workflowservice.GetWorkflowExecutionHistoryReverseResponse{
		History:       &historypb.History{Events: s.pages[index]},
		NextPageToken: token,
	}}, nil
}

func (s *pageSource) unary(request *historyservice.GetWorkflowExecutionHistoryReverseRequest) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	if err := s.validate(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	index, err := s.index(request.GetRequest().GetNextPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response, err := s.response(index, true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return response, nil
}

func (s *pageSource) stream(ctx context.Context, request *historyservice.GetWorkflowExecutionHistoryReverseRequest, send func(*historyservice.GetWorkflowExecutionHistoryReverseResponse) error) (err error) {
	if s.done != nil {
		defer func() { s.done <- err }()
	}
	if err = s.validate(request); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if len(request.GetRequest().GetNextPageToken()) != 0 {
		return status.Error(codes.InvalidArgument, "stream starts without a continuation")
	}
	for page := range s.pages {
		if page > 0 && s.permits != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.permits:
			}
		}
		response, responseErr := s.response(page, false)
		if responseErr != nil {
			return responseErr
		}
		if err = send(response); err != nil {
			return err
		}
	}
	return nil
}

func (s *pageSource) permit() {
	if s.permits != nil {
		s.permits <- struct{}{}
	}
}

func (s *pageSource) wait(t testing.TB) error {
	t.Helper()
	return <-s.done
}

type pageServer struct {
	historyservice.UnimplementedHistoryServiceServer
	source *pageSource
}

func (s *pageServer) GetWorkflowExecutionHistoryReverse(ctx context.Context, request *historyservice.GetWorkflowExecutionHistoryReverseRequest) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.source.unary(request)
}

func historyStreamHandler(server any, stream grpc.ServerStream) error {
	request := new(historyservice.GetWorkflowExecutionHistoryReverseRequest)
	if err := stream.RecvMsg(request); err != nil {
		return err
	}
	return server.(*pageServer).source.stream(stream.Context(), request, func(response *historyservice.GetWorkflowExecutionHistoryReverseResponse) error {
		return stream.SendMsg(response)
	})
}

type publicStreamServer struct{ grpc.ServerStream }

func (s *publicStreamServer) Send(response *workflowservice.GetWorkflowExecutionHistoryReverseResponse) error {
	return s.SendMsg(response)
}

func publicStreamHandler(server any, stream grpc.ServerStream) (err error) {
	request := new(workflowservice.GetWorkflowExecutionHistoryReverseRequest)
	if err = stream.RecvMsg(request); err != nil {
		return err
	}
	method := reflect.ValueOf(server).MethodByName(reverseHistoryStream)
	if !method.IsValid() {
		return status.Error(codes.Unimplemented, "reverse-history stream unavailable")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = status.Errorf(codes.Internal, "reverse-history stream signature: %v", recovered)
		}
	}()
	result := method.Call([]reflect.Value{reflect.ValueOf(request), reflect.ValueOf(&publicStreamServer{stream})})
	if len(result) != 1 || !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func descriptor(base grpc.ServiceDesc, handler grpc.StreamHandler) grpc.ServiceDesc {
	base.Methods = append([]grpc.MethodDesc(nil), base.Methods...)
	base.Streams = append([]grpc.StreamDesc(nil), base.Streams...)
	for index := range base.Streams {
		if base.Streams[index].StreamName == reverseHistoryStream {
			base.Streams[index].Handler = handler
			base.Streams[index].ServerStreams = true
			base.Streams[index].ClientStreams = false
			return base
		}
	}
	base.Streams = append(base.Streams, grpc.StreamDesc{StreamName: reverseHistoryStream, Handler: handler, ServerStreams: true})
	return base
}

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
	pages  int
	source *pageSource
	stats  *reverseHistoryStats
	client workflowservice.WorkflowServiceClient
	conn   *grpc.ClientConn
	stream bool
}

func newReverseHistoryFixture(tb testing.TB, pages int, record bool) *reverseHistoryFixture {
	tb.Helper()
	var stats *reverseHistoryStats
	if record {
		stats = &reverseHistoryStats{}
	}
	source := newPageSource(pages, stats)
	historyListener := bufconn.Listen(8 * 1024 * 1024)
	historyServer := grpc.NewServer(serverOptions(stats, "history")...)
	historyDescriptor := descriptor(historyservice.HistoryService_ServiceDesc, historyStreamHandler)
	historyServer.RegisterService(&historyDescriptor, &pageServer{source: source})
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
	publicDescriptor := descriptor(workflowservice.WorkflowService_ServiceDesc, publicStreamHandler)
	publicServer.RegisterService(&publicDescriptor, handler)
	go func() { _ = publicServer.Serve(publicListener) }()
	publicConn := dial(tb, publicListener)

	fixture := &reverseHistoryFixture{
		pages:  pages,
		source: source,
		stats:  stats,
		client: workflowservice.NewWorkflowServiceClient(publicConn),
		conn:   publicConn,
		stream: reflect.ValueOf(handler).MethodByName(reverseHistoryStream).IsValid(),
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
		MaximumPageSize: 1000, // the frontend must cap this to 256.
	}
}

func (f *reverseHistoryFixture) scanUnary(ctx context.Context, callback func(int) bool) ([]*historypb.HistoryEvent, error) {
	request := f.request()
	var events []*historypb.HistoryEvent
	for page := 0; ; page++ {
		if err := ctx.Err(); err != nil {
			return events, err
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
		events = append(events, response.GetHistory().GetEvents()...)
		if callback != nil && !callback(page) {
			cancel()
			return events, nil
		}
		if err = ctx.Err(); err != nil {
			return events, err
		}
		if page+1 < f.pages {
			f.source.permit()
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

func assertCalls(t testing.TB, stats *reverseHistoryStats, want int64) {
	t.Helper()
	if stats.public.Load() != want || stats.history.Load() != want || stats.reads.Load() != want {
		t.Fatalf("calls public=%d history=%d reads=%d want=%d", stats.public.Load(), stats.history.Load(), stats.reads.Load(), want)
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
	assertCalls(t, fixture.stats, calls)
	if fixture.stream {
		if err := fixture.source.wait(t); err != nil {
			t.Fatal(err)
		}
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
	assertCalls(t, fixture.stats, 1)
	if fixture.stream {
		if err := fixture.source.wait(t); err != nil && !canceled(err) {
			t.Fatal(err)
		}
	}

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
	assertCalls(t, fixture.stats, 1)
	if fixture.stream {
		if err := fixture.source.wait(t); err != nil && !canceled(err) {
			t.Fatal(err)
		}
	}

	t.Logf("reverse_history_transport_contract passed mode=%s pages=16 events=4096 public_rpc_count=%d frontend_history_rpc_count=%d fixture_page_source_reads=%d waterfall=%s", map[bool]string{true: "stream", false: "unary"}[fixture.stream], calls, calls, calls, waterfall)
}

func TestReverseHistoryUnaryCompatibility(t *testing.T) {
	fixture := newReverseHistoryFixture(t, 2, true)
	events, err := fixture.scanUnary(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefix(t, events, 2*reverseHistoryPageSize, 2*reverseHistoryPageSize)
	assertCalls(t, fixture.stats, 2)
	if fixture.stats.serialized.Load() != 1 || fixture.stats.deserialized.Load() != 1 {
		t.Fatal("unary continuation compatibility failed")
	}
	fixture.stats.reset()
	events, err = fixture.scanUnary(context.Background(), func(int) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	assertPrefix(t, events, reverseHistoryPageSize, 2*reverseHistoryPageSize)
	assertCalls(t, fixture.stats, 1)
	t.Log("reverse_history_unary_compatibility passed pages=2 public_rpc_count=2 frontend_history_rpc_count=2")
}

func BenchmarkReverseHistoryFullScanManyPages(b *testing.B) {
	fixture := newReverseHistoryFixture(b, reverseHistoryPageCount, false)
	if events, err := fixture.scan(context.Background(), nil); err != nil {
		b.Fatal(err)
	} else {
		assertPrefix(b, events, reverseHistoryPageCount*reverseHistoryPageSize, reverseHistoryPageCount*reverseHistoryPageSize)
	}
	b.ResetTimer()
	for b.Loop() {
		events, err := fixture.scan(context.Background(), nil)
		if err != nil || len(events) != reverseHistoryPageCount*reverseHistoryPageSize {
			b.Fatalf("scan events=%d err=%v", len(events), err)
		}
	}
}

func BenchmarkReverseHistoryFullScanOnePage(b *testing.B) {
	fixture := newReverseHistoryFixture(b, 1, false)
	if events, err := fixture.scan(context.Background(), nil); err != nil {
		b.Fatal(err)
	} else {
		assertPrefix(b, events, reverseHistoryPageSize, reverseHistoryPageSize)
	}
	b.ResetTimer()
	for b.Loop() {
		events, err := fixture.scan(context.Background(), nil)
		if err != nil || len(events) != reverseHistoryPageSize {
			b.Fatalf("scan events=%d err=%v", len(events), err)
		}
	}
}
