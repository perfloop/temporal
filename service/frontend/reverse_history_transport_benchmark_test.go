package frontend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
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
	reverseHistoryBenchmarkPageSize  = primitives.GetHistoryMaxPageSize
	reverseHistoryBenchmarkPageCount = 16
	reverseHistoryBenchmarkNamespace = "reverse-history-benchmark"
	reverseHistoryBenchmarkWorkflow  = "reverse-history-workflow"
	reverseHistoryBenchmarkRun       = "7c177db8-cb3b-4b8d-9385-1f0cfcd6e903"

	reverseHistoryStreamMethod = "StreamWorkflowExecutionHistoryReverse"
)

var reverseHistoryBenchmarkNamespaceID = namespace.ID("5b4f313d-7158-4cc2-a60b-1dc0951f8a4d")

type reverseHistoryTransportMode string

const (
	reverseHistoryUnaryMode  reverseHistoryTransportMode = "unary"
	reverseHistoryStreamMode reverseHistoryTransportMode = "stream"
)

// reverseHistoryStaticNamespaceRegistry supplies the only Registry operation used by
// WorkflowHandler.GetWorkflowExecutionHistoryReverse. Embedding the interface keeps
// the fixture focused on the route under test without introducing a mock call into the
// timed path.
type reverseHistoryStaticNamespaceRegistry struct {
	namespace.Registry
	id namespace.ID
}

func (r reverseHistoryStaticNamespaceRegistry) GetNamespaceID(namespace.Name) (namespace.ID, error) {
	return r.id, nil
}

type reverseHistoryTraceSpan struct {
	id    int64
	kind  string
	start time.Time
	end   time.Time
}

type reverseHistoryMeasurements struct {
	publicRPCCalls  atomic.Int64
	historyRPCCalls atomic.Int64

	continuationSerializations   atomic.Int64
	continuationDeserializations atomic.Int64
	continuationWireBytes        atomic.Int64
	sourcePageReads              atomic.Int64
	nextSpanID                   atomic.Int64

	mu    sync.Mutex
	spans []reverseHistoryTraceSpan
}

type reverseHistoryMeasurementSnapshot struct {
	publicRPCCalls               int64
	historyRPCCalls              int64
	continuationSerializations   int64
	continuationDeserializations int64
	continuationWireBytes        int64
	sourcePageReads              int64
	spans                        []reverseHistoryTraceSpan
}

func (m *reverseHistoryMeasurements) reset() {
	m.publicRPCCalls.Store(0)
	m.historyRPCCalls.Store(0)
	m.continuationSerializations.Store(0)
	m.continuationDeserializations.Store(0)
	m.continuationWireBytes.Store(0)
	m.sourcePageReads.Store(0)
	m.nextSpanID.Store(0)
	m.mu.Lock()
	m.spans = nil
	m.mu.Unlock()
}

func (m *reverseHistoryMeasurements) recordSpan(kind string, run func() error) error {
	id := m.nextSpanID.Add(1)
	start := time.Now()
	err := run()
	m.mu.Lock()
	m.spans = append(m.spans, reverseHistoryTraceSpan{
		id:    id,
		kind:  kind,
		start: start,
		end:   time.Now(),
	})
	m.mu.Unlock()
	return err
}

func (m *reverseHistoryMeasurements) snapshot() reverseHistoryMeasurementSnapshot {
	m.mu.Lock()
	spans := append([]reverseHistoryTraceSpan(nil), m.spans...)
	m.mu.Unlock()
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].id < spans[j].id
	})
	return reverseHistoryMeasurementSnapshot{
		publicRPCCalls:               m.publicRPCCalls.Load(),
		historyRPCCalls:              m.historyRPCCalls.Load(),
		continuationSerializations:   m.continuationSerializations.Load(),
		continuationDeserializations: m.continuationDeserializations.Load(),
		continuationWireBytes:        m.continuationWireBytes.Load(),
		sourcePageReads:              m.sourcePageReads.Load(),
		spans:                        spans,
	}
}

type reverseHistoryPageSource struct {
	pages         [][]*historypb.HistoryEvent
	pageSize      int
	branchToken   []byte
	measurements  *reverseHistoryMeasurements
	gateStream    bool
	streamPermits chan struct{}
	streamDone    chan error
}

func newReverseHistoryPageSource(
	pageCount int,
	pageSize int,
	measurements *reverseHistoryMeasurements,
	gateStream bool,
) *reverseHistoryPageSource {
	pages := make([][]*historypb.HistoryEvent, pageCount)
	for page := range pages {
		pages[page] = make([]*historypb.HistoryEvent, pageSize)
		for eventOffset := range pages[page] {
			eventID := int64(pageCount*pageSize - (page*pageSize + eventOffset))
			pages[page][eventOffset] = &historypb.HistoryEvent{
				EventId:   eventID,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			}
		}
	}

	source := &reverseHistoryPageSource{
		pages:        pages,
		pageSize:     pageSize,
		branchToken:  []byte("reverse-history-benchmark-branch-token"),
		measurements: measurements,
		gateStream:   gateStream,
	}
	if gateStream {
		source.streamPermits = make(chan struct{}, 1)
		source.streamDone = make(chan error, 4)
	}
	return source
}

func (s *reverseHistoryPageSource) pageCount() int {
	return len(s.pages)
}

func (s *reverseHistoryPageSource) totalEvents() int {
	return s.pageCount() * s.pageSize
}

func (s *reverseHistoryPageSource) validateRequest(request *historyservice.GetWorkflowExecutionHistoryReverseRequest) error {
	if request.GetNamespaceId() != reverseHistoryBenchmarkNamespaceID.String() {
		return fmt.Errorf("unexpected namespace ID %q", request.GetNamespaceId())
	}
	if request.GetRequest() == nil {
		return errors.New("missing reverse-history request")
	}
	if request.GetRequest().GetExecution().GetWorkflowId() != reverseHistoryBenchmarkWorkflow {
		return fmt.Errorf("unexpected workflow ID %q", request.GetRequest().GetExecution().GetWorkflowId())
	}
	if request.GetRequest().GetMaximumPageSize() != reverseHistoryBenchmarkPageSize {
		return fmt.Errorf(
			"frontend did not apply page-size cap: got %d want %d",
			request.GetRequest().GetMaximumPageSize(),
			reverseHistoryBenchmarkPageSize,
		)
	}
	return nil
}

func (s *reverseHistoryPageSource) pageIndex(token []byte) (int, error) {
	if len(token) == 0 {
		return 0, nil
	}
	continuation, err := historyapi.DeserializeHistoryToken(token)
	if err != nil {
		return 0, fmt.Errorf("deserialize continuation: %w", err)
	}
	if continuation.GetRunId() != reverseHistoryBenchmarkRun {
		return 0, fmt.Errorf("unexpected continuation run ID %q", continuation.GetRunId())
	}
	if continuation.GetFirstEventId() != 1 {
		return 0, fmt.Errorf("unexpected continuation first event ID %d", continuation.GetFirstEventId())
	}
	nextEventID := continuation.GetNextEventId()
	if nextEventID <= 0 || nextEventID >= int64(s.totalEvents()) {
		return 0, fmt.Errorf("unexpected continuation next event ID %d", nextEventID)
	}
	page := int((int64(s.totalEvents()) - nextEventID) / int64(s.pageSize))
	if page <= 0 || page >= s.pageCount() {
		return 0, fmt.Errorf("continuation resolves to invalid page %d", page)
	}
	if s.measurements != nil {
		s.measurements.continuationDeserializations.Add(1)
		s.measurements.continuationWireBytes.Add(int64(len(token)))
	}
	return page, nil
}

func (s *reverseHistoryPageSource) nextPageToken(nextPage int) ([]byte, error) {
	if nextPage >= s.pageCount() {
		return nil, nil
	}
	persistenceToken := make([]byte, 48)
	for i := range persistenceToken {
		persistenceToken[i] = byte(nextPage + i)
	}
	continuation := &tokenspb.HistoryContinuation{
		RunId:              reverseHistoryBenchmarkRun,
		FirstEventId:       1,
		NextEventId:        int64(s.totalEvents() - nextPage*s.pageSize),
		PersistenceToken:   persistenceToken,
		BranchToken:        s.branchToken,
		VersionHistoryItem: &historyspb.VersionHistoryItem{EventId: int64(s.totalEvents()), Version: 1},
	}
	token, err := historyapi.SerializeHistoryToken(continuation)
	if err != nil {
		return nil, fmt.Errorf("serialize continuation: %w", err)
	}
	if s.measurements != nil {
		s.measurements.continuationSerializations.Add(1)
		s.measurements.continuationWireBytes.Add(int64(len(token)))
	}
	return token, nil
}

func (s *reverseHistoryPageSource) response(page int, withContinuation bool) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	if page < 0 || page >= s.pageCount() {
		return nil, fmt.Errorf("page index %d outside [0,%d)", page, s.pageCount())
	}
	if s.measurements != nil {
		s.measurements.sourcePageReads.Add(1)
	}
	var nextPageToken []byte
	var err error
	if withContinuation {
		nextPageToken, err = s.nextPageToken(page + 1)
		if err != nil {
			return nil, err
		}
	}
	return &historyservice.GetWorkflowExecutionHistoryReverseResponse{
		Response: &workflowservice.GetWorkflowExecutionHistoryReverseResponse{
			History:       &historypb.History{Events: s.pages[page]},
			NextPageToken: nextPageToken,
		},
	}, nil
}

func (s *reverseHistoryPageSource) unaryResponse(
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	if err := s.validateRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	page, err := s.pageIndex(request.GetRequest().GetNextPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response, err := s.response(page, true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return response, nil
}

func (s *reverseHistoryPageSource) allowNextStreamPage() {
	if !s.gateStream {
		return
	}
	s.streamPermits <- struct{}{}
}

func (s *reverseHistoryPageSource) waitForStream(t testing.TB) error {
	t.Helper()
	if !s.gateStream {
		return nil
	}
	select {
	case err := <-s.streamDone:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for the History stream to finish")
	}
}

func (s *reverseHistoryPageSource) streamResponse(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
	send func(*historyservice.GetWorkflowExecutionHistoryReverseResponse) error,
) (retErr error) {
	if s.gateStream {
		defer func() {
			s.streamDone <- retErr
		}()
	}
	if err := s.validateRequest(request); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if len(request.GetRequest().GetNextPageToken()) != 0 {
		return status.Error(codes.InvalidArgument, "a stream must start without a continuation token")
	}
	for page := 0; page < s.pageCount(); page++ {
		if page > 0 && s.gateStream {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.streamPermits:
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := s.response(page, false)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := send(response); err != nil {
			return err
		}
	}
	return nil
}

type reverseHistoryPageServer struct {
	historyservice.UnimplementedHistoryServiceServer
	source *reverseHistoryPageSource
}

func (s *reverseHistoryPageServer) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.source.unaryResponse(request)
}

func reverseHistoryInternalStreamHandler(srv any, stream grpc.ServerStream) error {
	request := new(historyservice.GetWorkflowExecutionHistoryReverseRequest)
	if err := stream.RecvMsg(request); err != nil {
		return err
	}
	pageServer, ok := srv.(*reverseHistoryPageServer)
	if !ok {
		return status.Error(codes.Internal, "unexpected reverse-history page server")
	}
	return pageServer.source.streamResponse(stream.Context(), request, func(response *historyservice.GetWorkflowExecutionHistoryReverseResponse) error {
		return stream.SendMsg(response)
	})
}

type reverseHistoryPublicStreamServer struct {
	grpc.ServerStream
}

func (s *reverseHistoryPublicStreamServer) Send(response *workflowservice.GetWorkflowExecutionHistoryReverseResponse) error {
	return s.SendMsg(response)
}

func reverseHistoryPublicStreamHandler(srv any, stream grpc.ServerStream) (retErr error) {
	request := new(workflowservice.GetWorkflowExecutionHistoryReverseRequest)
	if err := stream.RecvMsg(request); err != nil {
		return err
	}

	method := reflect.ValueOf(srv).MethodByName(reverseHistoryStreamMethod)
	if !method.IsValid() {
		return status.Error(codes.Unimplemented, "reverse-history streaming RPC is not implemented")
	}
	methodType := method.Type()
	if methodType.NumIn() != 2 || methodType.NumOut() != 1 {
		return status.Error(codes.Internal, "reverse-history streaming RPC has an unexpected signature")
	}
	streamServer := &reverseHistoryPublicStreamServer{ServerStream: stream}
	requestValue := reflect.ValueOf(request)
	streamValue := reflect.ValueOf(streamServer)
	if !requestValue.Type().AssignableTo(methodType.In(0)) || !streamValue.Type().AssignableTo(methodType.In(1)) {
		return status.Error(codes.Internal, "reverse-history streaming RPC uses an incompatible generated signature")
	}
	if !methodType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return status.Error(codes.Internal, "reverse-history streaming RPC does not return an error")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			retErr = status.Errorf(codes.Internal, "reverse-history streaming RPC panicked: %v", recovered)
		}
	}()
	results := method.Call([]reflect.Value{requestValue, streamValue})
	if !results[0].IsNil() {
		return results[0].Interface().(error)
	}
	return nil
}

func reverseHistoryServiceDescriptor() grpc.ServiceDesc {
	descriptor := historyservice.HistoryService_ServiceDesc
	descriptor.Methods = append([]grpc.MethodDesc(nil), descriptor.Methods...)
	descriptor.Streams = append([]grpc.StreamDesc(nil), descriptor.Streams...)
	for i := range descriptor.Streams {
		if descriptor.Streams[i].StreamName == reverseHistoryStreamMethod {
			descriptor.Streams[i].Handler = reverseHistoryInternalStreamHandler
			descriptor.Streams[i].ServerStreams = true
			descriptor.Streams[i].ClientStreams = false
			return descriptor
		}
	}
	descriptor.Streams = append(descriptor.Streams, grpc.StreamDesc{
		StreamName:    reverseHistoryStreamMethod,
		Handler:       reverseHistoryInternalStreamHandler,
		ServerStreams: true,
	})
	return descriptor
}

func reverseHistoryWorkflowServiceDescriptor() grpc.ServiceDesc {
	descriptor := workflowservice.WorkflowService_ServiceDesc
	descriptor.Methods = append([]grpc.MethodDesc(nil), descriptor.Methods...)
	descriptor.Streams = append([]grpc.StreamDesc(nil), descriptor.Streams...)
	for i := range descriptor.Streams {
		if descriptor.Streams[i].StreamName == reverseHistoryStreamMethod {
			descriptor.Streams[i].Handler = reverseHistoryPublicStreamHandler
			descriptor.Streams[i].ServerStreams = true
			descriptor.Streams[i].ClientStreams = false
			return descriptor
		}
	}
	descriptor.Streams = append(descriptor.Streams, grpc.StreamDesc{
		StreamName:    reverseHistoryStreamMethod,
		Handler:       reverseHistoryPublicStreamHandler,
		ServerStreams: true,
	})
	return descriptor
}

func reverseHistoryServerOptions(measurements *reverseHistoryMeasurements, kind string) []grpc.ServerOption {
	if measurements == nil {
		return nil
	}
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(func(
			ctx context.Context,
			request any,
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			if kind == "public" {
				measurements.publicRPCCalls.Add(1)
			} else {
				measurements.historyRPCCalls.Add(1)
			}
			var response any
			err := measurements.recordSpan(kind, func() error {
				var handlerErr error
				response, handlerErr = handler(ctx, request)
				return handlerErr
			})
			return response, err
		}),
		grpc.StreamInterceptor(func(
			srv any,
			stream grpc.ServerStream,
			info *grpc.StreamServerInfo,
			handler grpc.StreamHandler,
		) error {
			if kind == "public" {
				measurements.publicRPCCalls.Add(1)
			} else {
				measurements.historyRPCCalls.Add(1)
			}
			return measurements.recordSpan(kind, func() error {
				return handler(srv, stream)
			})
		}),
	}
}

type reverseHistoryTransportFixture struct {
	mode         reverseHistoryTransportMode
	pageCount    int
	source       *reverseHistoryPageSource
	client       workflowservice.WorkflowServiceClient
	publicConn   *grpc.ClientConn
	measurements *reverseHistoryMeasurements
}

func newReverseHistoryTransportFixture(
	tb testing.TB,
	pageCount int,
	instrument bool,
) *reverseHistoryTransportFixture {
	tb.Helper()
	var measurements *reverseHistoryMeasurements
	if instrument {
		measurements = &reverseHistoryMeasurements{}
	}
	source := newReverseHistoryPageSource(pageCount, reverseHistoryBenchmarkPageSize, measurements, instrument)

	historyListener := bufconn.Listen(8 * 1024 * 1024)
	historyServer := grpc.NewServer(reverseHistoryServerOptions(measurements, "history")...)
	historyDescriptor := reverseHistoryServiceDescriptor()
	historyServer.RegisterService(&historyDescriptor, &reverseHistoryPageServer{source: source})
	go func() {
		_ = historyServer.Serve(historyListener)
	}()

	historyConn := reverseHistoryDial(tb, historyListener)
	workflowHandler := &WorkflowHandler{
		config:            NewConfig(dynamicconfig.NewNoopCollection(), 1),
		logger:            log.NewNoopLogger(),
		throttledLogger:   log.NewNoopLogger(),
		namespaceRegistry: reverseHistoryStaticNamespaceRegistry{id: reverseHistoryBenchmarkNamespaceID},
		historyClient:     historyservice.NewHistoryServiceClient(historyConn),
	}

	publicListener := bufconn.Listen(8 * 1024 * 1024)
	publicServer := grpc.NewServer(reverseHistoryServerOptions(measurements, "public")...)
	publicDescriptor := reverseHistoryWorkflowServiceDescriptor()
	publicServer.RegisterService(&publicDescriptor, workflowHandler)
	go func() {
		_ = publicServer.Serve(publicListener)
	}()
	publicConn := reverseHistoryDial(tb, publicListener)

	mode := reverseHistoryUnaryMode
	if reverseHistoryStreamMethodAvailable(workflowHandler) {
		mode = reverseHistoryStreamMode
	}
	fixture := &reverseHistoryTransportFixture{
		mode:         mode,
		pageCount:    pageCount,
		source:       source,
		client:       workflowservice.NewWorkflowServiceClient(publicConn),
		publicConn:   publicConn,
		measurements: measurements,
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

func reverseHistoryDial(tb testing.TB, listener *bufconn.Listener) *grpc.ClientConn {
	tb.Helper()
	connection, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		tb.Fatalf("dial bufconn: %v", err)
	}
	return connection
}

func reverseHistoryStreamMethodAvailable(handler *WorkflowHandler) bool {
	method := reflect.ValueOf(handler).MethodByName(reverseHistoryStreamMethod)
	if !method.IsValid() {
		return false
	}
	methodType := method.Type()
	return methodType.NumIn() == 2 && methodType.NumOut() == 1 &&
		methodType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem())
}

type reverseHistoryPageCallback func(page int, response *workflowservice.GetWorkflowExecutionHistoryReverseResponse) bool

func (f *reverseHistoryTransportFixture) scan(
	ctx context.Context,
	callback reverseHistoryPageCallback,
) ([]*historypb.HistoryEvent, error) {
	if f.mode == reverseHistoryStreamMode {
		return f.scanStream(ctx, callback)
	}
	return f.scanUnary(ctx, callback)
}

func (f *reverseHistoryTransportFixture) scanUnary(
	ctx context.Context,
	callback reverseHistoryPageCallback,
) ([]*historypb.HistoryEvent, error) {
	request := &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace: reverseHistoryBenchmarkNamespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: reverseHistoryBenchmarkWorkflow,
		},
		// The frontend must enforce the production 256-event maximum. Supplying
		// 1000 mirrors the in-tree batcher caller and makes the cap observable.
		MaximumPageSize: 1000,
	}
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
		if callback != nil && !callback(page, response) {
			return events, nil
		}
		if len(response.GetNextPageToken()) == 0 {
			return events, nil
		}
		request.NextPageToken = response.GetNextPageToken()
	}
}

func (f *reverseHistoryTransportFixture) scanStream(
	ctx context.Context,
	callback reverseHistoryPageCallback,
) ([]*historypb.HistoryEvent, error) {
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := f.publicConn.NewStream(
		scanCtx,
		&grpc.StreamDesc{StreamName: reverseHistoryStreamMethod, ServerStreams: true},
		"/"+workflowservice.WorkflowService_ServiceDesc.ServiceName+"/"+reverseHistoryStreamMethod,
	)
	if err != nil {
		return nil, err
	}
	request := &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace: reverseHistoryBenchmarkNamespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: reverseHistoryBenchmarkWorkflow,
		},
		MaximumPageSize: 1000,
	}
	if err := stream.SendMsg(request); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}

	var events []*historypb.HistoryEvent
	for page := 0; ; page++ {
		response := new(workflowservice.GetWorkflowExecutionHistoryReverseResponse)
		err := stream.RecvMsg(response)
		if errors.Is(err, io.EOF) {
			return events, nil
		}
		if err != nil {
			return events, err
		}
		events = append(events, response.GetHistory().GetEvents()...)
		if callback != nil && !callback(page, response) {
			cancel()
			return events, nil
		}
		if err := scanCtx.Err(); err != nil {
			return events, err
		}
		if page+1 < f.pageCount {
			f.source.allowNextStreamPage()
		}
	}
}

func reverseHistoryAssertEvents(t testing.TB, events []*historypb.HistoryEvent, expected int) {
	t.Helper()
	reverseHistoryAssertReversePrefix(t, events, expected, int64(expected))
}

func reverseHistoryAssertReversePrefix(
	t testing.TB,
	events []*historypb.HistoryEvent,
	expectedCount int,
	firstEventID int64,
) {
	t.Helper()
	if len(events) != expectedCount {
		t.Fatalf("event count: got %d want %d", len(events), expectedCount)
	}
	for index, event := range events {
		wantID := firstEventID - int64(index)
		if event.GetEventId() != wantID {
			t.Fatalf("event %d ID: got %d want %d", index, event.GetEventId(), wantID)
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			t.Fatalf("event %d type: got %s", index, event.GetEventType())
		}
	}
}

func reverseHistoryExpectedRPCCalls(mode reverseHistoryTransportMode, pages int) int64 {
	if mode == reverseHistoryStreamMode {
		return 1
	}
	return int64(pages)
}

func reverseHistoryAssertWaterfall(
	t testing.TB,
	snapshot reverseHistoryMeasurementSnapshot,
	mode reverseHistoryTransportMode,
	pages int,
) string {
	t.Helper()
	wantCalls := int(reverseHistoryExpectedRPCCalls(mode, pages))
	if len(snapshot.spans) != wantCalls*2 {
		t.Fatalf("trace spans: got %d want %d", len(snapshot.spans), wantCalls*2)
	}
	parts := make([]string, 0, len(snapshot.spans))
	for call := 0; call < wantCalls; call++ {
		publicSpan := snapshot.spans[call*2]
		historySpan := snapshot.spans[call*2+1]
		if publicSpan.kind != "public" || historySpan.kind != "history" {
			t.Fatalf("trace call %d order: got %s>%s want public>history", call, publicSpan.kind, historySpan.kind)
		}
		if historySpan.start.Before(publicSpan.start) || historySpan.end.After(publicSpan.end) {
			t.Fatalf("trace call %d is not a nested public-to-History hop", call)
		}
		if call > 0 && publicSpan.start.Before(snapshot.spans[(call-1)*2].end) {
			t.Fatalf("trace public call %d overlapped the preceding page", call)
		}
		parts = append(parts, publicSpan.kind, historySpan.kind)
	}
	return strings.Join(parts, ">")
}

func reverseHistoryAssertFullScan(
	t testing.TB,
	fixture *reverseHistoryTransportFixture,
	events []*historypb.HistoryEvent,
) reverseHistoryMeasurementSnapshot {
	t.Helper()
	reverseHistoryAssertEvents(t, events, fixture.pageCount*reverseHistoryBenchmarkPageSize)
	snapshot := fixture.measurements.snapshot()
	wantCalls := reverseHistoryExpectedRPCCalls(fixture.mode, fixture.pageCount)
	if snapshot.publicRPCCalls != wantCalls {
		t.Fatalf("public RPC count: got %d want %d", snapshot.publicRPCCalls, wantCalls)
	}
	if snapshot.historyRPCCalls != wantCalls {
		t.Fatalf("frontend-to-History RPC count: got %d want %d", snapshot.historyRPCCalls, wantCalls)
	}
	if snapshot.sourcePageReads != int64(fixture.pageCount) {
		t.Fatalf("fixture page-source reads: got %d want %d", snapshot.sourcePageReads, fixture.pageCount)
	}
	if fixture.mode == reverseHistoryUnaryMode {
		wantContinuations := int64(fixture.pageCount - 1)
		if snapshot.continuationSerializations != wantContinuations || snapshot.continuationDeserializations != wantContinuations {
			t.Fatalf(
				"continuation transitions: serializations=%d deserializations=%d want=%d",
				snapshot.continuationSerializations,
				snapshot.continuationDeserializations,
				wantContinuations,
			)
		}
		if snapshot.continuationWireBytes <= 0 {
			t.Fatal("unary full scan did not transfer continuation bytes")
		}
	} else if snapshot.continuationSerializations != 0 || snapshot.continuationDeserializations != 0 || snapshot.continuationWireBytes != 0 {
		t.Fatalf(
			"streamed full scan transferred continuation state: serializations=%d deserializations=%d bytes=%d",
			snapshot.continuationSerializations,
			snapshot.continuationDeserializations,
			snapshot.continuationWireBytes,
		)
	}
	return snapshot
}

func reverseHistoryAssertSinglePageStop(
	t testing.TB,
	fixture *reverseHistoryTransportFixture,
	events []*historypb.HistoryEvent,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("single-page stop: %v", err)
	}
	reverseHistoryAssertReversePrefix(t, events, reverseHistoryBenchmarkPageSize, int64(fixture.pageCount*reverseHistoryBenchmarkPageSize))
	snapshot := fixture.measurements.snapshot()
	if snapshot.publicRPCCalls != 1 || snapshot.historyRPCCalls != 1 || snapshot.sourcePageReads != 1 {
		t.Fatalf(
			"single-page stop issued extra work: public=%d history=%d reads=%d",
			snapshot.publicRPCCalls,
			snapshot.historyRPCCalls,
			snapshot.sourcePageReads,
		)
	}
	if fixture.mode == reverseHistoryStreamMode {
		streamErr := fixture.source.waitForStream(t)
		if streamErr != nil && !reverseHistoryIsCancellation(streamErr) {
			t.Fatalf("single-page stream completion: %v", streamErr)
		}
	}
}

func reverseHistoryIsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

func TestReverseHistoryTransportContract(t *testing.T) {
	fixture := newReverseHistoryTransportFixture(t, reverseHistoryBenchmarkPageCount, true)

	events, err := fixture.scan(context.Background(), nil)
	if err != nil {
		t.Fatalf("full scan: %v", err)
	}
	fullSnapshot := reverseHistoryAssertFullScan(t, fixture, events)
	waterfall := reverseHistoryAssertWaterfall(t, fullSnapshot, fixture.mode, fixture.pageCount)
	if fixture.mode == reverseHistoryStreamMode {
		if streamErr := fixture.source.waitForStream(t); streamErr != nil {
			t.Fatalf("full stream completion: %v", streamErr)
		}
	}

	fixture.measurements.reset()
	events, err = fixture.scan(context.Background(), func(int, *workflowservice.GetWorkflowExecutionHistoryReverseResponse) bool {
		return false
	})
	reverseHistoryAssertSinglePageStop(t, fixture, events, err)

	fixture.measurements.reset()
	ctx, cancel := context.WithCancel(context.Background())
	events, err = fixture.scan(ctx, func(page int, _ *workflowservice.GetWorkflowExecutionHistoryReverseResponse) bool {
		if page == 0 {
			cancel()
		}
		return true
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: got %v want context.Canceled", err)
	}
	reverseHistoryAssertReversePrefix(t, events, reverseHistoryBenchmarkPageSize, int64(fixture.pageCount*reverseHistoryBenchmarkPageSize))
	canceledSnapshot := fixture.measurements.snapshot()
	if canceledSnapshot.publicRPCCalls != 1 || canceledSnapshot.historyRPCCalls != 1 || canceledSnapshot.sourcePageReads != 1 {
		t.Fatalf(
			"cancellation issued extra work: public=%d history=%d reads=%d",
			canceledSnapshot.publicRPCCalls,
			canceledSnapshot.historyRPCCalls,
			canceledSnapshot.sourcePageReads,
		)
	}
	if fixture.mode == reverseHistoryStreamMode {
		streamErr := fixture.source.waitForStream(t)
		if streamErr != nil && !reverseHistoryIsCancellation(streamErr) {
			t.Fatalf("canceled stream completion: %v", streamErr)
		}
	}

	t.Logf(
		"reverse_history_transport_contract passed mode=%s pages=%d events=%d public_rpc_count=%d frontend_history_rpc_count=%d continuation_serializations=%d continuation_deserializations=%d continuation_wire_bytes=%d fixture_page_source_reads=%d waterfall=%s",
		fixture.mode,
		fixture.pageCount,
		reverseHistoryBenchmarkPageCount*reverseHistoryBenchmarkPageSize,
		fullSnapshot.publicRPCCalls,
		fullSnapshot.historyRPCCalls,
		fullSnapshot.continuationSerializations,
		fullSnapshot.continuationDeserializations,
		fullSnapshot.continuationWireBytes,
		fullSnapshot.sourcePageReads,
		waterfall,
	)
}

func BenchmarkReverseHistoryFullScanManyPages(b *testing.B) {
	fixture := newReverseHistoryTransportFixture(b, reverseHistoryBenchmarkPageCount, false)
	if events, err := fixture.scan(context.Background(), nil); err != nil {
		b.Fatalf("warmup full scan: %v", err)
	} else {
		reverseHistoryAssertEvents(b, events, reverseHistoryBenchmarkPageCount*reverseHistoryBenchmarkPageSize)
	}

	b.ResetTimer()
	for b.Loop() {
		events, err := fixture.scan(context.Background(), nil)
		if err != nil {
			b.Fatalf("full scan: %v", err)
		}
		if len(events) != reverseHistoryBenchmarkPageCount*reverseHistoryBenchmarkPageSize {
			b.Fatalf("full scan event count: got %d", len(events))
		}
	}
}

func BenchmarkReverseHistoryFullScanOnePage(b *testing.B) {
	fixture := newReverseHistoryTransportFixture(b, 1, false)
	if events, err := fixture.scan(context.Background(), nil); err != nil {
		b.Fatalf("warmup one-page scan: %v", err)
	} else {
		reverseHistoryAssertEvents(b, events, reverseHistoryBenchmarkPageSize)
	}

	b.ResetTimer()
	for b.Loop() {
		events, err := fixture.scan(context.Background(), nil)
		if err != nil {
			b.Fatalf("one-page scan: %v", err)
		}
		if len(events) != reverseHistoryBenchmarkPageSize {
			b.Fatalf("one-page scan event count: got %d", len(events))
		}
	}
}
