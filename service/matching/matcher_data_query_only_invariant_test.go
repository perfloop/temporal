package matching

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// assertPollerPQMutationBoundary keeps the queryOnlyCount shortcut coupled to
// pollerPQ's heap membership. The one intentionally corrupted state is covered
// by TestMatcherDataFindMatchQueryOnlyPollers; all other source construction
// and mutation paths must use pollerPQ's heap.Interface methods.
func assertPollerPQMutationBoundary(t *testing.T) {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate matcher package source")
	}

	dir := filepath.Dir(thisFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read matcher package source: %v", err)
	}

	fset := token.NewFileSet()
	var violations []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		violations = append(violations, pollerPQMutationViolations(fset, file, strings.HasSuffix(entry.Name(), "_test.go"))...)
	}

	if len(violations) > 0 {
		slices.Sort(violations)
		t.Fatalf("pollerPQ query-only-count invariant bypassed:\n%s", strings.Join(violations, "\n"))
	}
}

func assertPollerPQQueryOnlyCount(t *testing.T, pollers *pollerPQ) {
	t.Helper()

	want := 0
	for _, poller := range pollers.heap {
		if poller.queryOnly {
			want++
		}
	}
	if pollers.queryOnlyCount != want {
		t.Fatalf("queryOnlyCount = %d, want %d for heap %v", pollers.queryOnlyCount, want, pollers.heap)
	}
}

// assertPollerPQCounterDivergenceChangesMatchDecision documents the exact
// incorrect decision that the mutation boundary prevents: a normal poller
// paired with an all-query-only count would hide a normal task.
func assertPollerPQCounterDivergenceChangesMatchDecision(t *testing.T) {
	t.Helper()

	data := &matcherData{}
	task := &internalTask{}
	poller := &waitingPoller{}
	data.tasks.Add(task)
	data.pollers.Add(poller)
	assertPollerPQQueryOnlyCount(t, &data.pollers)
	if data.pollers.queryOnlyCount == len(data.pollers.heap) {
		t.Fatal("normal poller unexpectedly satisfies all-query-only shortcut")
	}

	// This call takes the established full heap scan because the normal poller
	// keeps queryOnlyCount below heap length.
	wantTask, wantPoller := data.findMatch(false)
	if wantTask != task || wantPoller != poller {
		t.Fatalf("full heap scan = (%v, %v), want (%v, %v)", wantTask, wantPoller, task, poller)
	}

	// Deliberately corrupt the derived count. This state is not a supported
	// pollerPQ construction; assertPollerPQMutationBoundary keeps it out of
	// production and ordinary test setup.
	data.pollers.queryOnlyCount = len(data.pollers.heap)
	gotTask, gotPoller := data.findMatch(false)
	if gotTask != nil || gotPoller != nil {
		t.Fatalf("corrupted all-query-only count = (%v, %v), want no match", gotTask, gotPoller)
	}
}

func pollerPQMutationViolations(fset *token.FileSet, file *ast.File, isTestFile bool) []string {
	var violations []string
	violation := func(node ast.Node, format string, args ...any) {
		position := fset.Position(node.Pos())
		violations = append(violations, position.String()+": "+fmt.Sprintf(format, args...))
	}

	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || !isPollerPQLiteral(literal) {
			return true
		}
		for _, element := range literal.Elts {
			keyValue, ok := element.(*ast.KeyValueExpr)
			if ok && isIdentifier(keyValue.Key, "heap") {
				violation(keyValue, "pollerPQ literals must not populate heap directly")
			}
		}
		return true
	})

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		receiver, isPollerPQMethod := pollerPQMethodReceiver(function)

		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.AssignStmt:
				for _, left := range node.Lhs {
					violations = append(violations, pollerPQFieldMutationViolations(fset, left, function.Name.Name, receiver, isPollerPQMethod, isTestFile)...)
				}
			case *ast.IncDecStmt:
				violations = append(violations, pollerPQFieldMutationViolations(fset, node.X, function.Name.Name, receiver, isPollerPQMethod, isTestFile)...)
			case *ast.CallExpr:
				if isHeapInit(node) && len(node.Args) == 1 && isPollerPQReference(node.Args[0], receiver, isPollerPQMethod) {
					violation(node, "heap.Init must not bypass pollerPQ count maintenance")
				}
			}
			return true
		})
	}

	return violations
}

func pollerPQFieldMutationViolations(
	fset *token.FileSet,
	expression ast.Expr,
	functionName string,
	receiver string,
	isPollerPQMethod bool,
	isTestFile bool,
) []string {
	field, owner := terminalSelector(expression)
	if field == "" {
		return nil
	}

	position := fset.Position(expression.Pos()).String()
	if field == "heap" {
		if isTestFile {
			if !selectorChainContains(owner, "tasks") {
				return []string{position + ": tests must not mutate a heap outside taskPQ setup"}
			}
			return nil
		}
		if isPollerPQReference(owner, receiver, isPollerPQMethod) && !isPollerPQHeapMethod(functionName) {
			return []string{position + ": pollerPQ heap mutation must be in Push, Pop, or Swap"}
		}
		if selectorChainContains(owner, "pollers") {
			return []string{position + ": pollerPQ heap mutation must use pollerPQ methods"}
		}
	}

	if isTestFile {
		if field == "queryOnlyCount" && functionName == "assertPollerPQCounterDivergenceChangesMatchDecision" {
			return nil
		}
		if field == "queryOnly" || field == "queryOnlyCount" {
			return []string{position + ": tests may not mutate query-only membership"}
		}
		return nil
	}
	if field != "queryOnly" && field != "queryOnlyCount" {
		return nil
	}
	if field == "queryOnlyCount" && isPollerPQReference(owner, receiver, isPollerPQMethod) && (functionName == "Push" || functionName == "Pop") {
		return nil
	}
	return []string{position + ": query-only membership may change only through pollerPQ Push or Pop"}
}

func isPollerPQLiteral(literal *ast.CompositeLit) bool {
	identifier, ok := literal.Type.(*ast.Ident)
	return ok && identifier.Name == "pollerPQ"
}

func pollerPQMethodReceiver(function *ast.FuncDecl) (string, bool) {
	if function.Recv == nil || len(function.Recv.List) != 1 || len(function.Recv.List[0].Names) != 1 {
		return "", false
	}

	receiverType := function.Recv.List[0].Type
	if pointer, ok := receiverType.(*ast.StarExpr); ok {
		receiverType = pointer.X
	}
	identifier, ok := receiverType.(*ast.Ident)
	if !ok || identifier.Name != "pollerPQ" {
		return "", false
	}
	return function.Recv.List[0].Names[0].Name, true
}

func terminalSelector(expression ast.Expr) (string, ast.Expr) {
	switch expression := expression.(type) {
	case *ast.SelectorExpr:
		return expression.Sel.Name, expression.X
	case *ast.IndexExpr:
		return terminalSelector(expression.X)
	case *ast.IndexListExpr:
		return terminalSelector(expression.X)
	case *ast.ParenExpr:
		return terminalSelector(expression.X)
	case *ast.StarExpr:
		return terminalSelector(expression.X)
	default:
		return "", nil
	}
}

func selectorChainContains(expression ast.Expr, name string) bool {
	switch expression := expression.(type) {
	case *ast.Ident:
		return expression.Name == name
	case *ast.SelectorExpr:
		return expression.Sel.Name == name || selectorChainContains(expression.X, name)
	case *ast.IndexExpr:
		return selectorChainContains(expression.X, name)
	case *ast.IndexListExpr:
		return selectorChainContains(expression.X, name)
	case *ast.ParenExpr:
		return selectorChainContains(expression.X, name)
	case *ast.StarExpr:
		return selectorChainContains(expression.X, name)
	default:
		return false
	}
}

func isPollerPQReference(expression ast.Expr, receiver string, isPollerPQMethod bool) bool {
	if isPollerPQMethod && isIdentifier(expression, receiver) {
		return true
	}
	return selectorChainContains(expression, "pollers")
}

func isPollerPQHeapMethod(functionName string) bool {
	return functionName == "Push" || functionName == "Pop" || functionName == "Swap"
}

func isHeapInit(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Init" && isIdentifier(selector.X, "heap")
}

func isIdentifier(expression ast.Expr, name string) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == name
}
