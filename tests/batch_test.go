package tests

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// batch renders one POST /spans/batch body from raw span documents.
func batch(spans ...string) string {
	return `{"spans":[` + strings.Join(spans, ",") + `]}`
}

func (c *client) postBatch(body string) (int, string, map[string]any) {
	c.t.Helper()
	return c.do(request{method: http.MethodPost, path: "/spans/batch", key: c.key(), body: body})
}

// postBatchOK posts a batch that must succeed and returns the decoded body.
func (c *client) postBatchOK(body string) map[string]any {
	c.t.Helper()
	status, raw, decoded := c.postBatch(body)
	if status != http.StatusCreated {
		c.t.Fatalf("POST /spans/batch: status %d body %s", status, raw)
	}
	return decoded
}

// rejectBatch posts a batch that must be refused and returns the error code.
func (c *client) rejectBatch(body string, wanted int) string {
	c.t.Helper()
	status, raw, decoded := c.postBatch(body)
	if status != wanted {
		c.t.Fatalf("POST /spans/batch: wanted status %d, got %d body %s", wanted, status, raw)
	}
	cause, _ := decoded["error"].(map[string]any)
	code, _ := cause["code"].(string)
	return code
}

func acceptedItems(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	items := []map[string]any{}
	for _, item := range listOf(t, body["accepted"]) {
		items = append(items, objectOf(t, item))
	}
	return items
}

func TestBatchIngestsOutOfOrderAcrossTraces(t *testing.T) {
	c := newClient(t)
	// Two traces in one batch; t-b1 arrives child-first, t-b2 is a lone root.
	child1 := span("t-b1", "s-c1", "s-root", "db", "query", "client", "2024-06-01T00:00:00.000000100Z", 200, "ok", nil)
	child2 := span("t-b1", "s-c2", "s-root", "db", "query", "client", "2024-06-01T00:00:00.000000400Z", 200, "error", nil)
	root1 := span("t-b1", "s-root", "", "gateway", "GET /x", "server", traceFixtureTime, 1000, "ok", count(3))
	root2 := span("t-b2", "s-lone", "", "worker", "run", "internal", traceFixtureTime, 50, "ok", count(1))
	body := batch(child1, root2, child2, root1)
	response := c.postBatchOK(body)

	if count := numberOf(t, response["count"]); count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}
	items := acceptedItems(t, response)
	if len(items) != 4 {
		t.Fatalf("accepted has %d items, want 4", len(items))
	}
	wantIDs := [][2]string{
		{"t-b1", "s-c1"}, {"t-b2", "s-lone"}, {"t-b1", "s-c2"}, {"t-b1", "s-root"},
	}
	for index, want := range wantIDs {
		item := items[index]
		if got, _ := item["trace_id"].(string); got != want[0] {
			t.Errorf("item %d trace_id = %q, want %q", index, got, want[0])
		}
		if got, _ := item["span_id"].(string); got != want[1] {
			t.Errorf("item %d span_id = %q, want %q", index, got, want[1])
		}
		if accepted, _ := item["accepted"].(bool); !accepted {
			t.Errorf("item %d accepted = %v", index, item["accepted"])
		}
	}
	// The first span of t-b1 is an orphan until its root lands, so the trace is
	// neither complete nor valid at that point; the root completes it.
	if complete, _ := items[0]["complete"].(bool); complete {
		t.Errorf("item 0 complete = true, want false while the root is missing")
	}
	if valid, _ := items[0]["valid"].(bool); valid {
		t.Errorf("item 0 valid = true, want false while the root is missing")
	}
	if violations := listOf(t, items[0]["violations"]); len(violations) == 0 {
		t.Errorf("item 0 violations should explain the missing root")
	}
	if complete, _ := items[1]["complete"].(bool); !complete {
		t.Errorf("item 1 (lone root) complete = false, want true")
	}
	if valid, _ := items[1]["valid"].(bool); !valid {
		t.Errorf("item 1 (lone root) valid = false, want true")
	}
	if complete, _ := items[3]["complete"].(bool); !complete {
		t.Errorf("item 3 complete = false, want true once the root landed")
	}
	if valid, _ := items[3]["valid"].(bool); !valid {
		t.Errorf("item 3 valid = false, want true: %v", items[3]["violations"])
	}
	if violations := listOf(t, items[3]["violations"]); len(violations) != 0 {
		t.Errorf("item 3 violations = %v, want none", violations)
	}

	// The batch spans take part in the existing reads like any other span.
	trace := c.get("/traces/t-b1")
	if count := numberOf(t, trace["span_count"]); count != 3 {
		t.Fatalf("t-b1 span_count = %d, want 3", count)
	}
	if errors := numberOf(t, trace["error_spans"]); errors != 1 {
		t.Fatalf("t-b1 error_spans = %d, want 1", errors)
	}
	path := c.get("/traces/t-b1/critical-path")
	if length := numberOf(t, path["length_ns"]); length != 1000 {
		t.Fatalf("critical path length = %d, want the root duration 1000", length)
	}
	graph := c.get("/services/graph?trace_id=t-b1")
	if services := listOf(t, graph["services"]); len(services) != 2 {
		t.Fatalf("graph services = %d, want 2 (gateway, db)", len(services))
	}
}

func TestBatchRejectsEnvelopeErrors(t *testing.T) {
	c := newClient(t)
	good := span("t-e", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1))
	cases := []struct {
		name string
		body string
	}{
		{"extra field", `{"spans":[], "other": 1}`},
		{"spans missing", `{}`},
		{"spans null", `{"spans":null}`},
		{"spans empty", `{"spans":[]}`},
		{"spans not an array", `{"spans":{}}`},
		{"body not an object", `[1,2]`},
		{"element not an object", `{"spans":[42]}`},
		{"element is an array", `{"spans":[[1]]}`},
		{"element is null", `{"spans":[null]}`},
		{"trailing content", `{"spans":[]} {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := c.rejectBatch(tc.body, http.StatusBadRequest); code != "validation_error" {
				t.Fatalf("code = %q, want validation_error", code)
			}
		})
	}
	// 1001 spans exceed the batch limit; 1000 are accepted.
	over := make([]string, 0, 1001)
	for index := 0; index < 1001; index++ {
		over = append(over, good)
	}
	if code := c.rejectBatch(batch(over...), http.StatusBadRequest); code != "validation_error" {
		t.Fatalf("1001 spans: code = %q, want validation_error", code)
	}
	many := make([]string, 0, 1000)
	for index := 0; index < 1000; index++ {
		many = append(many, span("t-big", fmt.Sprintf("s-%04d", index), "", "svc", "op", "internal",
			traceFixtureTime, 10, "ok", count(1)))
	}
	// Every span is a root of the same trace, which is invalid at assembly but
	// accepted at ingestion; the point is that 1000 spans go through.
	response := c.postBatchOK(batch(many...))
	if count := numberOf(t, response["count"]); count != 1000 {
		t.Fatalf("count = %d, want 1000", count)
	}
}

func TestBatchRequiresIdempotencyKey(t *testing.T) {
	c := newClient(t)
	good := batch(span("t-k", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1)))
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", body: good})
	if status != http.StatusBadRequest {
		t.Fatalf("missing key: status %d body %s", status, raw)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "validation_error" {
		t.Fatalf("missing key: code %q", code)
	}
	longKey := strings.Repeat("k", 201)
	status, _, decoded = c.do(request{method: http.MethodPost, path: "/spans/batch", key: longKey, body: good})
	if status != http.StatusBadRequest {
		t.Fatalf("long key: status %d", status)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "validation_error" {
		t.Fatalf("long key: code %q", code)
	}
	// A failed request must not consume the key: the same long key check above
	// never ran, and a proper request with a fresh key works.
	c.postBatchOK(good)
}

func TestBatchFieldErrorIsAtomicAndKeepsTheKey(t *testing.T) {
	c := newClient(t)
	key := c.key()
	good := span("t-a", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1))
	bad := `{"trace_id":"t-a","span_id":"s-2","parent_id":null,"service":"svc","operation":"op",` +
		`"kind":"bogus","start_time":"2024-06-01T00:00:00Z","duration_ns":5,"status":"ok","span_count":1}`
	body := batch(good, bad)
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
	if status != http.StatusBadRequest {
		t.Fatalf("status %d body %s", status, raw)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "validation_error" {
		t.Fatalf("code %q, want validation_error", code)
	}
	// Nothing of the batch is readable.
	if status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-a"}); status != http.StatusNotFound {
		t.Fatalf("trace of a failed batch: status %d, want 404", status)
	}
	// The key was not consumed: a fixed batch with the same key succeeds.
	fixed := batch(good, span("t-a", "s-2", "", "svc", "op", "internal", traceFixtureTime, 5, "ok", count(1)))
	status, raw, _ = c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: fixed})
	if status != http.StatusCreated {
		t.Fatalf("retry with the same key: status %d body %s", status, raw)
	}
}

func TestBatchConflictIsAtomicAndReportsFirstProblem(t *testing.T) {
	c := newClient(t)
	// Pre-existing span the batch will collide with.
	c.post(span("t-x", "s-root", "", "gateway", "GET /x", "server", traceFixtureTime, 1000, "ok", count(2)))

	// The second span duplicates a stored span id: a conflict. The first span
	// is legal but must not survive the aborted batch.
	key := c.key()
	first := span("t-y", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1))
	duplicate := span("t-x", "s-root", "", "gateway", "GET /x", "server", traceFixtureTime, 1000, "ok", count(2))
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: batch(first, duplicate)})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "conflict" {
		t.Fatalf("code %q, want conflict", code)
	}
	if status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-y"}); status != http.StatusNotFound {
		t.Fatalf("earlier span of a failed batch is readable: status %d", status)
	}
	list := c.get("/traces")
	if traces := listOf(t, list["traces"]); len(traces) != 1 {
		t.Fatalf("stored traces = %d, want only the pre-existing one", len(traces))
	}

	// A time conflict inside the batch (child not contained in its parent) is
	// also a 409 and also atomic.
	parent := span("t-z", "s-p", "", "svc", "op", "internal", traceFixtureTime, 100, "ok", count(2))
	outside := span("t-z", "s-c", "s-p", "svc", "op", "internal", "2024-06-01T00:00:00.000000050Z", 200, "ok", nil)
	if code := c.rejectBatch(batch(parent, outside), http.StatusConflict); code != "conflict" {
		t.Fatalf("time conflict: code %q, want conflict", code)
	}
	if status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-z"}); status != http.StatusNotFound {
		t.Fatalf("parent of a failed batch is readable: status %d", status)
	}

	// The first problem in request order wins: a conflict before a field error
	// reports the conflict, a field error before a conflict reports 400.
	clash := span("t-x", "s-root", "", "gateway", "GET /x", "server", traceFixtureTime, 1000, "ok", count(2))
	badField := `{"trace_id":"t-w","span_id":"s-1","parent_id":null,"service":"svc","operation":"op",` +
		`"kind":"bogus","start_time":"2024-06-01T00:00:00Z","duration_ns":5,"status":"ok","span_count":1}`
	if code := c.rejectBatch(batch(clash, badField), http.StatusConflict); code != "conflict" {
		t.Fatalf("conflict first: code %q, want conflict", code)
	}
	if code := c.rejectBatch(batch(badField, clash), http.StatusBadRequest); code != "validation_error" {
		t.Fatalf("field error first: code %q, want validation_error", code)
	}
}

func TestBatchRejectsCrossTraceAndDuplicateWithinBatch(t *testing.T) {
	c := newClient(t)
	// A span whose parent lives in another trace of the same batch conflicts.
	rootA := span("t-c1", "s-root", "", "svc", "op", "internal", traceFixtureTime, 100, "ok", count(1))
	strayB := span("t-c2", "s-1", "s-root", "svc", "op", "internal", traceFixtureTime, 10, "ok", nil)
	if code := c.rejectBatch(batch(rootA, strayB), http.StatusConflict); code != "conflict" {
		t.Fatalf("cross-trace parent: code %q, want conflict", code)
	}
	// A duplicated span id inside the batch conflicts.
	one := span("t-c3", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1))
	two := span("t-c3", "s-1", "", "svc", "op", "internal", traceFixtureTime, 20, "ok", count(1))
	if code := c.rejectBatch(batch(one, two), http.StatusConflict); code != "conflict" {
		t.Fatalf("duplicate in batch: code %q, want conflict", code)
	}
	if status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-c3"}); status != http.StatusNotFound {
		t.Fatalf("duplicate batch left state behind: status %d", status)
	}
}

func TestBatchIdempotentReplay(t *testing.T) {
	c := newClient(t)
	key := c.key()
	body := batch(
		span("t-r", "s-c", "s-root", "db", "query", "client", "2024-06-01T00:00:00.000000100Z", 200, "ok", nil),
		span("t-r", "s-root", "", "gateway", "GET /r", "server", traceFixtureTime, 1000, "ok", count(2)),
	)
	status, first, _ := c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
	if status != http.StatusCreated {
		t.Fatalf("first: status %d body %s", status, first)
	}
	// An identical replay returns the first response byte for byte, including
	// the intermediate reassembly snapshot of the child-first first item.
	status, replay, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
	if status != http.StatusCreated {
		t.Fatalf("replay: status %d body %s", status, replay)
	}
	if replay != first {
		t.Fatalf("replay differs:\n%s\n%s", first, replay)
	}
	items := acceptedItems(t, decoded)
	if complete, _ := items[0]["complete"].(bool); complete {
		t.Fatalf("replayed item 0 lost its original incomplete snapshot")
	}
	// The replay did not write anything: the spans exist exactly once, so a
	// fresh key carrying one of them again is a duplicate conflict.
	c.reject(span("t-r", "s-root", "", "gateway", "GET /r", "server", traceFixtureTime, 1000, "ok", count(2)),
		http.StatusConflict)

	// The same key with a different body is a conflict, even a valid one.
	other := batch(span("t-r2", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1)))
	status, _, decoded = c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: other})
	if status != http.StatusConflict {
		t.Fatalf("different body: status %d", status)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "conflict" {
		t.Fatalf("different body: code %q, want conflict", code)
	}

	// A key is bound to its operation in both directions.
	singleKey := c.key()
	c.postWithKey(span("t-s", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1)), singleKey)
	status, _, _ = c.do(request{method: http.MethodPost, path: "/spans/batch", key: singleKey, body: other})
	if status != http.StatusConflict {
		t.Fatalf("single-span key reused for a batch: status %d", status)
	}
	batchKey := c.key()
	c.do(request{method: http.MethodPost, path: "/spans/batch", key: batchKey, body: other})
	c.rejectWithKey(span("t-s2", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1)),
		batchKey, http.StatusConflict)
}

// rejectWithKey posts one span with an explicit key that must be refused.
func (c *client) rejectWithKey(body string, key string, wanted int) {
	c.t.Helper()
	status, raw, _ := c.do(request{method: http.MethodPost, path: "/spans", key: key, body: body})
	if status != wanted {
		c.t.Fatalf("POST /spans: wanted status %d, got %d body %s", wanted, status, raw)
	}
}

func TestBatchSurvivesRestart(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "tracepath.db")
	first := newClientOn(t, database)
	key := "batch-restart"
	body := batch(
		span("t-re", "s-c", "s-root", "db", "query", "client", "2024-06-01T00:00:00.000000100Z", 200, "ok", nil),
		span("t-re", "s-root", "", "gateway", "GET /re", "server", traceFixtureTime, 1000, "ok", count(2)),
	)
	status, created, _ := first.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
	if status != http.StatusCreated {
		t.Fatalf("batch: status %d body %s", status, created)
	}
	// A failed batch leaves nothing behind, not even after a restart.
	failed := batch(
		span("t-gone", "s-1", "", "svc", "op", "internal", traceFixtureTime, 10, "ok", count(1)),
		span("t-re", "s-root", "", "gateway", "GET /re", "server", traceFixtureTime, 1000, "ok", count(2)),
	)
	if code := first.rejectBatch(failed, http.StatusConflict); code != "conflict" {
		t.Fatalf("failed batch: code %q", code)
	}
	_, before, _ := first.do(request{method: http.MethodGet, path: "/traces/t-re"})
	first.server.Close()

	second := newClientOn(t, database)
	_, after, _ := second.do(request{method: http.MethodGet, path: "/traces/t-re"})
	if before != after {
		t.Fatalf("reopened store returned a different trace:\n%s\n%s", before, after)
	}
	if status, _, _ := second.do(request{method: http.MethodGet, path: "/traces/t-gone"}); status != http.StatusNotFound {
		t.Fatalf("failed batch visible after restart: status %d", status)
	}
	// The idempotency record survived too: a replay still returns the first
	// response without writing again.
	status, replay, _ := second.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
	if status != http.StatusCreated {
		t.Fatalf("replay after restart: status %d body %s", status, replay)
	}
	if replay != created {
		t.Fatalf("replay after restart differs from the first response:\n%s\n%s", created, replay)
	}
}
