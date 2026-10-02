package tests

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// retain posts a retention request that must succeed and returns the decoded
// response body.
func (c *client) retain(body string) map[string]any {
	c.t.Helper()
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/traces/retention", body: body})
	if status != http.StatusOK {
		c.t.Fatalf("POST /traces/retention: status %d body %s", status, raw)
	}
	return decoded
}

// rejectRetention posts a retention request that must be refused with a
// validation_error and returns the error code.
func (c *client) rejectRetention(r request, wanted int) string {
	c.t.Helper()
	r.method = http.MethodPost
	r.path = "/traces/retention"
	status, raw, decoded := c.do(r)
	if status != wanted {
		c.t.Fatalf("POST /traces/retention: wanted status %d, got %d body %s", wanted, status, raw)
	}
	cause, _ := decoded["error"].(map[string]any)
	code, _ := cause["code"].(string)
	return code
}

// traceIDsOf returns the trace ids of a GET /traces response in order.
func traceIDsOf(t *testing.T, listed map[string]any) []string {
	t.Helper()
	ids := []string{}
	for _, item := range listOf(t, listed["traces"]) {
		id, _ := objectOf(t, item)["trace_id"].(string)
		ids = append(ids, id)
	}
	return ids
}

// loadSimpleTrace ingests a two-span trace (root plus one child) whose root
// starts at the given instant.
func (c *client) loadSimpleTrace(traceID string, rootID string, start string) {
	c.t.Helper()
	child := span(traceID, rootID+"-c", rootID, "db", "query", "client", start, 100, "ok", nil)
	root := span(traceID, rootID, "", "gateway", "GET /", "server", start, 500, "ok", count(2))
	c.post(child)
	c.post(root)
}

func TestRetentionDeletesOnlyOlderTraces(t *testing.T) {
	c := newClient(t)
	c.loadSimpleTrace("t-old", "r-old", "2024-06-01T00:00:00Z")
	c.loadSimpleTrace("t-edge", "r-edge", "2024-06-02T00:00:00Z")
	c.loadSimpleTrace("t-new", "r-new", "2024-06-03T00:00:00Z")

	result := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	if got := numberOf(t, result["deleted_traces"]); got != 1 {
		t.Fatalf("deleted_traces = %d, want 1", got)
	}
	if got := numberOf(t, result["deleted_spans"]); got != 2 {
		t.Fatalf("deleted_spans = %d, want 2", got)
	}
	if got := numberOf(t, result["retained_traces"]); got != 2 {
		t.Fatalf("retained_traces = %d, want 2", got)
	}
	if got := numberOf(t, result["retained_spans"]); got != 4 {
		t.Fatalf("retained_spans = %d, want 4", got)
	}
	if before, _ := result["before"].(string); before != "2024-06-02T00:00:00Z" {
		t.Fatalf("before = %q, want the cut-off echoed in UTC", before)
	}

	// The boundary trace starts exactly at before and survives.
	ids := traceIDsOf(t, c.get("/traces"))
	if strings.Join(ids, ",") != "t-new,t-edge" {
		t.Fatalf("remaining traces = %v, want [t-new t-edge]", ids)
	}
	status, _, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-old"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-old: status %d body %v, want 404", status, decoded)
	}
	// Critical path and the service graph only see the retained traces.
	path := c.get("/traces/t-edge/critical-path")
	if got := numberOf(t, path["trace_duration_ns"]); got != 500 {
		t.Fatalf("critical path of t-edge: trace_duration_ns = %d, want 500", got)
	}
	graph := c.get("/services/graph")
	if got := numberOf(t, objectOf(t, graph["stats"])["trace_count"]); got != 2 {
		t.Fatalf("graph trace_count = %d, want 2", got)
	}
	if got := numberOf(t, objectOf(t, graph["stats"])["span_count"]); got != 4 {
		t.Fatalf("graph span_count = %d, want 4", got)
	}
}

func TestRetentionComparesAtNanosecondPrecision(t *testing.T) {
	c := newClient(t)
	c.loadSimpleTrace("t-ns", "r-ns", "2024-06-01T00:00:00.000000001Z")

	// A cut-off one nanosecond earlier keeps the trace.
	result := c.retain(`{"before":"2024-06-01T00:00:00.000000001Z"}`)
	if got := numberOf(t, result["deleted_traces"]); got != 0 {
		t.Fatalf("equal cut-off deleted %d traces, want 0", got)
	}
	// One nanosecond later deletes it.
	result = c.retain(`{"before":"2024-06-01T00:00:00.000000002Z"}`)
	if got := numberOf(t, result["deleted_traces"]); got != 1 {
		t.Fatalf("deleted_traces = %d, want 1", got)
	}
	if got := numberOf(t, result["deleted_spans"]); got != 2 {
		t.Fatalf("deleted_spans = %d, want 2", got)
	}
}

func TestRetentionKeepsTracesWithoutRoot(t *testing.T) {
	c := newClient(t)
	// An incomplete trace whose root was never ingested: its start time is
	// unknowable, so retention must keep it regardless of the cut-off.
	orphan := span("t-rootless", "s-child", "s-missing", "db", "query", "client", "2020-01-01T00:00:00Z", 100, "ok", nil)
	c.post(orphan)
	c.loadSimpleTrace("t-dated", "r-dated", "2020-01-01T00:00:00Z")

	result := c.retain(`{"before":"2030-01-01T00:00:00Z"}`)
	if got := numberOf(t, result["deleted_traces"]); got != 1 {
		t.Fatalf("deleted_traces = %d, want 1", got)
	}
	if got := numberOf(t, result["retained_traces"]); got != 1 {
		t.Fatalf("retained_traces = %d, want 1", got)
	}
	if got := numberOf(t, result["retained_spans"]); got != 1 {
		t.Fatalf("retained_spans = %d, want 1", got)
	}
	ids := traceIDsOf(t, c.get("/traces"))
	if strings.Join(ids, ",") != "t-rootless" {
		t.Fatalf("remaining traces = %v, want [t-rootless]", ids)
	}
}

func TestRetentionEchoesBeforeInUTC(t *testing.T) {
	c := newClient(t)
	result := c.retain(`{"before":"2024-06-02T05:30:00.500000000+02:00"}`)
	if before, _ := result["before"].(string); before != "2024-06-02T03:30:00.5Z" {
		t.Fatalf("before = %q, want 2024-06-02T03:30:00.5Z", before)
	}
}

func TestRetentionIsDeterministicOnTheSameState(t *testing.T) {
	c := newClient(t)
	c.loadSimpleTrace("t-old", "r-old", "2024-06-01T00:00:00Z")
	c.loadSimpleTrace("t-new", "r-new", "2024-06-03T00:00:00Z")

	first := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	second := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	// The second request runs on the state the first one produced, so it
	// deletes nothing and retains everything that is left.
	if got := numberOf(t, second["deleted_traces"]); got != 0 {
		t.Fatalf("second run deleted_traces = %d, want 0", got)
	}
	if got := numberOf(t, second["deleted_spans"]); got != 0 {
		t.Fatalf("second run deleted_spans = %d, want 0", got)
	}
	if numberOf(t, first["retained_traces"]) != numberOf(t, second["retained_traces"]) ||
		numberOf(t, first["retained_spans"]) != numberOf(t, second["retained_spans"]) {
		t.Fatalf("retained counts changed between runs: %v then %v", first, second)
	}
	// Repeating a cut-off that no longer matches anything is a no-op.
	third := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	if got := numberOf(t, third["deleted_traces"]); got != 0 {
		t.Fatalf("third run deleted_traces = %d, want 0", got)
	}
}

func TestRetentionRejectsBadRequests(t *testing.T) {
	c := newClient(t)
	c.loadSimpleTrace("t-keep", "r-keep", "2024-06-01T00:00:00Z")

	cases := []struct {
		name    string
		request request
	}{
		{"content type", request{body: `{"before":"2024-06-02T00:00:00Z"}`, contentType: "text/plain"}},
		{"missing content type", request{body: `{"before":"2024-06-02T00:00:00Z"}`, omitType: true}},
		{"not an object", request{body: `["2024-06-02T00:00:00Z"]`}},
		{"not json", request{body: `before`}},
		{"empty body", request{body: ``}},
		{"missing before", request{body: `{}`}},
		{"null before", request{body: `{"before":null}`}},
		{"empty before", request{body: `{"before":""}`}},
		{"non-string before", request{body: `{"before":2024}`}},
		{"date only", request{body: `{"before":"2024-06-02"}`}},
		{"no offset", request{body: `{"before":"2024-06-02T00:00:00"}`}},
		{"garbage before", request{body: `{"before":"yesterday"}`}},
		{"extra field", request{body: `{"before":"2024-06-02T00:00:00Z","after":"2024-06-03T00:00:00Z"}`}},
		{"trailing content", request{body: `{"before":"2024-06-02T00:00:00Z"} {}`}},
	}
	for _, tc := range cases {
		if code := c.rejectRetention(tc.request, http.StatusBadRequest); code != "validation_error" {
			t.Fatalf("%s: code %q, want validation_error", tc.name, code)
		}
	}
	// Nothing was deleted by the rejected requests.
	if got := numberOf(t, c.retain(`{"before":"2024-06-02T00:00:00Z"}`)["deleted_traces"]); got != 1 {
		t.Fatalf("deleted_traces after rejections = %d, want 1 (the trace must have survived them)", got)
	}
}

func TestRetentionSurvivesRestart(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "tracepath.db")
	first := newClientOn(t, database)
	first.loadSimpleTrace("t-old", "r-old", "2024-06-01T00:00:00Z")
	first.loadSimpleTrace("t-new", "r-new", "2024-06-03T00:00:00Z")
	first.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	first.server.Close()

	second := newClientOn(t, database)
	ids := traceIDsOf(t, second.get("/traces"))
	if strings.Join(ids, ",") != "t-new" {
		t.Fatalf("traces after restart = %v, want [t-new]", ids)
	}
	status, _, _ := second.do(request{method: http.MethodGet, path: "/traces/t-old"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-old after restart: status %d, want 404", status)
	}
	// Ingestion still works on the retained database, and the deleted trace id
	// can be reused because its spans are gone. Fresh keys avoid the persisted
	// idempotency records of the first client.
	second.keys = 1000
	second.loadSimpleTrace("t-old", "r-old", "2024-06-05T00:00:00Z")
	graph := second.get("/services/graph")
	if got := numberOf(t, objectOf(t, graph["stats"])["trace_count"]); got != 2 {
		t.Fatalf("graph trace_count after restart = %d, want 2", got)
	}
}

func TestRetentionDoesNotTouchIdempotency(t *testing.T) {
	c := newClient(t)
	// key-1 and key-2 are consumed by the two spans of this trace.
	c.loadSimpleTrace("t-keep", "r-keep", "2024-06-03T00:00:00Z")
	c.retain(`{"before":"2024-06-02T00:00:00Z"}`)

	// Retention takes no Idempotency-Key and leaves the ingestion records
	// alone: replaying a stored key still returns the original response.
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans", key: "key-2",
		body: span("t-keep", "r-keep", "", "gateway", "GET /", "server", "2024-06-03T00:00:00Z", 500, "ok", count(2))})
	if status != http.StatusCreated {
		t.Fatalf("idempotent replay after retention: status %d body %s", status, raw)
	}
	if accepted, _ := decoded["accepted"].(bool); !accepted {
		t.Fatalf("idempotent replay after retention: body %s", raw)
	}
	// And a key replayed for a different span is still a conflict.
	status, _, decoded = c.do(request{method: http.MethodPost, path: "/spans", key: "key-2",
		body: span("t-keep", "r-other", "", "gateway", "GET /", "server", "2024-06-03T00:00:00Z", 500, "ok", count(2))})
	if status != http.StatusConflict {
		t.Fatalf("key reuse after retention: status %d body %v, want 409", status, decoded)
	}
}
