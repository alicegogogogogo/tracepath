package tests

import (
	"net/http"
	"path/filepath"
	"testing"
)

// retain posts one retention request and requires success.
func (c *client) retain(body string) map[string]any {
	c.t.Helper()
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/traces/retention", body: body})
	if status != http.StatusOK {
		c.t.Fatalf("POST /traces/retention: wanted status 200, got %d body %s", status, raw)
	}
	return decoded
}

// rejectRetention posts one retention request that must fail with a
// validation_error.
func (c *client) rejectRetention(r request) (int, string) {
	c.t.Helper()
	r.method = http.MethodPost
	r.path = "/traces/retention"
	status, raw, decoded := c.do(r)
	if status != http.StatusBadRequest {
		c.t.Fatalf("POST /traces/retention: wanted status 400, got %d body %s", status, raw)
	}
	cause := objectOf(c.t, decoded["error"])
	code, _ := cause["code"].(string)
	if code != "validation_error" {
		c.t.Fatalf("POST /traces/retention: wanted validation_error, got %q body %s", code, raw)
	}
	return status, code
}

// loadTrace ingests one complete two-span trace whose root starts at start.
func (c *client) loadTrace(traceID string, service string, start string) {
	c.t.Helper()
	child := span(traceID, traceID+"-child", traceID+"-root", service, "work", "internal", start, 100, "ok", nil)
	root := span(traceID, traceID+"-root", "", service, "handle", "server", start, 500, "ok", count(2))
	c.post(child)
	c.post(root)
}

func TestRetentionDeletesOnlyStrictlyOlderTraces(t *testing.T) {
	c := newClient(t)
	c.loadTrace("t-old", "gateway", "2024-06-01T00:00:00Z")
	c.loadTrace("t-edge", "gateway", "2024-06-02T00:00:00Z")
	c.loadTrace("t-new", "gateway", "2024-06-03T00:00:00.000000001Z")

	response := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	if got := response["before"]; got != "2024-06-02T00:00:00Z" {
		t.Fatalf("before echo: got %v", got)
	}
	if got := numberOf(t, response["deleted_traces"]); got != 1 {
		t.Fatalf("deleted_traces: got %d", got)
	}
	if got := numberOf(t, response["deleted_spans"]); got != 2 {
		t.Fatalf("deleted_spans: got %d", got)
	}
	if got := numberOf(t, response["retained_traces"]); got != 2 {
		t.Fatalf("retained_traces: got %d", got)
	}
	if got := numberOf(t, response["retained_spans"]); got != 4 {
		t.Fatalf("retained_spans: got %d", got)
	}

	// The boundary trace starts exactly at before and must survive, as must
	// the newer one; the deleted trace is gone from every read endpoint.
	traces := listOf(t, c.get("/traces")["traces"])
	if len(traces) != 2 {
		t.Fatalf("GET /traces: wanted 2 traces, got %d", len(traces))
	}
	status, raw, _ := c.do(request{method: http.MethodGet, path: "/traces/t-old"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-old: wanted 404, got %d body %s", status, raw)
	}
	status, raw, _ = c.do(request{method: http.MethodGet, path: "/traces/t-old/critical-path"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-old/critical-path: wanted 404, got %d body %s", status, raw)
	}
	if _, ok := c.get("/traces/t-edge")["root"]; !ok {
		t.Fatalf("GET /traces/t-edge: boundary trace must be retained")
	}
	graph := c.get("/services/graph")
	stats := objectOf(t, graph["stats"])
	if got := numberOf(t, stats["trace_count"]); got != 2 {
		t.Fatalf("services/graph trace_count: got %d", got)
	}
	if got := numberOf(t, stats["span_count"]); got != 4 {
		t.Fatalf("services/graph span_count: got %d", got)
	}
}

func TestRetentionEchoesBeforeAsUTC(t *testing.T) {
	c := newClient(t)
	response := c.retain(`{"before":"2024-06-02T02:00:00.500000001+02:00"}`)
	if got := response["before"]; got != "2024-06-02T00:00:00.500000001Z" {
		t.Fatalf("before echo: got %v", got)
	}
	if got := numberOf(t, response["deleted_traces"]); got != 0 {
		t.Fatalf("deleted_traces on empty database: got %d", got)
	}
}

func TestRetentionComparesAtNanosecondPrecision(t *testing.T) {
	c := newClient(t)
	c.loadTrace("t-nano", "gateway", "2024-06-02T00:00:00.000000001Z")
	response := c.retain(`{"before":"2024-06-02T00:00:00.000000001Z"}`)
	if got := numberOf(t, response["deleted_traces"]); got != 0 {
		t.Fatalf("equal instant must be retained, deleted %d", got)
	}
	response = c.retain(`{"before":"2024-06-02T00:00:00.000000002Z"}`)
	if got := numberOf(t, response["deleted_traces"]); got != 1 {
		t.Fatalf("one nanosecond later must delete, deleted %d", got)
	}
}

func TestRetentionKeepsTracesWithoutRoot(t *testing.T) {
	c := newClient(t)
	// A trace whose root was never ingested has no determinable start time.
	orphan := span("t-partial", "s-child", "s-missing", "db", "query", "internal", "2024-01-01T00:00:00Z", 100, "ok", nil)
	c.post(orphan)
	c.loadTrace("t-old", "gateway", "2024-01-02T00:00:00Z")

	response := c.retain(`{"before":"2024-06-01T00:00:00Z"}`)
	if got := numberOf(t, response["deleted_traces"]); got != 1 {
		t.Fatalf("deleted_traces: got %d", got)
	}
	if got := numberOf(t, response["retained_traces"]); got != 1 {
		t.Fatalf("retained_traces: got %d", got)
	}
	if got := numberOf(t, response["retained_spans"]); got != 1 {
		t.Fatalf("retained_spans: got %d", got)
	}
	traces := listOf(t, c.get("/traces")["traces"])
	if len(traces) != 1 || objectOf(t, traces[0])["trace_id"] != "t-partial" {
		t.Fatalf("rootless trace must be retained, got %v", traces)
	}
}

func TestRetentionIsRepeatable(t *testing.T) {
	c := newClient(t)
	c.loadTrace("t-old", "gateway", "2024-06-01T00:00:00Z")
	c.loadTrace("t-new", "gateway", "2024-06-03T00:00:00Z")

	first := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	second := c.retain(`{"before":"2024-06-02T00:00:00Z"}`)
	if got := numberOf(t, second["deleted_traces"]); got != 0 {
		t.Fatalf("repeated request must delete nothing, got %d", got)
	}
	if got := numberOf(t, second["retained_traces"]); got != 1 {
		t.Fatalf("repeated request must retain the same trace, got %d", got)
	}
	if first["before"] != second["before"] {
		t.Fatalf("before echo must be stable: %v vs %v", first["before"], second["before"])
	}
}

func TestRetentionSurvivesRestart(t *testing.T) {
	database := filepath.Join(t.TempDir(), "tracepath.db")
	c := newClientOn(t, database)
	c.loadTrace("t-old", "gateway", "2024-06-01T00:00:00Z")
	c.loadTrace("t-new", "gateway", "2024-06-03T00:00:00Z")
	c.retain(`{"before":"2024-06-02T00:00:00Z"}`)

	reopened := newClientOn(t, database)
	traces := listOf(t, reopened.get("/traces")["traces"])
	if len(traces) != 1 || objectOf(t, traces[0])["trace_id"] != "t-new" {
		t.Fatalf("after restart only t-new may remain, got %v", traces)
	}
	status, raw, _ := reopened.do(request{method: http.MethodGet, path: "/traces/t-old"})
	if status != http.StatusNotFound {
		t.Fatalf("deleted trace must stay deleted after restart, got %d body %s", status, raw)
	}
}

func TestRetentionValidation(t *testing.T) {
	c := newClient(t)
	c.loadTrace("t-1", "gateway", "2024-06-01T00:00:00Z")

	cases := []request{
		{body: `{"before":"2024-06-02T00:00:00Z"}`, contentType: "text/plain"},
		{body: `{"before":"2024-06-02T00:00:00Z"}`, omitType: true},
		{body: `not json`},
		{body: `[{"before":"2024-06-02T00:00:00Z"}]`},
		{body: `{}`},
		{body: `{"before":""}`},
		{body: `{"before":"2024-06-02"}`},
		{body: `{"before":"not-a-time"}`},
		{body: `{"before":123}`},
		{body: `{"before":"2024-06-02T00:00:00Z","extra":true}`},
		{body: `{"before":"2024-06-02T00:00:00Z"} trailing`},
	}
	for _, tc := range cases {
		c.rejectRetention(tc)
	}

	// Every rejected request left the stored trace untouched.
	traces := listOf(t, c.get("/traces")["traces"])
	if len(traces) != 1 {
		t.Fatalf("rejected requests must not delete anything, got %v", traces)
	}
}

func TestRetentionDoesNotConsumeIdempotencyKey(t *testing.T) {
	c := newClient(t)
	c.loadTrace("t-1", "gateway", "2024-06-01T00:00:00Z")
	// The retention request carries an Idempotency-Key header; it must be
	// ignored, so the same key still works for a later span ingestion.
	status, raw, _ := c.do(request{
		method: http.MethodPost,
		path:   "/traces/retention",
		key:    "shared-key",
		body:   `{"before":"2024-06-02T00:00:00Z"}`,
	})
	if status != http.StatusOK {
		t.Fatalf("POST /traces/retention: got %d body %s", status, raw)
	}
	fresh := span("t-2", "s-root", "", "gateway", "handle", "server", "2024-06-03T00:00:00Z", 500, "ok", count(1))
	c.postWithKey(fresh, "shared-key")
}
