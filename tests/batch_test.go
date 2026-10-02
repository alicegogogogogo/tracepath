package tests

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// batchBody renders one POST /spans/batch request body from span documents.
func batchBody(spans ...string) string {
	return `{"spans":[` + strings.Join(spans, ",") + `]}`
}

func (c *client) postBatch(body string, key string) (int, string, map[string]any) {
	c.t.Helper()
	return c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
}

// mustBatch posts a batch that must succeed and returns the decoded body.
func (c *client) mustBatch(body string, key string) map[string]any {
	c.t.Helper()
	status, raw, decoded := c.postBatch(body, key)
	if status != http.StatusCreated {
		c.t.Fatalf("POST /spans/batch: status %d body %s", status, raw)
	}
	return decoded
}

// rejectBatch posts a batch that must fail and returns the error code.
func (c *client) rejectBatch(body string, key string, wanted int) string {
	c.t.Helper()
	status, raw, decoded := c.postBatch(body, key)
	if status != wanted {
		c.t.Fatalf("POST /spans/batch: wanted status %d, got %d body %s", wanted, status, raw)
	}
	cause, _ := decoded["error"].(map[string]any)
	code, _ := cause["code"].(string)
	return code
}

func acceptedAt(t *testing.T, decoded map[string]any, index int) map[string]any {
	t.Helper()
	accepted := listOf(t, decoded["accepted"])
	if index >= len(accepted) {
		t.Fatalf("accepted has %d entries, wanted index %d", len(accepted), index)
	}
	return objectOf(t, accepted[index])
}

func TestBatchIngestsAcrossTracesAndReportsProgress(t *testing.T) {
	c := newClient(t)
	// One trace whose root, child and sibling arrive in a single batch, out of
	// order, plus a complete second trace in the same request.
	childA := span("t-b1", "a", "r", "svc", "op-a", "internal", "2024-06-01T00:00:00.000000100Z", 100, "ok", nil)
	root := span("t-b1", "r", "", "svc", "op-r", "server", traceFixtureTime, 1000, "ok", count(3))
	childB := span("t-b1", "b", "r", "svc", "op-b", "internal", "2024-06-01T00:00:00.000000300Z", 100, "ok", nil)
	other := span("t-b2", "only", "", "svc", "op", "server", traceFixtureTime, 50, "ok", count(1))

	decoded := c.mustBatch(batchBody(childA, root, childB, other), c.key())
	if count := numberOf(t, decoded["count"]); count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}
	accepted := listOf(t, decoded["accepted"])
	if len(accepted) != 4 {
		t.Fatalf("accepted has %d entries, want 4", len(accepted))
	}

	// Entries follow the request order and report the trace state as of each write.
	first := acceptedAt(t, decoded, 0)
	if id, _ := first["span_id"].(string); id != "a" {
		t.Fatalf("accepted[0].span_id = %q, want a", id)
	}
	if tid, _ := first["trace_id"].(string); tid != "t-b1" {
		t.Fatalf("accepted[0].trace_id = %q, want t-b1", tid)
	}
	if ok, _ := first["accepted"].(bool); !ok {
		t.Fatalf("accepted[0].accepted = %v, want true", first["accepted"])
	}
	if complete, _ := first["complete"].(bool); complete {
		t.Fatalf("accepted[0].complete = true, want false while the root is missing")
	}
	if valid, _ := first["valid"].(bool); valid {
		t.Fatalf("accepted[0].valid = true, want false while the root is missing")
	}
	if len(listOf(t, first["violations"])) == 0 {
		t.Fatalf("accepted[0].violations is empty, want the orphan report")
	}

	third := acceptedAt(t, decoded, 2)
	if complete, _ := third["complete"].(bool); !complete {
		t.Fatalf("accepted[2].complete = false, want true after the last span")
	}
	if valid, _ := third["valid"].(bool); !valid {
		t.Fatalf("accepted[2].valid = false, want true after the last span")
	}
	if len(listOf(t, third["violations"])) != 0 {
		t.Fatalf("accepted[2].violations = %v, want empty", third["violations"])
	}

	fourth := acceptedAt(t, decoded, 3)
	if tid, _ := fourth["trace_id"].(string); tid != "t-b2" {
		t.Fatalf("accepted[3].trace_id = %q, want t-b2", tid)
	}
	if complete, _ := fourth["complete"].(bool); !complete {
		t.Fatalf("accepted[3].complete = false, want true for the one-span trace")
	}

	// The ingested spans take part in reassembly exactly like single spans.
	trace := c.get("/traces/t-b1")
	if count := numberOf(t, trace["span_count"]); count != 3 {
		t.Fatalf("trace span_count = %d, want 3", count)
	}
	if complete, _ := trace["complete"].(bool); !complete {
		t.Fatalf("trace t-b1 should be complete, got %v", trace)
	}
	path := c.get("/traces/t-b1/critical-path")
	if length := numberOf(t, path["length_ns"]); length != 1000 {
		t.Fatalf("critical path length = %d, want the root duration 1000", length)
	}
}

func TestBatchRejectsEnvelopeErrors(t *testing.T) {
	c := newClient(t)
	valid := span("t-env", "r", "", "svc", "op", "server", traceFixtureTime, 10, "ok", count(1))
	cases := []struct {
		name string
		body string
	}{
		{"empty object", `{}`},
		{"empty spans", `{"spans":[]}`},
		{"null spans", `{"spans":null}`},
		{"extra field", `{"spans":[` + valid + `],"note":"x"}`},
		{"array body", `[` + valid + `]`},
		{"scalar body", `7`},
		{"spans not an array", `{"spans":{}}`},
		{"number element", `{"spans":[1]}`},
		{"string element", `{"spans":["x"]}`},
		{"null element", `{"spans":[null]}`},
		{"element with unknown field", `{"spans":[{"trace_id":"t","span_id":"s","parent_id":null,` +
			`"service":"svc","operation":"op","kind":"server","start_time":"2024-06-01T00:00:00Z",` +
			`"duration_ns":1,"status":"ok","span_count":1,"bogus":true}]}`},
	}
	for _, tc := range cases {
		if code := c.rejectBatch(tc.body, c.key(), http.StatusBadRequest); code != "validation_error" {
			t.Fatalf("%s: code = %q, want validation_error", tc.name, code)
		}
	}

	// 1001 spans exceed the batch limit; 1000 are accepted.
	many := make([]string, 0, 1001)
	for index := 0; index < 1001; index++ {
		many = append(many, span(fmt.Sprintf("t-n%d", index), "r", "", "svc", "op", "server",
			traceFixtureTime, 1, "ok", count(1)))
	}
	if code := c.rejectBatch(batchBody(many...), c.key(), http.StatusBadRequest); code != "validation_error" {
		t.Fatalf("1001 spans: code = %q, want validation_error", code)
	}
	decoded := c.mustBatch(batchBody(many[:1000]...), c.key())
	if count := numberOf(t, decoded["count"]); count != 1000 {
		t.Fatalf("count = %d, want 1000", count)
	}
}

func TestBatchIsAtomicAndDoesNotConsumeTheKeyOnFailure(t *testing.T) {
	c := newClient(t)
	// A span that the batch will contradict.
	c.post(span("t-x", "dup", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1)))

	early := span("t-y", "early", "", "svc", "op", "server", traceFixtureTime, 10, "ok", count(1))
	duplicate := span("t-x", "dup", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1))
	late := span("t-z", "late", "", "svc", "op", "server", traceFixtureTime, 10, "ok", count(1))
	body := batchBody(early, duplicate, late)

	if code := c.rejectBatch(body, "batch-key", http.StatusConflict); code != "conflict" {
		t.Fatalf("code = %q, want conflict", code)
	}

	// Nothing of the failed batch is readable.
	listed := c.get("/traces")
	if traces := listOf(t, listed["traces"]); len(traces) != 1 {
		t.Fatalf("traces after failed batch = %d, want only the pre-existing one", len(traces))
	}
	status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-y"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-y after failed batch: status %d, want 404", status)
	}
	status, _, _ = c.do(request{method: http.MethodGet, path: "/traces/t-z"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-z after failed batch: status %d, want 404", status)
	}

	// The failed batch did not consume its key: the same key may carry a
	// corrected body.
	fixed := c.mustBatch(batchBody(early, late), "batch-key")
	if count := numberOf(t, fixed["count"]); count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}

func TestBatchReportsTheFirstProblemInRequestOrder(t *testing.T) {
	c := newClient(t)
	c.post(span("t-x", "dup", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1)))

	// The conflict on element 0 is reported, not the field error on element 1.
	duplicate := span("t-x", "dup", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1))
	badKind := span("t-f", "bad", "", "svc", "op", "bogus", traceFixtureTime, 1, "ok", count(1))
	if code := c.rejectBatch(batchBody(duplicate, badKind), c.key(), http.StatusConflict); code != "conflict" {
		t.Fatalf("conflict first: code = %q, want conflict", code)
	}

	// The field error on element 0 is reported, not the conflict on element 1.
	if code := c.rejectBatch(batchBody(badKind, duplicate), c.key(), http.StatusBadRequest); code != "validation_error" {
		t.Fatalf("validation first: code = %q, want validation_error", code)
	}

	// A duplicate inside the batch itself is a conflict and stores nothing.
	one := span("t-in", "s", "", "svc", "op", "server", traceFixtureTime, 5, "ok", count(1))
	if code := c.rejectBatch(batchBody(one, one), c.key(), http.StatusConflict); code != "conflict" {
		t.Fatalf("in-batch duplicate: code = %q, want conflict", code)
	}
	status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-in"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-in: status %d, want 404", status)
	}

	// A time conflict with an already stored parent is a conflict.
	c.post(span("t-p", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))
	outside := span("t-p", "child", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000050Z", 500, "ok", nil)
	if code := c.rejectBatch(batchBody(outside), c.key(), http.StatusConflict); code != "conflict" {
		t.Fatalf("containment: code = %q, want conflict", code)
	}
}

func TestBatchIdempotentReplayFreezesTheFirstResponse(t *testing.T) {
	c := newClient(t)
	child := span("t-r", "c", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000100Z", 100, "ok", nil)
	body := batchBody(child)

	status, firstRaw, first := c.postBatch(body, "replay-key")
	if status != http.StatusCreated {
		t.Fatalf("status %d body %s", status, firstRaw)
	}
	// The child is an orphan when the batch commits, so the first response
	// reports the trace as invalid.
	if valid, _ := acceptedAt(t, first, 0)["valid"].(bool); valid {
		t.Fatalf("first response valid = true, want false for the orphan child")
	}

	// The root arrives afterwards through the single-span route.
	c.post(span("t-r", "root", "", "svc", "op", "server", traceFixtureTime, 1000, "ok", count(2)))

	// Replaying the batch with the identical body returns the frozen first
	// response, even though the trace is complete and valid by now.
	status, replayRaw, replay := c.postBatch(body, "replay-key")
	if status != http.StatusCreated {
		t.Fatalf("replay status %d body %s", status, replayRaw)
	}
	if replayRaw != firstRaw {
		t.Fatalf("replay body differs:\n%s\n%s", firstRaw, replayRaw)
	}
	if valid, _ := acceptedAt(t, replay, 0)["valid"].(bool); valid {
		t.Fatalf("replay valid = true, want the frozen false of the first response")
	}

	// The same key with a different body is a conflict, and so is a key that
	// another operation already used.
	if code := c.rejectBatch(batchBody(child, child), "replay-key", http.StatusConflict); code != "conflict" {
		t.Fatalf("different body: code = %q, want conflict", code)
	}
	c.postWithKey(span("t-k", "s", "", "svc", "op", "server", traceFixtureTime, 1, "ok", count(1)), "shared-key")
	if code := c.rejectBatch(batchBody(child), "shared-key", http.StatusConflict); code != "conflict" {
		t.Fatalf("key of another operation: code = %q, want conflict", code)
	}
	batchKey := c.key()
	c.mustBatch(batchBody(span("t-k2", "s", "", "svc", "op", "server", traceFixtureTime, 1, "ok", count(1))), batchKey)
	singleStatus, singleRaw, _ := c.do(request{method: http.MethodPost, path: "/spans", key: batchKey,
		body: span("t-k2", "s", "", "svc", "op", "server", traceFixtureTime, 1, "ok", count(1))})
	if singleStatus != http.StatusConflict {
		t.Fatalf("batch key on POST /spans: status %d body %s, want 409", singleStatus, singleRaw)
	}
}

func TestBatchRequiresAnIdempotencyKey(t *testing.T) {
	c := newClient(t)
	valid := span("t-key", "r", "", "svc", "op", "server", traceFixtureTime, 10, "ok", count(1))

	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", body: batchBody(valid)})
	if status != http.StatusBadRequest {
		t.Fatalf("missing key: status %d body %s", status, raw)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "validation_error" {
		t.Fatalf("missing key: code = %q, want validation_error", code)
	}

	longKey := strings.Repeat("k", 201)
	if code := c.rejectBatch(batchBody(valid), longKey, http.StatusBadRequest); code != "validation_error" {
		t.Fatalf("over-long key: code = %q, want validation_error", code)
	}

	// The content type rule of the single-span route applies here too.
	status, raw, _ = c.do(request{method: http.MethodPost, path: "/spans/batch", key: c.key(),
		body: batchBody(valid), contentType: "text/plain"})
	if status != http.StatusBadRequest {
		t.Fatalf("text/plain: status %d body %s", status, raw)
	}
}

func TestBatchStateSurvivesRestart(t *testing.T) {
	database := filepath.Join(t.TempDir(), "tracepath.db")
	c := newClientOn(t, database)

	good := batchBody(
		span("t-keep", "c", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000100Z", 100, "ok", nil),
		span("t-keep", "root", "", "svc", "op", "server", traceFixtureTime, 1000, "ok", count(2)),
	)
	status, firstRaw, _ := c.postBatch(good, "restart-good")
	if status != http.StatusCreated {
		t.Fatalf("status %d body %s", status, firstRaw)
	}
	bad := batchBody(
		span("t-drop", "a", "", "svc", "op", "server", traceFixtureTime, 10, "ok", count(1)),
		span("t-drop", "a", "", "svc", "op", "server", traceFixtureTime, 10, "ok", count(1)),
	)
	c.rejectBatch(bad, "restart-bad", http.StatusConflict)

	// Reopen the same database file: the successful batch is fully visible,
	// the failed one left no trace, and the idempotency record still replays.
	reopened := newClientOn(t, database)
	trace := reopened.get("/traces/t-keep")
	if count := numberOf(t, trace["span_count"]); count != 2 {
		t.Fatalf("span_count after restart = %d, want 2", count)
	}
	if complete, _ := trace["complete"].(bool); !complete {
		t.Fatalf("t-keep should be complete after restart, got %v", trace)
	}
	status, _, _ = reopened.do(request{method: http.MethodGet, path: "/traces/t-drop"})
	if status != http.StatusNotFound {
		t.Fatalf("GET /traces/t-drop after restart: status %d, want 404", status)
	}
	status, replayRaw, _ := reopened.postBatch(good, "restart-good")
	if status != http.StatusCreated {
		t.Fatalf("replay after restart: status %d body %s", status, replayRaw)
	}
	if replayRaw != firstRaw {
		t.Fatalf("replay after restart differs:\n%s\n%s", firstRaw, replayRaw)
	}
}
