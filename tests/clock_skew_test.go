package tests

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"tracepath/internal/tracepath"
)

// Times expressed relative to the 2024-06-01T00:00:00Z fixture origin.
const (
	fiveBefore  = "2024-05-31T23:59:59.999999995Z"
	sixBefore   = "2024-05-31T23:59:59.999999994Z"
	fiveAfter   = "2024-06-01T00:00:00.000000005Z"
	skewOrigin  = traceFixtureTime
	skewHundred = 100
)

// newClientWithTolerance builds a client whose service widens every parent/child
// containment check by toleranceNS nanoseconds at each end.
func newClientWithTolerance(t *testing.T, toleranceNS int64) *client {
	t.Helper()
	return newClientOnWithTolerance(t, filepath.Join(t.TempDir(), "tracepath.db"), toleranceNS)
}

func newClientOnWithTolerance(t *testing.T, database string, toleranceNS int64) *client {
	t.Helper()
	store, err := tracepath.OpenStore(database)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service, err := tracepath.NewServiceWithConfig(store, tracepath.ServiceConfig{
		Clock:                func() time.Time { return fixedClock },
		ClockSkewToleranceNS: toleranceNS,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	server := httptest.NewServer(tracepath.NewServer(service))
	t.Cleanup(server.Close)
	return &client{t: t, server: server}
}

func violationCodes(t *testing.T, decoded map[string]any) []string {
	t.Helper()
	cause := objectOf(t, decoded["error"])
	codes := []string{}
	for _, item := range listOf(t, cause["violations"]) {
		violation := objectOf(t, item)
		code, _ := violation["code"].(string)
		codes = append(codes, code)
	}
	return codes
}

func hasCode(codes []string, wanted string) bool {
	for _, code := range codes {
		if code == wanted {
			return true
		}
	}
	return false
}

// TestClockSkewToleranceAcceptsExactlyAtTheBoundary pins the half-open
// semantics [parentStart-T, parentEnd+T): a child touching either widened
// boundary is contained; one nanosecond beyond is not. Children keep the
// parent's declared duration so the tolerance-independent subtree check is
// neutral and only interval containment is exercised.
func TestClockSkewToleranceAcceptsExactlyAtTheBoundary(t *testing.T) {
	c := newClientWithTolerance(t, 5)

	// Starts exactly 5ns early, ends at 95: the start sits on the widened
	// boundary, so the child is accepted and the trace stays valid.
	c.post(span("t-edge", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	accepted := c.post(span("t-edge", "edge", "root", "db", "query", "client", fiveBefore, skewHundred, "ok", nil))
	if valid, _ := accepted["valid"].(bool); !valid {
		t.Fatalf("a child on the widened start boundary must be valid: %v", accepted)
	}

	// Starts at 5, ends exactly on parentEnd+T: the end boundary is inclusive
	// as containment as well.
	c.post(span("t-end", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	accepted = c.post(span("t-end", "edge", "root", "db", "query", "client", fiveAfter, skewHundred, "ok", nil))
	if valid, _ := accepted["valid"].(bool); !valid {
		t.Fatalf("a child on the widened end boundary must be valid: %v", accepted)
	}

	// One nanosecond of extra overrun at either end is still rejected while the
	// parent already exists.
	c.post(span("t-bad-start", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	if _, code, _ := c.reject(span("t-bad-start", "edge", "root", "db", "query", "client",
		sixBefore, skewHundred, "ok", nil), http.StatusConflict); code != "conflict" {
		t.Fatalf("a start 6ns early must conflict under tolerance 5, code=%s", code)
	}
	c.post(span("t-bad-end", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	if _, code, _ := c.reject(span("t-bad-end", "edge", "root", "db", "query", "client",
		skewOrigin, 106, "ok", nil), http.StatusConflict); code != "conflict" {
		t.Fatalf("an end 6ns late must conflict under tolerance 5, code=%s", code)
	}
}

// TestClockSkewToleranceComparesEndsIndependently ensures slack at one end can
// never absorb an overrun at the other end.
func TestClockSkewToleranceComparesEndsIndependently(t *testing.T) {
	c := newClientWithTolerance(t, 5)

	// Start sits exactly on the widened boundary, but the end is 6ns out: the
	// start margin must not offset it.
	c.post(span("t-a", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	if _, code, _ := c.reject(span("t-a", "edge", "root", "db", "query", "client",
		fiveBefore, 111, "ok", nil), http.StatusConflict); code != "conflict" {
		t.Fatalf("end overrun must not be offset by start slack")
	}

	// Mirror case: end inside, start 6ns early.
	c.post(span("t-b", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	if _, code, _ := c.reject(span("t-b", "edge", "root", "db", "query", "client",
		sixBefore, skewHundred, "ok", nil), http.StatusConflict); code != "conflict" {
		t.Fatalf("start overrun must not be offset by end slack")
	}
}

// TestClockSkewReassemblyBeyondToleranceStaysInvalid covers a child accepted
// while its parent is still missing: reassembly must apply the same tolerance
// and report the violation once the trace is complete.
func TestClockSkewReassemblyBeyondToleranceStaysInvalid(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tolerance int64
		valid     bool
	}{
		{name: "beyond tolerance", tolerance: 5, valid: false},
		{name: "within tolerance", tolerance: 6, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClientWithTolerance(t, tc.tolerance)
			// Child arrives first and starts 6ns early, so ingestion cannot
			// check containment yet and accepts it. The duration equals the
			// root's, keeping the subtree check neutral: only the interval end
			// is under test.
			c.post(span("t-r", "edge", "root", "db", "query", "client", sixBefore, skewHundred, "ok", nil))
			c.post(span("t-r", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))

			status, raw, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-r"})
			if tc.valid {
				if status != http.StatusOK {
					t.Fatalf("status %d body %s", status, raw)
				}
				return
			}
			if status != http.StatusConflict {
				t.Fatalf("status %d body %s", status, raw)
			}
			cause := objectOf(t, decoded["error"])
			if code, _ := cause["code"].(string); code != "trace_invalid" {
				t.Fatalf("error code %s body %s", code, raw)
			}
			codes := violationCodes(t, decoded)
			if len(codes) != 1 || codes[0] != "time_not_contained" {
				t.Fatalf("want one time_not_contained violation, got %v", codes)
			}

			// The critical path stays reachable with HTTP 200 and carries the
			// same violation while refusing to claim validity.
			path := c.get("/traces/t-r/critical-path")
			if valid, _ := path["valid"].(bool); valid {
				t.Fatalf("critical path of an invalid trace must not be valid")
			}
			pathViolations := listOf(t, path["violations"])
			if len(pathViolations) != 1 || objectOf(t, pathViolations[0])["code"] != "time_not_contained" {
				t.Fatalf("critical path must expose the same violation: %v", path["violations"])
			}
		})
	}
}

// TestClockSkewBatchConflictIsAtomicAndLeavesKeyFree exercises the batch path
// with the parent already present: an out-of-range child fails the whole batch
// atomically and the idempotency key stays usable.
func TestClockSkewBatchConflictIsAtomicAndLeavesKeyFree(t *testing.T) {
	c := newClientWithTolerance(t, 5)
	root := span("t-batch", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2))
	badChild := span("t-batch", "edge", "root", "db", "query", "client", sixBefore, 106, "ok", nil)
	key := c.key()

	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: batch(root, badChild)})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	if code, _ := objectOf(t, decoded["error"])["code"].(string); code != "conflict" {
		t.Fatalf("error code %s", code)
	}
	// Nothing was committed.
	if miss, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-batch"}); miss != http.StatusNotFound {
		t.Fatalf("failed batch must not leave a trace, status=%d", miss)
	}

	// The failed batch never occupied the key: a different valid body reusing
	// the same key succeeds.
	retry := batch(span("t-ok", "root", "", "svc", "op", "server", skewOrigin, 10, "ok", count(1)))
	if status, raw, _ = c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: retry}); status != http.StatusCreated {
		t.Fatalf("the key must stay usable after an atomic failure: status %d body %s", status, raw)
	}
}

// TestClockSkewBatchAcceptedReplayReturnsFirstSnapshot checks the idempotent
// replay of a tolerance-accepted batch.
func TestClockSkewBatchAcceptedReplayReturnsFirstSnapshot(t *testing.T) {
	c := newClientWithTolerance(t, 5)
	root := span("t-rep", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2))
	edge := span("t-rep", "edge", "root", "db", "query", "client", fiveAfter, skewHundred, "ok", nil)
	body := batch(root, edge)
	key := c.key()

	first, firstRaw := mustPostBatch(t, c, key, body)
	items := acceptedItems(t, first)
	if len(items) != 2 {
		t.Fatalf("accepted items: %v", items)
	}
	if valid, _ := items[1]["valid"].(bool); !valid {
		t.Fatalf("the boundary child must leave a valid trace snapshot: %v", items[1])
	}
	_, secondRaw := mustPostBatch(t, c, key, body)
	if firstRaw != secondRaw {
		t.Fatalf("batch replay must return the stored first response\nfirst:  %s\nsecond: %s", firstRaw, secondRaw)
	}
}

func mustPostBatch(t *testing.T, c *client, key string, body string) (map[string]any, string) {
	t.Helper()
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans/batch", key: key, body: body})
	if status != http.StatusCreated {
		t.Fatalf("POST /spans/batch: status %d body %s", status, raw)
	}
	return decoded, raw
}

// TestClockSkewSingleSpanReplayReturnsFirstResult replays a tolerance-accepted
// single span and then proves a rejected request never consumes its key.
func TestClockSkewSingleSpanReplayReturnsFirstResult(t *testing.T) {
	c := newClientWithTolerance(t, 5)
	c.post(span("t-s", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(3)))
	edge := span("t-s", "edge", "root", "db", "query", "client", fiveBefore, skewHundred, "ok", nil)
	key := c.key()

	status, firstRaw, first := c.do(request{method: http.MethodPost, path: "/spans", key: key, body: edge})
	if status != http.StatusCreated {
		t.Fatalf("status %d body %s", status, firstRaw)
	}
	if start, _ := first["start_time"].(string); start != fiveBefore {
		t.Fatalf("ingested start_time must be echoed verbatim: %s", start)
	}
	status, secondRaw, _ := c.do(request{method: http.MethodPost, path: "/spans", key: key, body: edge})
	if status != http.StatusCreated || firstRaw != secondRaw {
		t.Fatalf("replay must return the first result: status %d\nfirst:  %s\nsecond: %s", status, firstRaw, secondRaw)
	}

	// A conflicting attempt does not occupy its key: repeating it is evaluated
	// against state again and stays a conflict.
	failed := span("t-s", "late", "root", "db", "query", "client", skewOrigin, 106, "ok", nil)
	failedKey := c.key()
	if status, _, _ = c.do(request{method: http.MethodPost, path: "/spans", key: failedKey, body: failed}); status != http.StatusConflict {
		t.Fatalf("an overrun beyond tolerance must conflict, status=%d", status)
	}
	if status, _, _ = c.do(request{method: http.MethodPost, path: "/spans", key: failedKey, body: failed}); status != http.StatusConflict {
		t.Fatalf("the same losing attempt must be evaluated again, status=%d", status)
	}
	// The key is still free for a different, tolerance-acceptable span.
	salvage := span("t-s", "edge2", "root", "db", "query", "client", fiveAfter, skewHundred, "ok", nil)
	if status, raw, _ := c.do(request{method: http.MethodPost, path: "/spans", key: failedKey, body: salvage}); status != http.StatusCreated {
		t.Fatalf("a rejected request must not consume its key: status %d body %s", status, raw)
	}
}

// TestClockSkewDoesNotRelaxSiblingOverlap positions two siblings that only fit
// their parent thanks to the tolerance, yet overlap each other: sibling_overlap
// must still fire and time_not_contained must not.
func TestClockSkewDoesNotRelaxSiblingOverlap(t *testing.T) {
	c := newClientWithTolerance(t, 5)
	// Both children run -5..45: interval-contained only because of the
	// tolerance and fully identical, hence overlapping.
	c.post(span("t-sib", "c1", "root", "db", "query", "client", fiveBefore, 50, "ok", nil))
	c.post(span("t-sib", "c2", "root", "db", "query", "client", fiveBefore, 50, "ok", nil))
	c.post(span("t-sib", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(3)))

	status, raw, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-sib"})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	codes := violationCodes(t, decoded)
	if !hasCode(codes, "sibling_overlap") {
		t.Fatalf("sibling overlap must survive the tolerance: %v", codes)
	}
	if hasCode(codes, "time_not_contained") {
		t.Fatalf("tolerance-accepted siblings must not report containment violations: %v", codes)
	}

	path := c.get("/traces/t-sib/critical-path")
	pathCodes := []string{}
	for _, item := range listOf(t, path["violations"]) {
		pathCodes = append(pathCodes, objectOf(t, item)["code"].(string))
	}
	if !hasCode(pathCodes, "sibling_overlap") {
		t.Fatalf("critical path must keep exposing sibling_overlap: %v", pathCodes)
	}
}

// TestClockSkewDoesNotRelaxSubtreeDuration verifies that a child accepted by a
// widened interval still reports the declared-duration subtree cross-check:
// tolerance never offsets duration arithmetic.
func TestClockSkewDoesNotRelaxSubtreeDuration(t *testing.T) {
	c := newClientWithTolerance(t, 5)
	// Child 0..105 ends exactly on parentEnd+T so interval containment passes,
	// but its declared subtree (105) exceeds the parent duration (100).
	c.post(span("t-sub", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	c.post(span("t-sub", "edge", "root", "db", "query", "client", skewOrigin, 105, "ok", nil))

	status, raw, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-sub"})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	codes := violationCodes(t, decoded)
	if len(codes) != 1 || codes[0] != "time_not_contained" {
		t.Fatalf("the subtree cross-check must still fire: %v", codes)
	}
	path := c.get("/traces/t-sub/critical-path")
	if valid, _ := path["valid"].(bool); valid {
		t.Fatalf("critical path must not claim validity for the subtree violation")
	}
}

// TestClockSkewAcceptedTraceParticipatesNormally follows a trace that is only
// valid under the tolerance through every read: canonical raw timestamps are
// preserved, self time and critical path are computed from declared values,
// error propagation, list filters and the service graph all include it.
func TestClockSkewAcceptedTraceParticipatesNormally(t *testing.T) {
	c := newClientWithTolerance(t, 5)

	// t-ok: root 0..100, child db 5..105 (duration 100). The child end sits on
	// the widened boundary while its declared duration equals the root's, so
	// the subtree check passes and the self-time invariant is preserved.
	c.post(span("t-ok", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	ingested := c.post(span("t-ok", "edge", "root", "db", "query", "client", fiveAfter, skewHundred, "ok", nil))
	if complete, _ := ingested["complete"].(bool); !complete {
		t.Fatalf("trace must be complete: %v", ingested)
	}
	if valid, _ := ingested["valid"].(bool); !valid {
		t.Fatalf("trace must be valid under tolerance 5: %v", ingested)
	}
	if start, _ := ingested["start_time"].(string); start != fiveAfter {
		t.Fatalf("POST /spans must echo the original start_time: %s", start)
	}

	trace := c.get("/traces/t-ok")
	if valid, _ := trace["valid"].(bool); !valid {
		t.Fatalf("GET trace must be valid: %v", trace)
	}
	rootNode := objectOf(t, trace["root"])
	if start, _ := rootNode["start_time"].(string); start != skewOrigin {
		t.Fatalf("root start_time rewritten: %s", start)
	}
	children := listOf(t, rootNode["children"])
	if len(children) != 1 {
		t.Fatalf("root children: %v", children)
	}
	childNode := objectOf(t, children[0])
	if start, _ := childNode["start_time"].(string); start != fiveAfter {
		t.Fatalf("child start_time must be the original value: %s", start)
	}
	// end_time is derived from the declared values, never shifted by T:
	// 5ns + 100ns = 105ns.
	if end, _ := childNode["end_time"].(string); end != "2024-06-01T00:00:00.000000105Z" {
		t.Fatalf("child end_time must stay derived from declared values: %s", end)
	}
	if self := numberOf(t, childNode["self_time_ns"]); self != 100 {
		t.Fatalf("child self_time_ns = %d, want 100", self)
	}
	if self := numberOf(t, rootNode["self_time_ns"]); self != 0 {
		t.Fatalf("root self_time_ns = %d, want 0 (child declared duration dominates)", self)
	}

	// Critical path is valid and weighted from declared durations; the path
	// length stays equal to the root duration (100).
	path := c.get("/traces/t-ok/critical-path")
	if valid, _ := path["valid"].(bool); !valid {
		t.Fatalf("critical path must be valid: %v", path)
	}
	if length := numberOf(t, path["length_ns"]); length != 100 {
		t.Fatalf("critical path length = %d, want 100", length)
	}
	steps := listOf(t, path["path"])
	if len(steps) != 2 {
		t.Fatalf("critical path steps: %v", steps)
	}
	if start, _ := objectOf(t, steps[1])["start_time"].(string); start != fiveAfter {
		t.Fatalf("critical path must show the original child start: %s", start)
	}

	// A second tolerance-valid trace carries an error span: propagation still
	// marks the root.
	c.post(span("t-err", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "error", count(2)))
	c.post(span("t-err", "edge", "root", "db", "query", "client", fiveAfter, skewHundred, "error", nil))
	errorTrace := c.get("/traces/t-err")
	errorRoot := objectOf(t, errorTrace["root"])
	if propagated, _ := errorRoot["propagated"].(bool); !propagated {
		t.Fatalf("the erroring child must propagate to the root")
	}

	// List filters include the tolerance-accepted trace like any other.
	listing := c.get("/traces?valid=true")
	if ids := traceIDsOf(t, listing); !containsString(ids, "t-ok") {
		t.Fatalf("valid filter must include the tolerance-accepted trace: %v", ids)
	}
	errors := c.get("/traces?status=error")
	errorIDs := traceIDsOf(t, errors)
	if !containsString(errorIDs, "t-err") || containsString(errorIDs, "t-ok") {
		t.Fatalf("status filter mismatch: %v", errorIDs)
	}

	// The service graph counts both usable traces and keeps the caller/callee
	// edge.
	graph := c.get("/services/graph")
	stats := objectOf(t, graph["stats"])
	if count := numberOf(t, stats["trace_count"]); count != 2 {
		t.Fatalf("graph trace_count = %d, want both tolerance-valid traces", count)
	}
	edges := listOf(t, graph["edges"])
	if len(edges) != 1 {
		t.Fatalf("graph edges: %v", edges)
	}
	edge := objectOf(t, edges[0])
	if edge["caller"] != "svc" || edge["callee"] != "db" {
		t.Fatalf("unexpected edge: %v", edge)
	}
}

// TestClockSkewStrictDefaultRejectsOverhang proves the zero-tolerance service
// the existing constructor builds still applies exact containment.
func TestClockSkewStrictDefaultRejectsOverhang(t *testing.T) {
	c := newClient(t)
	c.post(span("t-zero", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	// Five nanoseconds of skew must be a conflict in strict mode.
	if _, code, _ := c.reject(span("t-zero", "edge", "root", "db", "query", "client",
		fiveBefore, skewHundred, "ok", nil), http.StatusConflict); code != "conflict" {
		t.Fatalf("strict mode must reject a child starting 5ns early")
	}
}

// TestClockSkewRestartKeepsDeterministicReads persists a tolerance-accepted
// trace and reopens the database with the same tolerance: reads must come back
// identical, and without any new mandatory field in the database file.
func TestClockSkewRestartKeepsDeterministicReads(t *testing.T) {
	database := filepath.Join(t.TempDir(), "tracepath.db")
	first := newClientOnWithTolerance(t, database, 5)
	first.post(span("t-db", "root", "", "svc", "op", "server", skewOrigin, skewHundred, "ok", count(2)))
	first.post(span("t-db", "edge", "root", "db", "query", "client", fiveAfter, skewHundred, "ok", nil))
	traceBefore := first.get("/traces/t-db")
	first.server.Close()

	second := newClientOnWithTolerance(t, database, 5)
	traceAfter := second.get("/traces/t-db")
	if numberOf(t, objectOf(t, traceAfter["root"])["duration_ns"]) !=
		numberOf(t, objectOf(t, traceBefore["root"])["duration_ns"]) {
		t.Fatalf("root duration changed across restart")
	}
	children := listOf(t, objectOf(t, traceAfter["root"])["children"])
	child := objectOf(t, children[0])
	if child["start_time"] != fiveAfter || child["end_time"] != "2024-06-01T00:00:00.000000105Z" {
		t.Fatalf("normalized times changed across restart: %v", child)
	}
	if valid, _ := traceAfter["valid"].(bool); !valid {
		t.Fatalf("the trace must stay valid after a restart with the same tolerance")
	}

	// The same database reopened strictly must surface the skew as an invalid
	// trace: tolerance is process configuration, never persisted state.
	strict := newClientOn(t, database)
	status, raw, _ := strict.do(request{method: http.MethodGet, path: "/traces/t-db"})
	if status != http.StatusConflict {
		t.Fatalf("strict reopen must reject the skew: status %d body %s", status, raw)
	}
}

// TestNewServiceWithConfigRejectsNegativeTolerance fails before the service
// can serve traffic.
func TestNewServiceWithConfigRejectsNegativeTolerance(t *testing.T) {
	store, err := tracepath.OpenStore(filepath.Join(t.TempDir(), "tracepath.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := tracepath.NewServiceWithConfig(store, tracepath.ServiceConfig{ClockSkewToleranceNS: -1}); err == nil {
		t.Fatalf("a negative tolerance must be rejected")
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
