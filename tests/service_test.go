package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"tracepath/internal/tracepath"
)

// fixedClock makes every created_at value and idempotency replay reproducible.
var fixedClock = time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

type request struct {
	method      string
	path        string
	key         string
	body        string
	contentType string
	omitType    bool
}

type client struct {
	t      *testing.T
	server *httptest.Server
	keys   int
}

func newClient(t *testing.T) *client {
	t.Helper()
	return newClientOn(t, filepath.Join(t.TempDir(), "tracepath.db"))
}

func newClientOn(t *testing.T, database string) *client {
	t.Helper()
	store, err := tracepath.OpenStore(database)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service, err := tracepath.NewService(store, func() time.Time { return fixedClock })
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	server := httptest.NewServer(tracepath.NewServer(service))
	t.Cleanup(server.Close)
	return &client{t: t, server: server}
}

func (c *client) do(r request) (int, string, map[string]any) {
	c.t.Helper()
	var body io.Reader
	if r.body != "" {
		body = bytes.NewBufferString(r.body)
	}
	httpRequest, err := http.NewRequest(r.method, c.server.URL+r.path, body)
	if err != nil {
		c.t.Fatalf("build request: %v", err)
	}
	if !r.omitType {
		contentType := r.contentType
		if contentType == "" {
			contentType = "application/json"
		}
		httpRequest.Header.Set("Content-Type", contentType)
	}
	if r.key != "" {
		httpRequest.Header.Set("Idempotency-Key", r.key)
	}
	response, err := c.server.Client().Do(httpRequest)
	if err != nil {
		c.t.Fatalf("%s %s: %v", r.method, r.path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		c.t.Fatalf("read %s %s: %v", r.method, r.path, err)
	}
	decoded := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			c.t.Fatalf("%s %s returned non-object JSON %s", r.method, r.path, raw)
		}
	}
	return response.StatusCode, string(raw), decoded
}

func (c *client) get(path string) map[string]any {
	c.t.Helper()
	status, raw, decoded := c.do(request{method: http.MethodGet, path: path})
	if status != http.StatusOK {
		c.t.Fatalf("GET %s: status %d body %s", path, status, raw)
	}
	return decoded
}

func (c *client) post(body string) map[string]any {
	c.t.Helper()
	return c.postWithKey(body, c.key())
}

func (c *client) postWithKey(body string, key string) map[string]any {
	c.t.Helper()
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans", key: key, body: body})
	if status != http.StatusCreated {
		c.t.Fatalf("POST /spans: status %d body %s", status, raw)
	}
	return decoded
}

// reject posts a span that must be refused and returns the failing status and code.
func (c *client) reject(body string, wanted int) (int, string, map[string]any) {
	c.t.Helper()
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans", key: c.key(), body: body})
	if status != wanted {
		c.t.Fatalf("POST /spans: wanted status %d, got %d body %s", wanted, status, raw)
	}
	cause, _ := decoded["error"].(map[string]any)
	code, _ := cause["code"].(string)
	return status, code, decoded
}

func (c *client) key() string {
	c.keys++
	return fmt.Sprintf("key-%d", c.keys)
}

// span renders one span JSON document. start, duration and the key order are
// fully explicit so every fixture is reproducible.
func span(traceID string, spanID string, parentID string, service string, operation string,
	kind string, start string, durationNS int64, status string, spanCount *int) string {
	document := map[string]any{
		"trace_id":    traceID,
		"span_id":     spanID,
		"service":     service,
		"operation":   operation,
		"kind":        kind,
		"start_time":  start,
		"duration_ns": durationNS,
		"status":      status,
	}
	if parentID == "" {
		document["parent_id"] = nil
	} else {
		document["parent_id"] = parentID
	}
	if spanCount != nil {
		document["span_count"] = *spanCount
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func count(value int) *int { return &value }

// traceFixtureTime is the fixed wall clock every synthetic trace uses.
const traceFixtureTime = "2024-06-01T00:00:00Z"

// loadFixture ingests a six-span trace out of order (children first) so that
// deferred validation is exercised on every run. Direct children of a span
// never overlap, which is what keeps self_time exact:
//
//	gateway    0–1000    self 100
//	└─ auth    100–300   self 200
//	└─ db      350–950   self 150
//	   └─ query1 400–600 self 200
//	   └─ query2 650–900 self 250   (status error)
//	└─ queue   950–1000  self 50
func (c *client) loadFixture() string {
	c.t.Helper()
	root := span("t-1", "s-root", "", "gateway", "GET /checkout", "server", traceFixtureTime, 1000, "ok", count(6))
	auth := span("t-1", "s-auth", "s-root", "auth", "verify", "client", "2024-06-01T00:00:00.000000100Z", 200, "ok", nil)
	db := span("t-1", "s-db", "s-root", "db", "select", "client", "2024-06-01T00:00:00.000000350Z", 600, "ok", nil)
	query1 := span("t-1", "s-q1", "s-db", "db", "query", "internal", "2024-06-01T00:00:00.000000400Z", 200, "ok", nil)
	query2 := span("t-1", "s-q2", "s-db", "db", "query", "internal", "2024-06-01T00:00:00.000000650Z", 250, "error", nil)
	publish := span("t-1", "s-pub", "s-root", "queue", "publish", "producer", "2024-06-01T00:00:00.000000950Z", 50, "ok", nil)
	c.post(query1)
	c.post(query2)
	c.post(publish)
	c.post(db)
	c.post(auth)
	last := c.post(root)
	if complete, _ := last["complete"].(bool); !complete {
		c.t.Fatalf("fixture should be complete, got %v", last)
	}
	return "t-1"
}

func listOf(t *testing.T, value any) []any {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("expected a JSON array, got %T", value)
	}
	return items
}

func numberOf(t *testing.T, value any) int64 {
	t.Helper()
	number, ok := value.(float64)
	if !ok {
		t.Fatalf("expected a JSON number, got %T", value)
	}
	return int64(number)
}

func objectOf(t *testing.T, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected a JSON object, got %T", value)
	}
	return object
}

// walkSpans returns every node of the tree in pre-order.
func walkSpans(t *testing.T, root any) []map[string]any {
	t.Helper()
	visited := []map[string]any{}
	var visit func(node any)
	visit = func(node any) {
		object := objectOf(t, node)
		visited = append(visited, object)
		for _, child := range listOf(t, object["children"]) {
			visit(child)
		}
	}
	visit(root)
	return visited
}

// nodeByID indexes the flat span list of a trace by span id.
func nodeByID(t *testing.T, trace map[string]any) map[string]map[string]any {
	t.Helper()
	indexed := map[string]map[string]any{}
	for _, node := range listOf(t, trace["spans"]) {
		object := objectOf(t, node)
		id, _ := object["span_id"].(string)
		indexed[id] = object
	}
	return indexed
}

func TestHealthReportsOK(t *testing.T) {
	c := newClient(t)
	status, raw, decoded := c.do(request{method: http.MethodGet, path: "/health"})
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if raw != `{"status":"ok"}` {
		t.Fatalf("unexpected body %s", raw)
	}
	if decoded["status"] != "ok" {
		t.Fatalf("unexpected payload %v", decoded)
	}
}

func TestSpanIngestionAcceptsChildrenBeforeParent(t *testing.T) {
	c := newClient(t)
	c.loadFixture()

	status, _, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if status != http.StatusOK {
		t.Fatalf("trace should be complete and usable, got status %d body %v", status, decoded)
	}
	if valid, _ := decoded["valid"].(bool); !valid {
		t.Fatalf("trace should be valid: %v", decoded)
	}
	if count := numberOf(t, decoded["declared_span_count"]); count != 6 {
		t.Fatalf("declared span count %d", count)
	}
	if count := numberOf(t, decoded["span_count"]); count != 6 {
		t.Fatalf("span count %d", count)
	}
	if services := numberOf(t, decoded["service_count"]); services != 4 {
		t.Fatalf("service count %d", services)
	}
	if errors := numberOf(t, decoded["error_spans"]); errors != 1 {
		t.Fatalf("error span count %d", errors)
	}
}

func TestSelfTimeIsDurationMinusChildDurations(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	nodes := nodeByID(t, c.get("/traces/t-1"))

	wanted := map[string]int64{
		"s-root": 400, // 1000 - 600 (the db subtree is the longest child)
		"s-auth": 200,
		"s-db":   350, // 600 - 250 (q2 is the longest child)
		"s-q1":   200,
		"s-q2":   250,
		"s-pub":  50,
	}
	for spanID, want := range wanted {
		if got := numberOf(t, nodes[spanID]["self_time_ns"]); got != want {
			t.Errorf("self_time_ns of %s = %d, want %d", spanID, got, want)
		}
	}
	// A leaf keeps its whole duration as self time, and every self time is >= 0.
	for id, node := range nodes {
		if self := numberOf(t, node["self_time_ns"]); self < 0 {
			t.Errorf("self_time_ns of %s is negative: %d", id, self)
		}
	}
	// self_time is the share of a span's duration that its single longest child
	// does not cover, so self_time plus the longest child's subtree duration is
	// the span duration exactly. This is what lets the critical path add up to
	// the root duration.
	for _, node := range nodes {
		longest := int64(0)
		for _, child := range listOf(t, node["children"]) {
			if subtree := numberOf(t, objectOf(t, child)["subtree_duration_ns"]); subtree > longest {
				longest = subtree
			}
		}
		if got, want := numberOf(t, node["self_time_ns"])+longest, numberOf(t, node["duration_ns"]); got != want {
			t.Errorf("span %v: self_time plus longest child subtree = %d, want its duration %d", node["span_id"], got, want)
		}
	}
}

func TestCriticalPathEqualsRootDuration(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	path := c.get("/traces/t-1/critical-path")

	ids := []string{}
	for _, step := range listOf(t, path["path"]) {
		object := objectOf(t, step)
		id, _ := object["span_id"].(string)
		ids = append(ids, id)
	}
	// The db branch carries the most exclusive time: 400 (root) + 350 (db) + 250 (q2).
	wanted := []string{"s-root", "s-db", "s-q2"}
	if !reflect.DeepEqual(ids, wanted) {
		t.Fatalf("critical path %v, want %v", ids, wanted)
	}
	if got, want := numberOf(t, path["length_ns"]), numberOf(t, path["trace_duration_ns"]); got != want {
		t.Fatalf("critical path length %d, want root duration %d", got, want)
	}
	if valid, _ := path["valid"].(bool); !valid {
		t.Fatalf("critical path of a valid trace should be valid")
	}
	if path["method"] == "" {
		t.Fatalf("the critical path must document its algorithm")
	}
	// Every hop of the path must contribute its own self_time.
	total := int64(0)
	for _, step := range listOf(t, path["path"]) {
		total += numberOf(t, objectOf(t, step)["self_time_ns"])
	}
	if total != numberOf(t, path["length_ns"]) {
		t.Fatalf("path self times sum to %d, length_ns is %v", total, path["length_ns"])
	}
}

// TestCriticalPathAlwaysEqualsRootDuration checks the headline invariant on
// several shapes: whatever the trace looks like, the longest self_time-weighted
// root-to-leaf path must add up to exactly the root duration.
func TestCriticalPathAlwaysEqualsRootDuration(t *testing.T) {
	cases := []struct {
		name      string
		rootSpan  string
		extraSpan []string
	}{
		{
			name:     "single leaf",
			rootSpan: span("t-leaf", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1)),
		},
		{
			name:     "chain",
			rootSpan: span("t-chain", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)),
			extraSpan: []string{
				span("t-chain", "a", "root", "svc", "op", "internal", traceFixtureTime, 90, "ok", nil),
				span("t-chain", "b", "a", "svc", "op", "internal", traceFixtureTime, 50, "ok", nil),
			},
		},
		{
			name:     "side branches",
			rootSpan: span("t-branch", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)),
			extraSpan: []string{
				span("t-branch", "a", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000010Z", 30, "ok", nil),
				span("t-branch", "b", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000050Z", 40, "ok", nil),
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			c := newClient(t)
			c.post(testCase.rootSpan)
			for _, document := range testCase.extraSpan {
				c.post(document)
			}
			traceID := traceIDOf(t, testCase.rootSpan)
			path := c.get("/traces/" + traceID + "/critical-path")
			length := numberOf(t, path["length_ns"])
			trace := c.get("/traces/" + traceID)
			root := objectOf(t, trace["root"])
			if want := numberOf(t, root["duration_ns"]); length != want {
				t.Fatalf("critical path length %d, want root duration %d", length, want)
			}
			if valid, _ := path["valid"].(bool); !valid {
				t.Fatalf("trace should be valid")
			}
		})
	}
}

// traceIDOf reads the trace id out of a span document.
func traceIDOf(t *testing.T, document string) string {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal([]byte(document), &decoded); err != nil {
		t.Fatalf("decode span document: %v", err)
	}
	id, _ := decoded["trace_id"].(string)
	return id
}

// TestCriticalPathTieBreaksAreDeterministic pins the ranking of the critical
// path on a branched trace and the determinism of equal branches.
func TestCriticalPathTieBreaksAreDeterministic(t *testing.T) {
	c := newClient(t)
	root := span("t-2", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(4))
	// Non-overlapping siblings, in start order: a 10–50, c 50–60, b 60–90. a is the
	// longest chain below the root, so it carries the critical path and the root
	// keeps the rest of its duration as exclusive time.
	left := span("t-2", "a", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000010Z", 40, "ok", nil)
	right := span("t-2", "b", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000060Z", 30, "ok", nil)
	tail := span("t-2", "c", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000050Z", 10, "ok", nil)
	c.post(root)
	c.post(tail)
	c.post(left)
	c.post(right)

	path := c.get("/traces/t-2/critical-path")
	ids := []string{}
	for _, step := range listOf(t, path["path"]) {
		id, _ := objectOf(t, step)["span_id"].(string)
		ids = append(ids, id)
	}
	if !reflect.DeepEqual(ids, []string{"root", "a"}) {
		t.Fatalf("the longest chain must carry the critical path, got %v", ids)
	}
	if got, want := numberOf(t, path["length_ns"]), int64(100); got != want {
		t.Fatalf("length %d != root duration %d", got, want)
	}
	if got, want := numberOf(t, nodeByID(t, c.get("/traces/t-2"))["root"]["self_time_ns"]), int64(60); got != want {
		t.Fatalf("root self time %d, want 100 - 40", got)
	}

	// Without a dominating child the tie is decided by span ids, so the same
	// input always yields the same path.
	c.post(span("t-3", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)))
	first := c.post(span("t-3", "b", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000060Z", 30, "ok", nil))
	c.post(span("t-3", "a", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000010Z", 40, "ok", nil))
	c.post(span("t-3", "z", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000070Z", 30, "ok", nil))
	if complete, _ := first["complete"].(bool); complete {
		t.Fatalf("the trace is not complete until its last span arrives")
	}
	tied := c.get("/traces/t-3/critical-path")
	tiedIDs := []string{}
	for _, step := range listOf(t, tied["path"]) {
		id, _ := objectOf(t, step)["span_id"].(string)
		tiedIDs = append(tiedIDs, id)
	}
	if !reflect.DeepEqual(tiedIDs, []string{"root", "a"}) {
		t.Fatalf("a tie must be broken by the smallest span id sequence, got %v", tiedIDs)
	}
	if got, want := numberOf(t, tied["length_ns"]), int64(100); got != want {
		t.Fatalf("length %d != root duration %d", got, want)
	}
	// t-2 has one dominating child (a, 40 ns), so the root keeps 100 - 40.
	nodes := nodeByID(t, c.get("/traces/t-2"))
	if got := numberOf(t, nodes["root"]["self_time_ns"]); got != 60 {
		t.Fatalf("root self time %d, want 100 - 40", got)
	}
	for _, id := range []string{"a", "b", "c"} {
		if got, want := numberOf(t, nodes[id]["self_time_ns"]), numberOf(t, nodes[id]["duration_ns"]); got != want {
			t.Fatalf("leaf %s self time %d != duration %d", id, got, want)
		}
	}
}

func TestErrorPropagationMarksOriginsAndAncestors(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	nodes := nodeByID(t, c.get("/traces/t-1"))

	origins := func(id string) []string {
		values := []string{}
		for _, origin := range listOf(t, nodes[id]["error_origins"]) {
			name, _ := origin.(string)
			values = append(values, name)
		}
		return values
	}
	if !reflect.DeepEqual(origins("s-q2"), []string{"s-q2"}) {
		t.Fatalf("error origin of s-q2: %v", origins("s-q2"))
	}
	if !reflect.DeepEqual(origins("s-db"), []string{"s-q2"}) {
		t.Fatalf("error origin of s-db: %v", origins("s-db"))
	}
	if !reflect.DeepEqual(origins("s-root"), []string{"s-q2"}) {
		t.Fatalf("error origin of s-root: %v", origins("s-root"))
	}
	if origin, _ := nodes["s-q2"]["error_origin"].(bool); !origin {
		t.Errorf("s-q2 must be flagged as an error origin")
	}
	// s-q2 failed where it ran, so it is neither propagated nor an ancestor.
	if propagated, _ := nodes["s-q2"]["propagated"].(bool); propagated {
		t.Errorf("s-q2 must not be flagged as propagated")
	}
	for _, propagated := range []string{"s-db", "s-root"} {
		if value, _ := nodes[propagated]["propagated"].(bool); !value {
			t.Errorf("%s must be flagged as propagated", propagated)
		}
		// The declared status is untouched: only the origin carries an error.
		if value, _ := nodes[propagated]["error"].(bool); value {
			t.Errorf("%s must keep its declared ok status", propagated)
		}
		if value, _ := nodes[propagated]["error_origin"].(bool); value {
			t.Errorf("%s must not be an error origin", propagated)
		}
	}
	for _, clean := range []string{"s-auth", "s-pub", "s-q1"} {
		if value, _ := nodes[clean]["error"].(bool); value {
			t.Errorf("%s must not be marked as an error", clean)
		}
		if value, _ := nodes[clean]["error_origin"].(bool); value {
			t.Errorf("%s must not be marked as an error origin", clean)
		}
		if value, _ := nodes[clean]["propagated"].(bool); value {
			t.Errorf("%s must not be marked as propagated", clean)
		}
	}
}

func TestServiceGraphAggregatesCallersAndCallees(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	graph := c.get("/services/graph")

	edges := map[string]map[string]any{}
	for _, edge := range listOf(t, graph["edges"]) {
		object := objectOf(t, edge)
		caller, _ := object["caller"].(string)
		callee, _ := object["callee"].(string)
		edges[caller+"->"+callee] = object
	}
	if len(edges) != 3 {
		t.Fatalf("expected 3 edges, got %v", edges)
	}
	if got := numberOf(t, edges["gateway->db"]["call_count"]); got != 1 {
		t.Errorf("gateway->db call count %d", got)
	}
	if got := numberOf(t, edges["gateway->db"]["error_count"]); got != 0 {
		t.Errorf("gateway->db error count %d", got)
	}
	if got := numberOf(t, edges["gateway->db"]["p95_duration_ns"]); got != 600 {
		t.Errorf("gateway->db p95 %d", got)
	}
	if got := numberOf(t, edges["gateway->db"]["p50_duration_ns"]); got != 600 {
		t.Errorf("gateway->db p50 %d", got)
	}
	if got := numberOf(t, edges["gateway->db"]["min_duration_ns"]); got != 600 {
		t.Errorf("gateway->db min %d", got)
	}
	if got := numberOf(t, edges["gateway->db"]["max_duration_ns"]); got != 600 {
		t.Errorf("gateway->db max %d", got)
	}
	if got := numberOf(t, edges["gateway->db"]["mean_duration_ns"]); got != 600 {
		t.Errorf("gateway->db mean %d", got)
	}
	// Calls inside one service are not cross-service traffic, so the two db
	// queries that hang off the db span produce no db->db edge.
	if _, found := edges["db->db"]; found {
		t.Errorf("intra-service calls must not become edges: %v", edges["db->db"])
	}

	services := map[string]map[string]any{}
	for _, service := range listOf(t, graph["services"]) {
		object := objectOf(t, service)
		name, _ := object["service"].(string)
		services[name] = object
	}
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"auth", "db", "gateway", "queue"}) {
		t.Fatalf("services %v", names)
	}
	if got := numberOf(t, services["db"]["span_count"]); got != 3 {
		t.Errorf("db span count %d", got)
	}
	if got := numberOf(t, services["db"]["self_time_ns"]); got != 800 {
		t.Errorf("db self time %d", got)
	}
	if got := numberOf(t, services["gateway"]["root_spans"]); got != 1 {
		t.Errorf("gateway root spans %d", got)
	}
	operations := listOf(t, services["db"]["operations"])
	if len(operations) != 2 {
		t.Fatalf("db operations %v", operations)
	}
	first := objectOf(t, operations[0])
	if first["operation"] != "query" {
		t.Fatalf("operations must be sorted by name, got %v", first["operation"])
	}

	stats := objectOf(t, graph["stats"])
	if got := numberOf(t, stats["trace_count"]); got != 1 {
		t.Errorf("trace count %d", got)
	}
	if got := numberOf(t, stats["span_count"]); got != 6 {
		t.Errorf("span count %d", got)
	}
	if got := numberOf(t, stats["error_spans"]); got != 1 {
		t.Errorf("error spans %d", got)
	}
	if got := numberOf(t, stats["duration_ns"]); got != 1000 {
		t.Errorf("total duration %d", got)
	}
	if traceID := graph["trace_id"]; traceID != nil {
		t.Errorf("unfiltered graph must not name a trace, got %v", traceID)
	}
}

func TestServiceGraphCanBeScopedToOneTrace(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	// The root is ingested last so that the trace is complete as soon as it lands.
	c.post(span("t-2", "r2-child", "r2", "inventory", "lookup", "client",
		"2024-06-01T00:00:02.000000100Z", 100, "ok", nil))
	root := c.post(span("t-2", "r2", "", "billing", "charge", "server", "2024-06-01T00:00:02Z", 500, "ok", count(2)))
	if complete, _ := root["complete"].(bool); !complete {
		t.Fatalf("standalone trace should be complete: %v", root)
	}

	graph := c.get("/services/graph?trace_id=t-2")
	if graph["trace_id"] != "t-2" {
		t.Fatalf("trace filter not echoed: %v", graph["trace_id"])
	}
	stats := objectOf(t, graph["stats"])
	if got := numberOf(t, stats["trace_count"]); got != 1 {
		t.Errorf("trace count %d", got)
	}
	if got := numberOf(t, stats["span_count"]); got != 2 {
		t.Errorf("span count %d", got)
	}
	// The scoped graph only contains the scoped trace's services.
	services := []string{}
	for _, item := range listOf(t, graph["services"]) {
		name, _ := objectOf(t, item)["service"].(string)
		services = append(services, name)
	}
	if !reflect.DeepEqual(services, []string{"billing", "inventory"}) {
		t.Errorf("scoped services %v", services)
	}
	edges := listOf(t, graph["edges"])
	if len(edges) != 1 {
		t.Fatalf("scoped edges %v", edges)
	}
	edge := objectOf(t, edges[0])
	if edge["caller"] != "billing" || edge["callee"] != "inventory" {
		t.Errorf("scoped edge %v", edge)
	}

	status, _, _ := c.do(request{method: http.MethodGet, path: "/services/graph?trace_id=missing"})
	if status != http.StatusNotFound {
		t.Fatalf("unknown trace filter should be 404, got %d", status)
	}
}

func TestUnknownFieldAndTimestampAreRejected(t *testing.T) {
	c := newClient(t)
	_, code, _ := c.reject(`{"trace_id":"t","span_id":"s","service":"svc","operation":"op",`+
		`"kind":"server","start_time":"`+traceFixtureTime+`","duration_ns":1,"status":"ok",`+
		`"span_count":1,"surprise":true}`, http.StatusBadRequest)
	if code != "validation_error" {
		t.Fatalf("unknown field code %s", code)
	}
	_, code, _ = c.reject(`{"trace_id":"t","span_id":"s","service":"svc","operation":"op",`+
		`"kind":"server","start_time":"01/06/2024","duration_ns":1,"status":"ok","span_count":1}`,
		http.StatusBadRequest)
	if code != "validation_error" {
		t.Fatalf("bad timestamp code %s", code)
	}
	status, _, _ := c.do(request{method: http.MethodPost, path: "/spans",
		body: `{"trace_id":"t","span_id":"s","service":"svc","operation":"op","kind":"server",` +
			`"start_time":"` + traceFixtureTime + `","duration_ns":1,"status":"ok","span_count":1}`})
	if status != http.StatusBadRequest {
		t.Fatalf("a mutating POST without Idempotency-Key should be 400, got %d", status)
	}
}

func TestChildOutsideParentIsRejectedWithReason(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))

	status, code, decoded := c.reject(span("t-1", "late", "root", "svc", "op", "internal",
		"2024-06-01T00:00:00.000000090Z", 50, "ok", nil), http.StatusConflict)
	if code != "conflict" {
		t.Fatalf("code %s", code)
	}
	cause := objectOf(t, decoded["error"])
	if message, _ := cause["message"].(string); !bytes.Contains([]byte(message), []byte("not contained in parent")) {
		t.Fatalf("unexpected message %q", message)
	}
	status, _, _ = c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if status != http.StatusConflict {
		t.Fatalf("an incomplete trace should be 409, got %d", status)
	}
}

func TestParentInAnotherTraceIsRejected(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))
	c.reject(span("t-2", "child", "root", "svc", "op", "internal", traceFixtureTime, 10, "ok", nil),
		http.StatusConflict)
	status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/t-2"})
	if status != http.StatusNotFound {
		t.Fatalf("the rejected span must not create a trace, got %d", status)
	}
}

func TestDuplicateSpanIsAConflict(t *testing.T) {
	c := newClient(t)
	document := span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1))
	c.post(document)
	_, code, _ := c.reject(document, http.StatusConflict)
	if code != "conflict" {
		t.Fatalf("duplicate code %s", code)
	}
}

func TestIdempotentIngestionReplaysTheOriginalResponse(t *testing.T) {
	c := newClient(t)
	document := span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(1))
	first := c.postWithKey(document, "shared")
	status, raw, decoded := c.do(request{method: http.MethodPost, path: "/spans", key: "shared", body: document})
	if status != http.StatusCreated {
		t.Fatalf("replay status %d", status)
	}
	firstRaw, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	replayRaw, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Equal(firstRaw, replayRaw) {
		t.Fatalf("replay body differs:\n%s\n%s", firstRaw, raw)
	}

	// A key may not name a second span, whatever the payload says.
	reuseStatus, reuseRaw, _ := c.do(request{method: http.MethodPost, path: "/spans", key: "shared",
		body: span("t-1", "other", "root", "svc", "op", "internal", traceFixtureTime, 1, "ok", nil)})
	if reuseStatus != http.StatusConflict {
		t.Fatalf("key reuse status %d body %s", reuseStatus, reuseRaw)
	}
}

func TestTraceViolationsAreReportedNotFabricated(t *testing.T) {
	c := newClient(t)
	// The root declares two spans but its child is never ingested.
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))

	status, raw, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	cause := objectOf(t, decoded["error"])
	if cause["code"] != "trace_invalid" {
		t.Fatalf("code %v", cause["code"])
	}
	violations := listOf(t, cause["violations"])
	if len(violations) == 0 {
		t.Fatalf("violations must be reported: %s", raw)
	}
	first := objectOf(t, violations[0])
	if first["code"] != "trace_incomplete" && first["code"] != "orphan_span" {
		t.Fatalf("unexpected violation %v", first)
	}
	// Critical path still answers, but refuses to claim validity.
	path := c.get("/traces/t-1/critical-path")
	if valid, _ := path["valid"].(bool); valid {
		t.Fatalf("an incomplete trace must not be reported as valid")
	}
	if length := numberOf(t, path["length_ns"]); length != 100 {
		t.Fatalf("root-only critical path length %d", length)
	}
}

func TestOverDeclaredSpanCountIsDetected(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))
	c.post(span("t-1", "one", "root", "svc", "op", "internal", traceFixtureTime, 10, "ok", nil))
	c.post(span("t-1", "two", "root", "svc", "op", "internal", "2024-06-01T00:00:00.000000020Z", 10, "ok", nil))
	status, raw, _ := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	if !bytes.Contains([]byte(raw), []byte("span_count_exceeded")) {
		t.Fatalf("expected a span_count_exceeded violation, got %s", raw)
	}
}

func TestSiblingOverlapIsReportedAsViolation(t *testing.T) {
	c := newClient(t)
	// a was ingested while b was still missing, so ingestion could not see that
	// the two children overlap: it can only compare a span with the parent it
	// already knows.
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)))
	c.post(span("t-1", "a", "root", "svc", "op", "internal", traceFixtureTime, 50, "ok", nil))
	c.post(span("t-1", "d", "root", "svc", "op", "internal",
		"2024-06-01T00:00:00.000000030Z", 20, "ok", nil))

	status, raw, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	cause := objectOf(t, decoded["error"])
	byCode := map[string]map[string]any{}
	for _, violation := range listOf(t, cause["violations"]) {
		object := objectOf(t, violation)
		code, _ := object["code"].(string)
		byCode[code] = object
	}
	overlap, found := byCode["sibling_overlap"]
	if !found {
		t.Fatalf("overlapping siblings must be reported, got %s", raw)
	}
	if overlap["span_id"] != "a" {
		t.Fatalf("the overlap must name the earlier sibling: %v", overlap)
	}
	if message, _ := overlap["message"].(string); !bytes.Contains([]byte(message), []byte("overlap")) &&
		!bytes.Contains([]byte(message), []byte("before sibling")) {
		t.Fatalf("unexpected message %q", message)
	}
	// The rejected trace makes the critical path refuse to claim validity.
	path := c.get("/traces/t-1/critical-path")
	if valid, _ := path["valid"].(bool); valid {
		t.Fatalf("a trace with overlapping siblings must not be valid")
	}
}

func TestNonOverlappingSiblingsAreAccepted(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)))
	c.post(span("t-1", "a", "root", "svc", "op", "internal", traceFixtureTime, 30, "ok", nil))
	c.post(span("t-1", "b", "root", "svc", "op", "internal",
		"2024-06-01T00:00:00.000000030Z", 70, "ok", nil))

	trace := c.get("/traces/t-1")
	if valid, _ := trace["valid"].(bool); !valid {
		t.Fatalf("contiguous siblings must be accepted: %v", trace["violations"])
	}
	nodes := nodeByID(t, trace)
	// The longest child subtree is b (70), so the root keeps 30 of its duration.
	if got := numberOf(t, nodes["root"]["self_time_ns"]); got != 30 {
		t.Fatalf("root self time %d, want 100 - 70", got)
	}
	// Both branches weigh 30 and 70; the last branch wins on self time.
	path := c.get("/traces/t-1/critical-path")
	ids := []string{}
	for _, step := range listOf(t, path["path"]) {
		id, _ := objectOf(t, step)["span_id"].(string)
		ids = append(ids, id)
	}
	if !reflect.DeepEqual(ids, []string{"root", "b"}) {
		t.Fatalf("critical path %v", ids)
	}
	if got, want := numberOf(t, path["length_ns"]), int64(100); got != want {
		t.Fatalf("length %d want %d", got, want)
	}
}

func TestChildrenCoveringTheWholeParentLeaveNoSelfTime(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)))
	c.post(span("t-1", "a", "root", "svc", "op", "internal", traceFixtureTime, 40, "ok", nil))
	// 60..100 fits inside 0..100 after a and reaches the parent's end, so the
	// root has no exclusive time of its own left.
	c.post(span("t-1", "b", "root", "svc", "op", "internal",
		"2024-06-01T00:00:00.000000060Z", 40, "ok", nil))

	nodes := nodeByID(t, c.get("/traces/t-1"))
	// Both children last 40, so the root keeps 60 of its duration.
	if got := numberOf(t, nodes["root"]["self_time_ns"]); got != 60 {
		t.Fatalf("root self time %d, want 100 - 40", got)
	}
	if got := numberOf(t, nodes["b"]["self_time_ns"]); got != 40 {
		t.Fatalf("leaf self time %d, want 40", got)
	}
	path := c.get("/traces/t-1/critical-path")
	ids := []string{}
	for _, step := range listOf(t, path["path"]) {
		id, _ := objectOf(t, step)["span_id"].(string)
		ids = append(ids, id)
	}
	// a and b weigh 40 each; the tie is broken by span id.
	if !reflect.DeepEqual(ids, []string{"root", "a"}) {
		t.Fatalf("critical path %v", ids)
	}
	if got, want := numberOf(t, path["length_ns"]), int64(100); got != want {
		t.Fatalf("length %d want %d", got, want)
	}
}

func TestTraceListIsDeterministicAndFilterable(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	c.post(span("t-2", "r2", "", "billing", "charge", "server", "2024-06-01T00:00:02Z", 500, "ok", count(1)))

	all := c.get("/traces")
	traces := listOf(t, all["traces"])
	if len(traces) != 2 {
		t.Fatalf("expected 2 traces, got %v", traces)
	}
	if objectOf(t, traces[0])["trace_id"] != "t-2" {
		t.Fatalf("traces must be ordered by root start time descending: %v", traces)
	}
	filtered := c.get("/traces?service=queue")
	if got := len(listOf(t, filtered["traces"])); got != 1 {
		t.Fatalf("service filter returned %d traces", got)
	}
	limited := c.get("/traces?limit=1")
	if got := len(listOf(t, limited["traces"])); got != 1 {
		t.Fatalf("limit returned %d traces", got)
	}
	status, _, _ := c.do(request{method: http.MethodGet, path: "/traces?limit=0"})
	if status != http.StatusBadRequest {
		t.Fatalf("limit=0 should be 400, got %d", status)
	}
	status, _, _ = c.do(request{method: http.MethodGet, path: "/traces?nope=1"})
	if status != http.StatusBadRequest {
		t.Fatalf("unknown query parameter should be 400, got %d", status)
	}
}

// traceIDsOf extracts the trace ids of a GET /traces response in order.
func traceIDsOf(t *testing.T, response map[string]any) []string {
	t.Helper()
	ids := []string{}
	for _, item := range listOf(t, response["traces"]) {
		id, _ := objectOf(t, item)["trace_id"].(string)
		ids = append(ids, id)
	}
	return ids
}

func TestTraceListQueryFilters(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	c.post(span("t-2", "r2", "", "billing", "charge", "server", "2024-06-01T00:00:02Z", 500, "ok", count(1)))
	// t-3 declares two spans but only its root arrives, so it stays incomplete.
	c.post(span("t-3", "r3", "", "search", "lookup", "server", "2024-06-01T00:00:01Z", 800, "ok", count(2)))

	assert := func(path string, wanted ...string) {
		t.Helper()
		got := traceIDsOf(t, c.get(path))
		if !reflect.DeepEqual(got, wanted) {
			t.Fatalf("GET %s: got %v, want %v", path, got, wanted)
		}
	}

	// q matches trace_id, service and operation, case-insensitively.
	assert("/traces?q=T-1", "t-1")
	assert("/traces?q=CHECKOUT", "t-1")
	assert("/traces?q=Queue", "t-1")
	assert("/traces?q=billing", "t-2")

	// operation is an exact match on any span of the trace.
	assert("/traces?operation=query", "t-1")
	assert("/traces?operation=charge", "t-2")

	// status keeps traces with an error span, or only fully ok traces.
	assert("/traces?status=error", "t-1")
	assert("/traces?status=ok", "t-2", "t-3")

	// start_from is inclusive, start_to exclusive, both on the root start.
	assert("/traces?start_from=2024-06-01T00:00:01Z", "t-2", "t-3")
	assert("/traces?start_to=2024-06-01T00:00:01Z", "t-1")
	assert("/traces?start_from=2024-06-01T00:00:00Z&start_to=2024-06-01T00:00:02Z", "t-3", "t-1")

	// duration bounds include the root duration.
	assert("/traces?min_duration_ns=800", "t-3", "t-1")
	assert("/traces?max_duration_ns=500", "t-2")
	assert("/traces?min_duration_ns=500&max_duration_ns=1000", "t-2", "t-3", "t-1")

	// complete and valid filter the current reassembly result.
	assert("/traces?complete=false", "t-3")
	assert("/traces?valid=false", "t-3")
	assert("/traces?complete=true&valid=true", "t-2", "t-1")

	// filters combine with logical AND, also with the existing service filter.
	assert("/traces?service=db&status=error", "t-1")
	assert("/traces?q=query&complete=true", "t-1")
	assert("/traces?operation=lookup&status=ok&max_duration_ns=800", "t-3")

	// no hit is HTTP 200 with an empty array.
	empty := c.get("/traces?q=no-such-thing")
	if got := listOf(t, empty["traces"]); len(got) != 0 {
		t.Fatalf("expected no traces, got %v", got)
	}
}

func TestTraceListRejectsMalformedFilters(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	bad := []string{
		"/traces?start_from=not-a-time",
		"/traces?start_to=2024-13-01T00:00:00Z",
		"/traces?min_duration_ns=-5",
		"/traces?min_duration_ns=1.5",
		"/traces?max_duration_ns=abc",
		"/traces?min_duration_ns=",
		"/traces?min_duration_ns=10&max_duration_ns=5",
		"/traces?complete=yes",
		"/traces?valid=1",
		"/traces?status=broken",
		"/traces?status=",
		"/traces?operation=",
		"/traces?unknown=1",
	}
	for _, path := range bad {
		status, _, decoded := c.do(request{method: http.MethodGet, path: path})
		if status != http.StatusBadRequest {
			t.Fatalf("GET %s: status %d, want 400", path, status)
		}
		if code := objectOf(t, decoded["error"])["code"]; code != "validation_error" {
			t.Fatalf("GET %s: error code %v, want validation_error", path, code)
		}
	}
}

func TestTraceOutputIsByteStableAcrossReads(t *testing.T) {
	c := newClient(t)
	c.loadFixture()
	_, first, _ := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	_, second, _ := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if first != second {
		t.Fatalf("trace rendering is not deterministic:\n%s\n%s", first, second)
	}
	_, firstPath, _ := c.do(request{method: http.MethodGet, path: "/traces/t-1/critical-path"})
	_, secondPath, _ := c.do(request{method: http.MethodGet, path: "/traces/t-1/critical-path"})
	if firstPath != secondPath {
		t.Fatalf("critical path rendering is not deterministic")
	}
	_, firstGraph, _ := c.do(request{method: http.MethodGet, path: "/services/graph"})
	_, secondGraph, _ := c.do(request{method: http.MethodGet, path: "/services/graph"})
	if firstGraph != secondGraph {
		t.Fatalf("service graph rendering is not deterministic")
	}
}

func TestStateSurvivesRestart(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "tracepath.db")
	first := newClientOn(t, database)
	first.loadFixture()
	_, before, _ := first.do(request{method: http.MethodGet, path: "/traces/t-1"})
	first.server.Close()

	second := newClientOn(t, database)
	_, after, _ := second.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if before != after {
		t.Fatalf("reopened store returned a different trace:\n%s\n%s", before, after)
	}
	// A duplicate span is still refused after the restart, so identity survives.
	second.reject(span("t-1", "s-root", "", "gateway", "GET /checkout", "server", traceFixtureTime, 1000, "ok", count(6)),
		http.StatusConflict)
}

func TestUnknownRouteAndMethod(t *testing.T) {
	c := newClient(t)
	for _, r := range []request{
		{method: http.MethodGet, path: "/nope"},
		{method: http.MethodPost, path: "/traces"},
		{method: http.MethodGet, path: "/traces/t-1/unknown"},
		{method: http.MethodGet, path: "/services"},
	} {
		status, _, _ := c.do(r)
		if status != http.StatusNotFound {
			t.Errorf("%s %s: status %d", r.method, r.path, status)
		}
	}
	status, _, _ := c.do(request{method: http.MethodGet, path: "/traces/missing"})
	if status != http.StatusNotFound {
		t.Errorf("missing trace status %d", status)
	}
	status, _, _ = c.do(request{method: http.MethodGet, path: "/traces/missing/critical-path"})
	if status != http.StatusNotFound {
		t.Errorf("missing trace critical path status %d", status)
	}
}

func TestSpanCountRulesAtIngestion(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))
	c.post(span("t-1", "a", "root", "svc", "op", "internal", traceFixtureTime, 10, "ok", nil))
	// span_count is only allowed on a root span.
	c.reject(span("t-1", "b", "root", "svc", "op", "internal",
		"2024-06-01T00:00:00.000000020Z", 10, "ok", count(2)), http.StatusBadRequest)
	// A root span without span_count cannot be sized.
	c.reject(span("t-1", "root2", "", "svc", "op", "server", traceFixtureTime, 100, "ok", nil),
		http.StatusBadRequest)
	c.reject(span("t-1", "root3", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(0)),
		http.StatusBadRequest)
	// Root span ids are unique like every other span id.
	c.reject(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)),
		http.StatusConflict)
}

// TestTimeContainmentViolationIsReportedNotRejected reaches a stored containment
// violation through a grandchild that was ingested while its parent was still
// missing: the check can only run against spans that already exist.
func TestTimeContainmentViolationIsReportedNotRejected(t *testing.T) {
	c := newClient(t)
	// 30..90 fits inside 0..100, so ingestion accepts it while its parent is
	// still missing; the check can only run against spans that already exist.
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(3)))
	c.post(span("t-1", "gchild", "child", "svc", "op", "internal",
		"2024-06-01T00:00:00.000000030Z", 60, "ok", nil))
	c.post(span("t-1", "child", "root", "svc", "op", "internal", traceFixtureTime, 60, "ok", nil))

	// The grandchild now overhangs its parent: 30..90 is not inside 0..60.
	status, raw, decoded := c.do(request{method: http.MethodGet, path: "/traces/t-1"})
	if status != http.StatusConflict {
		t.Fatalf("status %d body %s", status, raw)
	}
	cause := objectOf(t, decoded["error"])
	violations := listOf(t, cause["violations"])
	if len(violations) != 1 {
		t.Fatalf("the overhang is the only violation, got %s", raw)
	}
	containment := objectOf(t, violations[0])
	if containment["code"] != "time_not_contained" {
		t.Fatalf("violation %v", containment)
	}
	if containment["span_id"] != "gchild" {
		t.Fatalf("violation must name the offending span: %v", containment)
	}
	message, _ := containment["message"].(string)
	if !bytes.Contains([]byte(message), []byte("not contained in parent")) {
		t.Fatalf("unexpected message %q", message)
	}
	// A stored span that breaks containment makes the trace unusable, so the
	// critical path refuses to claim validity for it.
	path := c.get("/traces/t-1/critical-path")
	if valid, _ := path["valid"].(bool); valid {
		t.Fatalf("a trace with a containment violation must not be valid")
	}
}

func TestContentTypeMustBeJSON(t *testing.T) {
	c := newClient(t)
	status, _, _ := c.do(request{method: http.MethodPost, path: "/spans", key: "k",
		body: `{}`, contentType: "text/plain"})
	if status != http.StatusBadRequest {
		t.Fatalf("status %d", status)
	}
	status, _, _ = c.do(request{method: http.MethodPost, path: "/spans", key: "k", body: `{}`, omitType: true})
	if status != http.StatusBadRequest {
		t.Fatalf("missing content type status %d", status)
	}
}

func TestCriticalPathOfPartialTraceExposesViolations(t *testing.T) {
	c := newClient(t)
	c.post(span("t-1", "root", "", "svc", "op", "server", traceFixtureTime, 100, "ok", count(2)))
	status, raw, _ := c.do(request{method: http.MethodGet, path: "/traces/t-1/critical-path"})
	if status != http.StatusOK {
		t.Fatalf("status %d body %s", status, raw)
	}
	if !bytes.Contains([]byte(raw), []byte(`"violations":[{"code":"trace_incomplete"`)) {
		t.Fatalf("critical path must always expose violations: %s", raw)
	}
}
