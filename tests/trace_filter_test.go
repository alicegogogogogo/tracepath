package tests

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

// loadFilterFixture builds five traces that differ on every filterable axis:
//
//	t-1  the six-span fixture: complete, valid, one error span, root at 00:00:00,
//	     duration 1000, services gateway/auth/db/queue
//	t-2  complete, valid, all ok, root at 00:00:02, duration 500
//	t-3  incomplete root (declares 2 spans, 1 arrived), root at 00:00:03, dur 100
//	t-4  complete but invalid (overlapping siblings), root at 00:00:04, dur 100
//	t-5  complete, valid, all ok, non-ASCII service "Café", root at 00:00:05
func loadFilterFixture(t *testing.T) *client {
	t.Helper()
	c := newClient(t)
	c.loadFixture()
	c.post(span("t-2", "r2", "", "billing", "charge", "server",
		"2024-06-01T00:00:02Z", 500, "ok", count(1)))
	c.post(span("t-3", "r3", "", "svc-c", "plan", "server",
		"2024-06-01T00:00:03Z", 100, "ok", count(2)))
	// t-4 reaches its declared span count, so complete is true while the
	// overlapping siblings keep valid false.
	c.post(span("t-4", "r4", "", "svc-d", "serve", "server",
		"2024-06-01T00:00:04Z", 100, "ok", count(3)))
	c.post(span("t-4", "a", "r4", "svc-d", "serve", "internal",
		"2024-06-01T00:00:04Z", 50, "ok", nil))
	c.post(span("t-4", "d", "r4", "svc-d", "serve", "internal",
		"2024-06-01T00:00:04.000000030Z", 20, "ok", nil))
	c.post(span("t-5", "r5", "", "Café", "sip", "server",
		"2024-06-01T00:00:05Z", 50, "ok", count(1)))
	return c
}

// listTraceIDs GETs a /traces query and returns the trace ids in response
// order, which is root start time descending then trace id.
func listTraceIDs(t *testing.T, c *client, query string) []string {
	t.Helper()
	decoded := c.get("/traces" + query)
	ids := []string{}
	for _, item := range listOf(t, decoded["traces"]) {
		id, _ := objectOf(t, item)["trace_id"].(string)
		ids = append(ids, id)
	}
	return ids
}

func enc(value string) string { return url.QueryEscape(value) }

func TestTraceFiltersMatchSemantics(t *testing.T) {
	c := loadFilterFixture(t)
	// Newest root first; every id appears once.
	all := []string{"t-5", "t-4", "t-3", "t-2", "t-1"}
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"no parameters stays unfiltered", "", all},
		{"empty q stays unfiltered", "?q=", all},

		{"q matches trace id", "?q=t-1", []string{"t-1"}},
		{"q is case insensitive on trace id", "?q=T-1", []string{"t-1"}},
		{"q matches a service", "?q=gateway", []string{"t-1"}},
		{"q folds service case", "?q=GATEWAY", []string{"t-1"}},
		{"q matches an operation", "?q=checkout", []string{"t-1"}},
		{"q folds operation case", "?q=CHECKOUT", []string{"t-1"}},
		{"q folds non-ASCII case", "?q=" + enc("CAFÉ"), []string{"t-5"}},
		{"q without a hit is an empty list", "?q=does-not-exist", []string{}},

		{"operation exact match", "?operation=query", []string{"t-1"}},
		{"operation matches another trace", "?operation=charge", []string{"t-2"}},
		{"operation is case sensitive", "?operation=Query", []string{}},

		{"service keeps traces containing it", "?service=db", []string{"t-1"}},
		{"service on the unicode trace", "?service=" + enc("Café"), []string{"t-5"}},

		{"status error keeps traces with an error span", "?status=error", []string{"t-1"}},
		{"status ok keeps traces without error spans", "?status=ok", []string{"t-5", "t-4", "t-3", "t-2"}},

		{"start_from is inclusive", "?start_from=2024-06-01T00:00:02Z", []string{"t-5", "t-4", "t-3", "t-2"}},
		{"start_to is exclusive", "?start_to=2024-06-01T00:00:02Z", []string{"t-1"}},
		{"start window combines inclusivity and exclusivity",
			"?start_from=2024-06-01T00:00:02Z&start_to=2024-06-01T00:00:04Z",
			[]string{"t-3", "t-2"}},
		{"start_to keeps nanosecond precision", "?start_to=2024-06-01T00:00:00.000000001Z",
			[]string{"t-1"}},
		{"start_from keeps nanosecond precision", "?start_from=2024-06-01T00:00:00.000000001Z",
			[]string{"t-5", "t-4", "t-3", "t-2"}},

		{"min duration is inclusive", "?min_duration_ns=1000", []string{"t-1"}},
		{"max duration is inclusive", "?max_duration_ns=100", []string{"t-5", "t-4", "t-3"}},
		{"duration window", "?min_duration_ns=500&max_duration_ns=1000", []string{"t-2", "t-1"}},
		{"duration window without a hit", "?min_duration_ns=1001", []string{}},

		{"complete true", "?complete=true", []string{"t-5", "t-4", "t-2", "t-1"}},
		{"complete false", "?complete=false", []string{"t-3"}},
		{"valid true", "?valid=true", []string{"t-5", "t-2", "t-1"}},
		{"valid false", "?valid=false", []string{"t-4", "t-3"}},
		{"complete true and valid false isolates t-4", "?complete=true&valid=false", []string{"t-4"}},

		{"filters combine with logical AND", "?status=error&complete=true&valid=true", []string{"t-1"}},
		{"AND over status completeness and validity", "?status=ok&complete=true&valid=true",
			[]string{"t-5", "t-2"}},
		{"AND across q status and complete", "?q=t-&status=ok&complete=true",
			[]string{"t-5", "t-4", "t-2"}},
		{"AND across time and duration", "?start_from=2024-06-01T00:00:02Z&min_duration_ns=500",
			[]string{"t-2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := listTraceIDs(t, c, tc.query)
			if len(got) == 0 {
				got = []string{}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("GET /traces%s -> %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestTraceFiltersRejectBadParameters(t *testing.T) {
	c := loadFilterFixture(t)
	bad := []string{
		"?nope=1",
		"?start_from=not-a-time",
		"?start_to=2024-06-01",
		"?min_duration_ns=-1",
		"?min_duration_ns=abc",
		"?min_duration_ns=1.5",
		"?min_duration_ns=1e3",
		"?max_duration_ns=",
		"?min_duration_ns=10&max_duration_ns=9",
		"?complete=yes",
		"?complete=1",
		"?valid=",
		"?status=failed",
		"?status=",
		"?operation=",
		"?operation=" + enc("  "),
	}
	for _, query := range bad {
		t.Run(query, func(t *testing.T) {
			status, _, decoded := c.do(request{method: http.MethodGet, path: "/traces" + query})
			if status != http.StatusBadRequest {
				t.Fatalf("GET /traces%s: status %d, want 400", query, status)
			}
			cause := objectOf(t, decoded["error"])
			if code, _ := cause["code"].(string); code != "validation_error" {
				t.Fatalf("GET /traces%s: code %q, want validation_error", query, code)
			}
		})
	}
}

func TestTraceFiltersOnARootlessTrace(t *testing.T) {
	c := newClient(t)
	// One orphan span whose parent never arrives: the trace exists but its span
	// cannot be attached to any root, so the reassembly carries neither a root
	// start time, a root duration nor the orphan in its span tree.
	c.post(span("t-o", "child", "ghost", "svc-x", "op-x", "client",
		traceFixtureTime, 10, "ok", nil))

	if got := listTraceIDs(t, c, ""); !reflect.DeepEqual(got, []string{"t-o"}) {
		t.Fatalf("the rootless trace still appears unfiltered: %v", got)
	}
	// service, q and operation all inspect the reassembled span tree, exactly
	// like the legacy service filter, so the unattached orphan matches none of
	// them.
	for _, query := range []string{"?service=svc-x", "?q=svc-x", "?operation=op-x"} {
		if got := listTraceIDs(t, c, query); len(got) != 0 {
			t.Fatalf("%s must not match the unattached orphan, got %v", query, got)
		}
	}
	// Reassembly reports no error span for the detached span, so status=ok keeps
	// it while status=error drops it.
	if got := listTraceIDs(t, c, "?status=ok"); !reflect.DeepEqual(got, []string{"t-o"}) {
		t.Fatalf("the all-ok orphan matches status=ok: %v", got)
	}
	if got := listTraceIDs(t, c, "?status=error"); len(got) != 0 {
		t.Fatalf("the detached orphan must not match status=error: %v", got)
	}
	if got := listTraceIDs(t, c, "?complete=false"); !reflect.DeepEqual(got, []string{"t-o"}) {
		t.Fatalf("the orphan is incomplete: %v", got)
	}
	if got := listTraceIDs(t, c, "?valid=false"); !reflect.DeepEqual(got, []string{"t-o"}) {
		t.Fatalf("the orphan is invalid: %v", got)
	}
	if got := listTraceIDs(t, c, "?complete=true"); len(got) != 0 {
		t.Fatalf("complete=true must drop the orphan, got %v", got)
	}
	for _, query := range []string{
		"?start_from=2024-01-01T00:00:00Z",
		"?start_to=2026-01-01T00:00:00Z",
		"?min_duration_ns=0",
		"?max_duration_ns=999999999",
	} {
		if got := listTraceIDs(t, c, query); len(got) != 0 {
			t.Fatalf("%s must drop a trace without a root, got %v", query, got)
		}
	}
}

func TestTraceFiltersKeepOrderingAndLimit(t *testing.T) {
	c := loadFilterFixture(t)
	got := listTraceIDs(t, c, "?complete=true&limit=2")
	if want := []string{"t-5", "t-4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered limit %v, want %v", got, want)
	}
	// The summary fields survive filtering unchanged.
	decoded := c.get("/traces?q=t-1")
	items := listOf(t, decoded["traces"])
	if len(items) != 1 {
		t.Fatalf("expected one trace, got %v", items)
	}
	summary := objectOf(t, items[0])
	if summary["root_service"] != "gateway" ||
		numberOf(t, summary["duration_ns"]) != 1000 ||
		numberOf(t, summary["span_count"]) != 6 ||
		numberOf(t, summary["error_spans"]) != 1 ||
		summary["complete"] != true || summary["valid"] != true {
		t.Fatalf("filtered summary lost fields: %v", summary)
	}
}
