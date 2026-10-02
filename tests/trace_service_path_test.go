package tests

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// loadServicePathFixture builds traces that exercise the service_path chain
// matcher on every axis:
//
//	t-chain   gateway -> db -> cache (the cache leaf failed)
//	t-branch  gateway -> {auth, db -> cache}
//	t-repeat  svc-a -> svc-b -> svc-a (a non-adjacent service repeat)
//	t-multi   gateway -> db -> cache twice (two matching chains in one trace)
//	t-flat    a single svc-x root, matching no chain
//
// Roots start one second apart from 00:00:00 to 00:00:04, so the list order is
// t-flat, t-multi, t-repeat, t-branch, t-chain.
func loadServicePathFixture(t *testing.T) *client {
	t.Helper()
	c := newClient(t)
	c.post(span("t-chain", "root", "", "gateway", "handle", "server",
		"2024-06-01T00:00:00Z", 1000, "ok", count(3)))
	c.post(span("t-chain", "mid", "root", "db", "select", "client",
		"2024-06-01T00:00:00.000000100Z", 500, "ok", nil))
	c.post(span("t-chain", "leaf", "mid", "cache", "get", "client",
		"2024-06-01T00:00:00.000000200Z", 100, "error", nil))

	c.post(span("t-branch", "root", "", "gateway", "handle", "server",
		"2024-06-01T00:00:01Z", 1000, "ok", count(4)))
	c.post(span("t-branch", "a", "root", "auth", "verify", "client",
		"2024-06-01T00:00:01.000000100Z", 100, "ok", nil))
	c.post(span("t-branch", "d", "root", "db", "select", "client",
		"2024-06-01T00:00:01.000000300Z", 400, "ok", nil))
	c.post(span("t-branch", "c", "d", "cache", "get", "client",
		"2024-06-01T00:00:01.000000400Z", 100, "ok", nil))

	c.post(span("t-repeat", "root", "", "svc-a", "handle", "server",
		"2024-06-01T00:00:02Z", 1000, "ok", count(3)))
	c.post(span("t-repeat", "mid", "root", "svc-b", "work", "client",
		"2024-06-01T00:00:02.000000100Z", 500, "ok", nil))
	c.post(span("t-repeat", "leaf", "mid", "svc-a", "callback", "client",
		"2024-06-01T00:00:02.000000200Z", 100, "ok", nil))

	c.post(span("t-multi", "root", "", "gateway", "handle", "server",
		"2024-06-01T00:00:03Z", 1000, "ok", count(5)))
	c.post(span("t-multi", "d1", "root", "db", "select", "client",
		"2024-06-01T00:00:03.000000100Z", 300, "ok", nil))
	c.post(span("t-multi", "c1", "d1", "cache", "get", "client",
		"2024-06-01T00:00:03.000000200Z", 100, "ok", nil))
	c.post(span("t-multi", "d2", "root", "db", "select", "client",
		"2024-06-01T00:00:03.000000500Z", 300, "ok", nil))
	c.post(span("t-multi", "c2", "d2", "cache", "get", "client",
		"2024-06-01T00:00:03.000000600Z", 100, "ok", nil))

	c.post(span("t-flat", "root", "", "svc-x", "handle", "server",
		"2024-06-01T00:00:04Z", 100, "ok", count(1)))
	return c
}

// servicePathQuery renders one ordered service_path chain as a query string.
func servicePathQuery(names ...string) string {
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, "service_path="+enc(name))
	}
	return "?" + strings.Join(parts, "&")
}

func TestServicePathMatchesOrderedChains(t *testing.T) {
	c := loadServicePathFixture(t)
	chained := []string{"t-multi", "t-branch", "t-chain"}
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"full chain", servicePathQuery("gateway", "db", "cache"), chained},
		{"chain may start below the root", servicePathQuery("db", "cache"), chained},
		{"two hop prefix", servicePathQuery("gateway", "db"), chained},
		{"grandchild is not a direct hop", servicePathQuery("gateway", "cache"), []string{}},
		{"reversed chain does not match", servicePathQuery("cache", "db"), []string{}},
		{"names match exactly", servicePathQuery("Gateway", "db"), []string{}},
		{"unknown service matches nothing", servicePathQuery("gateway", "nope"), []string{}},
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

func TestServicePathMatchesAcrossBranches(t *testing.T) {
	c := loadServicePathFixture(t)
	// Each branch of t-branch is its own chain, and a chain never hops between
	// siblings.
	cases := []struct {
		query string
		want  []string
	}{
		{servicePathQuery("gateway", "auth"), []string{"t-branch"}},
		{servicePathQuery("gateway", "db", "cache"), []string{"t-multi", "t-branch", "t-chain"}},
		{servicePathQuery("auth", "cache"), []string{}},
		{servicePathQuery("auth", "db"), []string{}},
	}
	for _, tc := range cases {
		got := listTraceIDs(t, c, tc.query)
		if len(got) == 0 {
			got = []string{}
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("GET /traces%s -> %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestServicePathAllowsNonAdjacentRepeats(t *testing.T) {
	c := loadServicePathFixture(t)
	if got := listTraceIDs(t, c, servicePathQuery("svc-a", "svc-b", "svc-a")); !reflect.DeepEqual(got, []string{"t-repeat"}) {
		t.Fatalf("a non-adjacent repeat must match t-repeat, got %v", got)
	}
	if got := listTraceIDs(t, c, servicePathQuery("svc-b", "svc-a")); !reflect.DeepEqual(got, []string{"t-repeat"}) {
		t.Fatalf("the inner hop must match t-repeat, got %v", got)
	}
	// A four-hop chain is longer than the trace, so it cannot match.
	if got := listTraceIDs(t, c, servicePathQuery("svc-a", "svc-b", "svc-a", "svc-b")); len(got) != 0 {
		t.Fatalf("a chain longer than the trace must not match, got %v", got)
	}
}

func TestServicePathReturnsEachTraceOnce(t *testing.T) {
	c := loadServicePathFixture(t)
	// t-multi contains the chain gateway -> db -> cache twice, once per branch.
	got := listTraceIDs(t, c, servicePathQuery("gateway", "db", "cache"))
	count := 0
	for _, id := range got {
		if id == "t-multi" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("t-multi matched %d chains but must be listed once, got %v", 2, got)
	}
}

func TestServicePathWithoutMatchIsAnEmptyList(t *testing.T) {
	c := loadServicePathFixture(t)
	decoded := c.get("/traces" + servicePathQuery("gateway", "nope"))
	traces := listOf(t, decoded["traces"])
	if len(traces) != 0 {
		t.Fatalf("a chain without a match must return an empty list, got %v", traces)
	}
}

func TestServicePathRejectsInvalidChains(t *testing.T) {
	c := loadServicePathFixture(t)
	longName := strings.Repeat("x", 129)
	many := make([]string, 0, 33)
	for i := 0; i < 33; i++ {
		if i%2 == 0 {
			many = append(many, "a")
		} else {
			many = append(many, "b")
		}
	}
	bad := []string{
		servicePathQuery("only-one"),
		servicePathQuery(""),
		servicePathQuery("a", ""),
		servicePathQuery("a", "  "),
		servicePathQuery(longName, "b"),
		servicePathQuery("a", "a"),
		servicePathQuery("a", "b", "b"),
		servicePathQuery(many...),
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
	// Thirty-two alternating names is the longest legal chain.
	status, _, _ := c.do(request{method: http.MethodGet, path: "/traces" + servicePathQuery(many[:32]...)})
	if status != http.StatusOK {
		t.Fatalf("a 32-hop chain must be accepted, got status %d", status)
	}
}

func TestServicePathCombinesWithOtherFilters(t *testing.T) {
	c := loadServicePathFixture(t)
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"AND with status error",
			servicePathQuery("db", "cache") + "&status=error", []string{"t-chain"}},
		{"AND with status ok",
			servicePathQuery("gateway", "db") + "&status=ok", []string{"t-multi", "t-branch"}},
		{"AND with q",
			servicePathQuery("gateway", "db") + "&q=auth", []string{"t-branch"}},
		{"AND with service",
			servicePathQuery("gateway", "db") + "&service=cache", []string{"t-multi", "t-branch", "t-chain"}},
		{"AND with complete",
			servicePathQuery("gateway", "db") + "&complete=true", []string{"t-multi", "t-branch", "t-chain"}},
		{"limit caps the filtered list",
			servicePathQuery("gateway", "db") + "&limit=1", []string{"t-multi"}},
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
	// The filtered summaries keep their shape and the existing order.
	decoded := c.get("/traces" + servicePathQuery("gateway", "db", "cache"))
	items := listOf(t, decoded["traces"])
	first := objectOf(t, items[0])
	if first["trace_id"] != "t-multi" || first["root_service"] != "gateway" ||
		numberOf(t, first["span_count"]) != 5 || first["complete"] != true || first["valid"] != true {
		t.Fatalf("the service_path filter changed the summary shape: %v", first)
	}
}
