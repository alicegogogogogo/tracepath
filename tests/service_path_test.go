package tests

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// loadPathFixture builds traces that exercise chain matching:
//
//	t-1  the six-span fixture: gateway → {auth, db → {db, db}, queue}
//	t-p  svc-a → svc-b → svc-a: a service repeats non-adjacently on one chain
//	t-q  gateway with two db children: the same chain matches twice
func loadPathFixture(t *testing.T) *client {
	t.Helper()
	c := newClient(t)
	c.loadFixture()
	c.post(span("t-p", "p-root", "", "svc-a", "entry", "server",
		"2024-06-01T00:00:10Z", 300, "ok", count(3)))
	c.post(span("t-p", "p-mid", "p-root", "svc-b", "forward", "client",
		"2024-06-01T00:00:10.000000010Z", 200, "ok", nil))
	c.post(span("t-p", "p-leaf", "p-mid", "svc-a", "finish", "client",
		"2024-06-01T00:00:10.000000020Z", 100, "ok", nil))
	c.post(span("t-q", "q-root", "", "gateway", "GET /twice", "server",
		"2024-06-01T00:00:20Z", 300, "ok", count(3)))
	c.post(span("t-q", "q-db1", "q-root", "db", "select", "client",
		"2024-06-01T00:00:20.000000010Z", 100, "ok", nil))
	c.post(span("t-q", "q-db2", "q-root", "db", "select", "client",
		"2024-06-01T00:00:20.000000120Z", 100, "ok", nil))
	return c
}

func TestServicePathMatchesChains(t *testing.T) {
	c := loadPathFixture(t)
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"single chain across two services", "?service_path=gateway&service_path=db",
			[]string{"t-q", "t-1"}},
		{"branch to the queue child", "?service_path=gateway&service_path=queue",
			[]string{"t-1"}},
		{"branch to the auth child", "?service_path=gateway&service_path=auth",
			[]string{"t-1"}},
		{"siblings are not a chain", "?service_path=db&service_path=queue", []string{}},
		{"reversed order does not match", "?service_path=db&service_path=gateway", []string{}},
		{"non-adjacent service repeats on one chain",
			"?service_path=svc-a&service_path=svc-b&service_path=svc-a", []string{"t-p"}},
		{"the chain may start mid-trace", "?service_path=svc-b&service_path=svc-a",
			[]string{"t-p"}},
		{"no matching chain is an empty list", "?service_path=gateway&service_path=cache",
			[]string{}},
		{"names match verbatim and case sensitively", "?service_path=Gateway&service_path=db",
			[]string{}},
		{"a name is not trimmed", "?service_path=" + enc("gateway ") + "&service_path=db",
			[]string{}},
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

// TestServicePathListsAMultiHitTraceOnce checks that a trace containing the
// chain on two distinct branches still appears exactly once.
func TestServicePathListsAMultiHitTraceOnce(t *testing.T) {
	c := loadPathFixture(t)
	got := listTraceIDs(t, c, "?service_path=gateway&service_path=db")
	count := 0
	for _, id := range got {
		if id == "t-q" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("t-q matches gateway->db on two branches but was listed %d times: %v", count, got)
	}
}

func TestServicePathCombinesWithOtherFilters(t *testing.T) {
	c := loadPathFixture(t)
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"AND with status error", "?service_path=gateway&service_path=db&status=error",
			[]string{"t-1"}},
		{"AND with status ok", "?service_path=gateway&service_path=db&status=ok",
			[]string{"t-q"}},
		{"AND with service", "?service_path=gateway&service_path=db&service=queue",
			[]string{"t-1"}},
		{"AND with complete", "?service_path=gateway&service_path=db&complete=true",
			[]string{"t-q", "t-1"}},
		{"AND with a duration bound that drops one hit",
			"?service_path=gateway&service_path=db&min_duration_ns=400", []string{"t-1"}},
		// Filtering runs before ordering and limit, so the newest hit survives.
		{"limit applies after filtering", "?service_path=gateway&service_path=db&limit=1",
			[]string{"t-q"}},
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

func TestServicePathRejectsBadParameters(t *testing.T) {
	c := loadPathFixture(t)
	tooMany := make([]string, 0, 33)
	for i := 0; i < 33; i++ {
		if i%2 == 0 {
			tooMany = append(tooMany, "service_path=a")
		} else {
			tooMany = append(tooMany, "service_path=b")
		}
	}
	bad := []string{
		"?service_path=gateway",
		"?service_path=",
		"?service_path=gateway&service_path=",
		"?service_path=db&service_path=db",
		"?service_path=gateway&service_path=db&service_path=db",
		"?service_path=" + strings.Repeat("a", 129) + "&service_path=db",
		"?" + strings.Join(tooMany, "&"),
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

// TestServicePathBoundariesAreAccepted pins the valid extremes: a 128-character
// name and a 32-element chain are legal and simply find nothing.
func TestServicePathBoundariesAreAccepted(t *testing.T) {
	c := loadPathFixture(t)
	longName := strings.Repeat("a", 128)
	if got := listTraceIDs(t, c, "?service_path="+longName+"&service_path=db"); len(got) != 0 {
		t.Fatalf("a 128-character name must be accepted, got %v", got)
	}
	chain := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		if i%2 == 0 {
			chain = append(chain, "service_path=a")
		} else {
			chain = append(chain, "service_path=b")
		}
	}
	if got := listTraceIDs(t, c, "?"+strings.Join(chain, "&")); len(got) != 0 {
		t.Fatalf("a 32-element chain must be accepted, got %v", got)
	}
}

// TestServicePathDoesNotAffectUnfilteredListing ensures the parameter only
// narrows the candidate set: without it the listing is byte-identical in shape
// and order to the legacy behaviour.
func TestServicePathDoesNotAffectUnfilteredListing(t *testing.T) {
	c := loadPathFixture(t)
	got := listTraceIDs(t, c, "")
	want := []string{"t-q", "t-p", "t-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unfiltered listing %v, want %v", got, want)
	}
	// The summary fields are unchanged by the presence of the filter.
	decoded := c.get("/traces?service_path=gateway&service_path=db")
	items := listOf(t, decoded["traces"])
	if len(items) != 2 {
		t.Fatalf("expected two traces, got %v", items)
	}
	summary := objectOf(t, items[1])
	if summary["trace_id"] != "t-1" ||
		summary["root_service"] != "gateway" ||
		numberOf(t, summary["span_count"]) != 6 ||
		numberOf(t, summary["error_spans"]) != 1 ||
		summary["complete"] != true || summary["valid"] != true {
		t.Fatalf("filtered summary lost fields: %v", summary)
	}
}
