package tracepath

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	// idLayout is the timestamp format accepted on input and produced on output.
	// It is time.RFC3339Nano trimmed of trailing zeros, so "…T00:00:00Z" stays
	// readable while "…T00:00:00.000000001Z" keeps nanosecond precision.
	idLayout = "2006-01-02T15:04:05.999999999Z07:00"

	maxIdentifierLength = 64
	maxTextLength       = 128
	maxAttributeCount   = 32
	maxAttributeKey     = 64
	maxAttributeValue   = 512
	maxSpanCount        = 100000
	maxBodyBytes        = 1 << 20
	// maxBatchSpans bounds the number of spans one POST /spans/batch carries.
	maxBatchSpans = 1000
)

// spanKinds is the closed set of span kinds.
var spanKinds = map[string]bool{
	"internal": true,
	"server":   true,
	"client":   true,
	"producer": true,
	"consumer": true,
}

// spanStatuses is the closed set of span statuses.
var spanStatuses = map[string]bool{"ok": true, "error": true}

// SpanInput is one ingested span exactly as it arrived. The raw strings are
// kept verbatim so that persisted state round-trips byte for byte.
type SpanInput struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   *string           `json:"parent_id"`
	Service    string            `json:"service"`
	Operation  string            `json:"operation"`
	Kind       string            `json:"kind"`
	StartTime  string            `json:"start_time"`
	DurationNS int64             `json:"duration_ns"`
	Status     string            `json:"status"`
	Attributes map[string]string `json:"attributes"`
	SpanCount  *int              `json:"span_count"`
}

// IdempotencyRecord stores which resource the first request with a key created.
// The response body is re-rendered from state on every replay, so the recorded
// identity is enough to detect a key reused for a different operation.
type IdempotencyRecord struct {
	Key       string `json:"key"`
	Operation string `json:"operation"`
	Identity  string `json:"identity"`
	CreatedAt string `json:"created_at"`
	// Response holds the stored response of a batch ingestion. A batch response
	// reports the reassembly state of every trace as it was right after each
	// span landed, which later writes change, so a replay cannot re-render it
	// from state and returns this snapshot instead. Empty for single spans.
	Response json.RawMessage `json:"response,omitempty"`
}

// Violation is one structural error found while assembling a trace.
type Violation struct {
	Code    string `json:"code"`
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	Message string `json:"message"`
}

// TraceNode is one span of the reassembled tree with its derived timing. The
// declared duration is never rewritten; timing violations are reported instead.
type TraceNode struct {
	TraceID    string  `json:"trace_id"`
	SpanID     string  `json:"span_id"`
	ParentID   *string `json:"parent_id"`
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	Kind       string  `json:"kind"`
	Status     string  `json:"status"`
	StartTime  string  `json:"start_time"`
	EndTime    string  `json:"end_time"`
	DurationNS int64   `json:"duration_ns"`
	SelfTimeNS int64   `json:"self_time_ns"`
	// TotalDurationNS is the duration of the whole subtree of this span, derived
	// from the declared values rather than from wall-clock arithmetic.
	TotalDurationNS int64 `json:"subtree_duration_ns"`
	// maxPath is the longest self_time-weighted path from this span downwards. It
	// is internal bookkeeping and equals the span duration by construction.
	maxPath      int64
	Depth        int               `json:"depth"`
	Error        bool              `json:"error"`
	ErrorOrigin  bool              `json:"error_origin"`
	Propagated   bool              `json:"propagated"`
	ErrorOrigins []string          `json:"error_origins"`
	Attributes   map[string]string `json:"attributes"`
	Children     []*TraceNode      `json:"children"`
}

// assignTiming derives self_time and the critical path of one subtree.
//
// LongestPath(t) is the largest sum of self_time on any path from t downwards.
// The critical branch K is the child with the largest LongestPath. self_time(t)
// is then everything of t's duration that K does not account for:
// duration(t) - LongestPath(K), where a leaf is its own critical branch. That
// makes self_time exactly the exclusive time off the critical path, so the self
// times along the critical path add up to duration(t), which is the invariant
// this service promises.
//
// Children are independent of each other, so LongestPath(child) can be computed
// before self_time(t) and does not change afterwards.
func assignTiming(node *TraceNode) {
	if len(node.Children) == 0 {
		node.SelfTimeNS = node.DurationNS
		node.maxPath = node.DurationNS
		return
	}
	critical := node.Children[0]
	for _, child := range node.Children {
		assignTiming(child)
		if child.maxPath > critical.maxPath {
			critical = child
		}
	}
	selfTime := node.DurationNS - critical.maxPath
	if selfTime < 0 {
		selfTime = 0
	}
	node.SelfTimeNS = selfTime
	node.maxPath = selfTime + critical.maxPath
	if node.maxPath > node.DurationNS {
		node.maxPath = node.DurationNS
	}
}

// Trace is a fully reassembled trace. Valid is false when Violations is not
// empty; the tree is still returned so a caller can inspect the damage.
type Trace struct {
	TraceID       string       `json:"trace_id"`
	Root          *TraceNode   `json:"root"`
	DurationNS    int64        `json:"duration_ns"`
	SpanCount     int          `json:"span_count"`
	ServiceCount  int          `json:"service_count"`
	ErrorSpans    int          `json:"error_spans"`
	RootSpanCount int          `json:"declared_span_count"`
	Complete      bool         `json:"complete"`
	Valid         bool         `json:"valid"`
	Violations    []Violation  `json:"violations"`
	Spans         []*TraceNode `json:"spans"`
}

// PathStep is one hop of the critical path.
type PathStep struct {
	SpanID     string  `json:"span_id"`
	ParentID   *string `json:"parent_id"`
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	Depth      int     `json:"depth"`
	SelfTimeNS int64   `json:"self_time_ns"`
	StartTime  string  `json:"start_time"`
	EndTime    string  `json:"end_time"`
}

// CriticalPath is the weighted longest path through the span tree.
type CriticalPath struct {
	TraceID         string       `json:"trace_id"`
	Method          string       `json:"method"`
	TraceDurationNS int64        `json:"trace_duration_ns"`
	Path            []PathStep   `json:"path"`
	LengthNS        int64        `json:"length_ns"`
	Complete        bool         `json:"complete"`
	Valid           bool         `json:"valid"`
	Violations      []Validation `json:"violations"`
}

// OperationStat aggregates one (service, operation) pair.
type OperationStat struct {
	Operation      string `json:"operation"`
	SpanCount      int    `json:"span_count"`
	ErrorSpans     int    `json:"error_spans"`
	SelfTimeNS     int64  `json:"self_time_ns"`
	MinDurationNS  int64  `json:"min_duration_ns"`
	MaxDurationNS  int64  `json:"max_duration_ns"`
	MeanDurationNS int64  `json:"mean_duration_ns"`
	P50DurationNS  int64  `json:"p50_duration_ns"`
	P95DurationNS  int64  `json:"p95_duration_ns"`
	durations      []int64
}

// ServiceStat aggregates one service.
type ServiceStat struct {
	Service        string           `json:"service"`
	SpanCount      int              `json:"span_count"`
	ErrorSpans     int              `json:"error_spans"`
	RootSpans      int              `json:"root_spans"`
	SelfTimeNS     int64            `json:"self_time_ns"`
	MinDurationNS  int64            `json:"min_duration_ns"`
	MaxDurationNS  int64            `json:"max_duration_ns"`
	MeanDurationNS int64            `json:"mean_duration_ns"`
	P50DurationNS  int64            `json:"p50_duration_ns"`
	P95DurationNS  int64            `json:"p95_duration_ns"`
	Operations     []*OperationStat `json:"operations"`
	durations      []int64
}

// ServiceEdge aggregates one caller/callee pair.
type ServiceEdge struct {
	Caller         string `json:"caller"`
	Callee         string `json:"callee"`
	CallCount      int    `json:"call_count"`
	ErrorCount     int    `json:"error_count"`
	MinDurationNS  int64  `json:"min_duration_ns"`
	MaxDurationNS  int64  `json:"max_duration_ns"`
	MeanDurationNS int64  `json:"mean_duration_ns"`
	P50DurationNS  int64  `json:"p50_duration_ns"`
	P95DurationNS  int64  `json:"p95_duration_ns"`
	durations      []int64
}

// GraphStats is the process-wide roll-up that accompanies the service graph.
type GraphStats struct {
	TraceCount     int   `json:"trace_count"`
	SpanCount      int   `json:"span_count"`
	ErrorSpans     int   `json:"error_spans"`
	DurationNS     int64 `json:"duration_ns"`
	MeanDurationNS int64 `json:"mean_duration_ns"`
	P50DurationNS  int64 `json:"p50_duration_ns"`
	P95DurationNS  int64 `json:"p95_duration_ns"`
	PartialTraces  int   `json:"partial_traces"`
	durations      []int64
}

// ServiceGraph is the aggregated service dependency graph.
type ServiceGraph struct {
	TraceID  *string        `json:"trace_id"`
	Services []*ServiceStat `json:"services"`
	Edges    []*ServiceEdge `json:"edges"`
	Stats    *GraphStats    `json:"stats"`
}

// TraceSummary is one entry of GET /traces.
type TraceSummary struct {
	TraceID      string  `json:"trace_id"`
	RootSpanID   *string `json:"root_span_id"`
	RootService  *string `json:"root_service"`
	StartTime    string  `json:"start_time"`
	DurationNS   int64   `json:"duration_ns"`
	SpanCount    int     `json:"span_count"`
	ServiceCount int     `json:"service_count"`
	ErrorSpans   int     `json:"error_spans"`
	Complete     bool    `json:"complete"`
	Valid        bool    `json:"valid"`
	// rootInstant is the parsed root start time used by the list ordering and by
	// the start_from/start_to filters. It is not part of the JSON contract.
	rootInstant time.Time
}

// decodeObject strictly decodes one JSON object. Unknown fields, trailing
// content and non-object bodies are all rejected.
func decodeObject(body []byte, target any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return ValidationError("request body must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ValidationError("request body must be a JSON object with known fields: %s", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ValidationError("request body must contain exactly one JSON object")
	}
	return nil
}

// formatTime renders one UTC instant in the canonical layout.
func formatTime(instant time.Time) string {
	return instant.UTC().Format(idLayout)
}

// parseTime accepts an RFC3339 timestamp with optional fractional seconds.
func parseTime(value string, field string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, ValidationError("%s is required", field)
	}
	instant, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, ValidationError("%s must be an RFC3339 timestamp", field)
	}
	return instant.UTC(), nil
}

// containsFold reports whether text contains needle under Unicode simple case
// folding, the substring analogue of strings.EqualFold. Runes are compared by
// walking the complete SimpleFold equivalence cycle, the same rule
// strings.EqualFold applies at one rune position.
func containsFold(text string, needle string) bool {
	if needle == "" {
		return true
	}
	haystack := []rune(text)
	target := []rune(needle)
	if len(target) > len(haystack) {
		return false
	}
	for start := 0; start <= len(haystack)-len(target); start++ {
		matches := true
		for offset := range target {
			if !runesEqualFold(haystack[start+offset], target[offset]) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

// runesEqualFold reports whether two runes are simple-case-fold equivalent.
func runesEqualFold(left rune, right rune) bool {
	if left == right {
		return true
	}
	for r := unicode.SimpleFold(left); r != left; r = unicode.SimpleFold(r) {
		if r == right {
			return true
		}
	}
	return false
}

func validateIdentifier(value string, field string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", ValidationError("%s is required", field)
	}
	if len(trimmed) > maxIdentifierLength {
		return "", ValidationError("%s must be at most %d characters", field, maxIdentifierLength)
	}
	return trimmed, nil
}

func validateText(value string, field string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", ValidationError("%s is required", field)
	}
	if len(trimmed) > maxTextLength {
		return "", ValidationError("%s must be at most %d characters", field, maxTextLength)
	}
	return trimmed, nil
}

// validateSpanInput checks every rule that can be decided without looking at
// the other spans of the trace.
func validateSpanInput(input *SpanInput) (time.Time, error) {
	if _, err := validateIdentifier(input.TraceID, "trace_id"); err != nil {
		return time.Time{}, err
	}
	if _, err := validateIdentifier(input.SpanID, "span_id"); err != nil {
		return time.Time{}, err
	}
	if input.ParentID != nil {
		if strings.TrimSpace(*input.ParentID) == "" {
			return time.Time{}, ValidationError("parent_id must be null or a non-empty identifier")
		}
	}
	if _, err := validateText(input.Service, "service"); err != nil {
		return time.Time{}, err
	}
	if _, err := validateText(input.Operation, "operation"); err != nil {
		return time.Time{}, err
	}
	if !spanKinds[input.Kind] {
		return time.Time{}, ValidationError("kind must be one of internal, server, client, producer or consumer")
	}
	if !spanStatuses[input.Status] {
		return time.Time{}, ValidationError("status must be ok or error")
	}
	start, err := parseTime(input.StartTime, "start_time")
	if err != nil {
		return time.Time{}, err
	}
	if input.DurationNS < 0 {
		return time.Time{}, ValidationError("duration_ns must be zero or positive")
	}
	if int64(start.UnixNano())+input.DurationNS > int64(maxTime.UnixNano()) {
		return time.Time{}, ValidationError("start_time + duration_ns is beyond the supported range")
	}
	if len(input.Attributes) > maxAttributeCount {
		return time.Time{}, ValidationError("attributes must contain at most %d entries", maxAttributeCount)
	}
	keys := make([]string, 0, len(input.Attributes))
	for key := range input.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			return time.Time{}, ValidationError("attribute keys must not be empty")
		}
		if len(key) > maxAttributeKey {
			return time.Time{}, ValidationError("attribute key %s must be at most %d characters", key, maxAttributeKey)
		}
		if len(input.Attributes[key]) > maxAttributeValue {
			return time.Time{}, ValidationError("attribute %s must be at most %d characters", key, maxAttributeValue)
		}
	}
	if input.ParentID != nil && *input.ParentID == input.SpanID {
		return time.Time{}, ValidationError("parent_id must not equal span_id on span %s", input.SpanID)
	}
	if input.ParentID == nil {
		if input.SpanCount == nil {
			return time.Time{}, ValidationError("span_count is required on a root span (parent_id null)")
		}
		if *input.SpanCount < 1 {
			return time.Time{}, ValidationError("span_count must be at least 1")
		}
		if *input.SpanCount > maxSpanCount {
			return time.Time{}, ValidationError("span_count must be at most %d", maxSpanCount)
		}
	} else if input.SpanCount != nil {
		return time.Time{}, ValidationError("span_count is only allowed on a root span")
	}
	return start, nil
}

// maxTime bounds start_time + duration_ns so that UnixNano arithmetic cannot
// overflow int64.
var maxTime = time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC)

// lineOf resolves a span's trace id by walking parent links, which also detects
// a parent cycle and a parent that lives in another trace. A parent that has
// not been ingested yet simply ends the walk: every span of a trace carries the
// same trace id, so the input's own id is the answer.
func lineOf(spans map[string]*SpanInput, input *SpanInput) (string, error) {
	seen := map[string]bool{input.SpanID: true}
	current := input
	for current.ParentID != nil {
		parentID := *current.ParentID
		if seen[parentID] {
			return "", ConflictError("span %s has a cyclic parent_id chain", input.SpanID)
		}
		parent, found := spans[parentID]
		if !found {
			return input.TraceID, nil
		}
		seen[parentID] = true
		if parent.TraceID != input.TraceID {
			return "", ConflictError("span %s belongs to trace %s but its parent %s belongs to trace %s",
				input.SpanID, input.TraceID, parentID, parent.TraceID)
		}
		current = parent
	}
	return input.TraceID, nil
}

type treeBuilder struct {
	spans      map[string]*SpanInput
	children   map[string][]*SpanInput
	roots      []*SpanInput
	violations []Violation
	// clockSkewTolerance widens the parent interval for the time_not_contained
	// check only: a child may start up to this many nanoseconds before its
	// parent and end up to this many after it. Every other invariant (sibling
	// overlap, subtree duration, root count) stays strict.
	clockSkewTolerance time.Duration
}

// assembleTrace rebuilds the span tree of one trace and derives every timing
// value. Violations are collected, never fatal, so a caller always sees why a
// trace is unusable. clockSkewToleranceNS relaxes only the parent/child time
// containment check; 0 is the strict mode.
func assembleTrace(traceID string, spans map[string]*SpanInput, clockSkewToleranceNS int64) *Trace {
	trace := &Trace{
		TraceID:    traceID,
		SpanCount:  len(spans),
		Valid:      true,
		Violations: []Violation{},
		Spans:      []*TraceNode{},
	}
	builder := &treeBuilder{
		spans:              spans,
		children:           map[string][]*SpanInput{},
		clockSkewTolerance: time.Duration(clockSkewToleranceNS),
	}
	ids := make([]string, 0, len(spans))
	for id := range spans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		span := spans[id]
		if span.ParentID == nil {
			builder.roots = append(builder.roots, span)
			continue
		}
		if _, found := spans[*span.ParentID]; !found {
			builder.violations = append(builder.violations, Violation{
				Code:    "orphan_span",
				TraceID: traceID,
				SpanID:  span.SpanID,
				Message: fmt.Sprintf("span %s references parent %s which is not part of trace %s",
					span.SpanID, *span.ParentID, traceID),
			})
			continue
		}
		builder.children[*span.ParentID] = append(builder.children[*span.ParentID], span)
	}
	declared := 0
	if len(builder.roots) == 1 && builder.roots[0].SpanCount != nil {
		declared = *builder.roots[0].SpanCount
	}
	switch {
	case len(builder.roots) == 1:
	case len(builder.roots) == 0:
		builder.violations = append(builder.violations, Violation{
			Code:    "no_root_span",
			TraceID: traceID,
			SpanID:  "",
			Message: fmt.Sprintf("trace %s has no span with a null parent_id", traceID),
		})
	default:
		names := make([]string, 0, len(builder.roots))
		for _, root := range builder.roots {
			names = append(names, root.SpanID)
		}
		sort.Strings(names)
		builder.violations = append(builder.violations, Violation{
			Code:    "multiple_root_spans",
			TraceID: traceID,
			SpanID:  names[0],
			Message: fmt.Sprintf("trace %s has %d root spans: %s", traceID, len(names), strings.Join(names, ", ")),
		})
	}
	trace.RootSpanCount = declared
	trace.Complete = len(builder.roots) == 1 && declared != 0 && declared == len(spans)
	if declared != 0 && declared < len(spans) {
		builder.violations = append(builder.violations, Violation{
			Code:    "span_count_exceeded",
			TraceID: traceID,
			SpanID:  builder.roots[0].SpanID,
			Message: fmt.Sprintf("trace %s declared span_count %d but %d spans were ingested",
				traceID, declared, len(spans)),
		})
	}
	if declared != 0 && declared > len(spans) {
		builder.violations = append(builder.violations, Violation{
			Code:    "trace_incomplete",
			TraceID: traceID,
			SpanID:  builder.roots[0].SpanID,
			Message: fmt.Sprintf("trace %s declared span_count %d but only %d spans were ingested",
				traceID, declared, len(spans)),
		})
	}

	// Every span that has no reachable root is still reported, as its own tree.
	reachable := map[string]bool{}
	for _, root := range builder.roots {
		builder.markReachable(root, reachable)
	}
	for _, id := range ids {
		if !reachable[id] && !builder.isOrphan(id) {
			builder.violations = append(builder.violations, Violation{
				Code:    "unreachable_span",
				TraceID: traceID,
				SpanID:  id,
				Message: fmt.Sprintf("span %s is not reachable from a root span", id),
			})
		}
	}

	sortedRoots := append([]*SpanInput{}, builder.roots...)
	sort.Slice(sortedRoots, func(i, j int) bool {
		left, _ := parseTime(sortedRoots[i].StartTime, "start_time")
		right, _ := parseTime(sortedRoots[j].StartTime, "start_time")
		if !left.Equal(right) {
			return left.Before(right)
		}
		return sortedRoots[i].SpanID < sortedRoots[j].SpanID
	})
	for _, root := range sortedRoots {
		start, _ := parseTime(root.StartTime, "start_time")
		node := builder.buildNode(root, start, 0)
		assignTiming(node)
		trace.Spans = append(trace.Spans, flatten(node)...)
		if trace.Root == nil {
			trace.Root = node
			trace.DurationNS = node.DurationNS
		}
	}
	services := map[string]bool{}
	for _, node := range trace.Spans {
		collectServices(node, services)
	}
	trace.ServiceCount = len(services)
	trace.ErrorSpans = countErrorSpans(trace.Spans)
	trace.Valid = len(builder.violations) == 0
	trace.Violations = builder.violations
	sortViolations(trace.Violations)
	return trace
}

func (b *treeBuilder) isOrphan(id string) bool {
	span := b.spans[id]
	return span.ParentID != nil && b.spans[*span.ParentID] == nil
}

func (b *treeBuilder) markReachable(span *SpanInput, seen map[string]bool) {
	if seen[span.SpanID] {
		return
	}
	seen[span.SpanID] = true
	for _, child := range b.children[span.SpanID] {
		b.markReachable(child, seen)
	}
}

// buildNode derives one node and everything below it. parentEnd is the
// exclusive end instant of the enclosing span; a child that overruns it is
// recorded as a violation and the node is still returned.
func (b *treeBuilder) buildNode(span *SpanInput, parentEnd time.Time, depth int) *TraceNode {
	start, err := parseTime(span.StartTime, "start_time")
	if err != nil {
		start = time.Time{}
	}
	end := start.Add(time.Duration(span.DurationNS))
	node := &TraceNode{
		TraceID:      span.TraceID,
		SpanID:       span.SpanID,
		ParentID:     span.ParentID,
		Service:      span.Service,
		Operation:    span.Operation,
		Kind:         span.Kind,
		Status:       span.Status,
		StartTime:    span.StartTime,
		EndTime:      formatTime(end),
		DurationNS:   span.DurationNS,
		Depth:        depth,
		Error:        span.Status == "error",
		Attributes:   map[string]string{},
		Children:     []*TraceNode{},
		ErrorOrigins: []string{},
	}
	for key, value := range span.Attributes {
		node.Attributes[key] = value
	}
	children := append([]*SpanInput{}, b.children[span.SpanID]...)
	sort.Slice(children, func(i, j int) bool {
		left, _ := parseTime(children[i].StartTime, "start_time")
		right, _ := parseTime(children[j].StartTime, "start_time")
		if !left.Equal(right) {
			return left.Before(right)
		}
		return children[i].SpanID < children[j].SpanID
	})
	previousEnd := start
	previousID := ""
	// The parent interval is widened by the configured clock skew tolerance on
	// both sides, independently: a child may start at parent start - T and end
	// at parent end + T, boundaries included. The tolerance never applies to
	// the sibling overlap or subtree duration checks below.
	toleratedStart := start.Add(-b.clockSkewTolerance)
	toleratedEnd := end.Add(b.clockSkewTolerance)
	for _, child := range children {
		childStart, _ := parseTime(child.StartTime, "start_time")
		childEnd := childStart.Add(time.Duration(child.DurationNS))
		if childStart.Before(toleratedStart) || childEnd.After(toleratedEnd) {
			b.violations = append(b.violations, Violation{
				Code:    "time_not_contained",
				TraceID: span.TraceID,
				SpanID:  child.SpanID,
				Message: fmt.Sprintf("span %s runs from %s to %s which is not contained in parent %s from %s to %s",
					child.SpanID, child.StartTime, formatTime(childEnd), span.SpanID, span.StartTime, formatTime(end)),
			})
		}
		// Siblings must not overlap. Comparing each child against the furthest end
		// seen so far catches an overlap with any earlier sibling, not only with
		// the neighbour that starts just before it.
		if previousID != "" && childStart.Before(previousEnd) {
			b.violations = append(b.violations, Violation{
				Code:    "sibling_overlap",
				TraceID: span.TraceID,
				SpanID:  previousID,
				Message: fmt.Sprintf("span %s starts at %s before sibling %s ends at %s inside parent %s",
					child.SpanID, child.StartTime, previousID, formatTime(previousEnd), span.SpanID),
			})
		}
		if childEnd.After(previousEnd) {
			previousEnd = childEnd
			previousID = child.SpanID
		}
		node.Children = append(node.Children, b.buildNode(child, end, depth+1))
	}
	node.TotalDurationNS = span.DurationNS
	for _, child := range node.Children {
		if child.TotalDurationNS > span.DurationNS {
			b.violations = append(b.violations, Violation{
				Code:    "time_not_contained",
				TraceID: span.TraceID,
				SpanID:  child.SpanID,
				Message: fmt.Sprintf("subtree of %s lasts %d but its parent %s only lasts %d",
					child.SpanID, child.TotalDurationNS, span.SpanID, span.DurationNS),
			})
		}
		if child.TotalDurationNS > node.TotalDurationNS {
			node.TotalDurationNS = child.TotalDurationNS
		}
	}
	origins := map[string]bool{}
	if node.Error {
		origins[span.SpanID] = true
	}
	for _, child := range node.Children {
		for _, origin := range child.ErrorOrigins {
			origins[origin] = true
		}
	}
	for origin := range origins {
		node.ErrorOrigins = append(node.ErrorOrigins, origin)
	}
	sort.Strings(node.ErrorOrigins)
	node.ErrorOrigin = node.Error
	// propagated marks a span whose subtree contains a failure that did not
	// happen on the span itself. The declared status stays untouched: only the
	// failing origin carries status "error".
	node.Propagated = false
	for _, origin := range node.ErrorOrigins {
		if origin != node.SpanID {
			node.Propagated = true
			break
		}
	}
	return node
}

func collectServices(node *TraceNode, into map[string]bool) {
	if into[node.Service] {
		return
	}
	into[node.Service] = true
	for _, child := range node.Children {
		collectServices(child, into)
	}
}

// countErrorSpans counts failed spans in a flat span list. The list already
// contains every node once, so the nodes themselves are never walked again.
func countErrorSpans(nodes []*TraceNode) int {
	total := 0
	for _, node := range nodes {
		if node.Error {
			total++
		}
	}
	return total
}

func sortViolations(violations []Violation) {
	sort.Slice(violations, func(i, j int) bool {
		if violations[i].SpanID != violations[j].SpanID {
			return violations[i].SpanID < violations[j].SpanID
		}
		return violations[i].Code < violations[j].Code
	})
}

// flatten walks the tree in pre-order and returns the nodes it contains.
func flatten(root *TraceNode) []*TraceNode {
	if root == nil {
		return nil
	}
	nodes := []*TraceNode{root}
	for _, child := range root.Children {
		nodes = append(nodes, flatten(child)...)
	}
	return nodes
}
