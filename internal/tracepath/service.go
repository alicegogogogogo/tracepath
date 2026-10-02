package tracepath

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Service implements the TracePath contract on top of a Store. It owns span
// ingestion, trace reassembly, critical path analysis, error propagation
// marking and the service dependency graph.
type Service struct {
	store *Store
	clock func() time.Time
}

// NewService wires a store and an injectable clock.
func NewService(store *Store, clock func() time.Time) (*Service, error) {
	if store == nil {
		return nil, InternalError("service requires a store")
	}
	if clock == nil {
		clock = time.Now
	}
	return &Service{store: store, clock: clock}, nil
}

func (s *Service) now() string {
	return s.clock().UTC().Format(idLayout)
}

// validateIdempotencyKey enforces the presence and length rules every
// idempotent operation shares.
func validateIdempotencyKey(key string) error {
	if key == "" {
		return ValidationError("Idempotency-Key header is required")
	}
	if len(key) > 200 {
		return ValidationError("Idempotency-Key must be at most 200 characters")
	}
	return nil
}

// runIdempotent executes action at most once per key. The first response is
// re-rendered from committed state for every later request that carries the
// same key and the same resource identity, so a replay is byte-identical by
// construction. Reusing a key for another operation, or for another resource,
// is a conflict.
func (s *Service) runIdempotent(key string, operation string, identity string, action func(state *State) (any, error)) (any, error) {
	if err := validateIdempotencyKey(key); err != nil {
		return nil, err
	}
	var response any
	err := s.store.Update(func(state *State) error {
		if record, found := state.Idempotency[key]; found {
			if record.Operation != operation {
				return ConflictError("idempotency key was already used for another operation")
			}
			if record.Identity != identity {
				return ConflictError("idempotency key was already used for resource %s", record.Identity)
			}
			response = identity
			return nil
		}
		value, err := action(state)
		if err != nil {
			return err
		}
		state.Idempotency[key] = &IdempotencyRecord{
			Key:       key,
			Operation: operation,
			Identity:  identity,
			CreatedAt: s.now(),
		}
		response = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// spanResponse is the public projection of one ingested span.
type spanResponse struct {
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
	Accepted   bool              `json:"accepted"`
	SpanCount  int               `json:"span_count"`
	Complete   bool              `json:"complete"`
	Valid      bool              `json:"valid"`
	Violations []Validation      `json:"violations"`
}

// renderSpan projects one stored span onto its public projection. The identity
// of the projected span is returned as well.
func renderSpan(state *State, traceID string, spanID string) (any, error) {
	spans, found := state.Spans[traceID]
	if !found {
		return nil, InternalError("trace %s disappeared during ingestion", traceID)
	}
	span, found := spans[spanID]
	if !found {
		return nil, InternalError("span %s of trace %s disappeared during ingestion", spanID, traceID)
	}
	trace := assembleTrace(traceID, spans)
	declared := trace.SpanCount
	if trace.RootSpanCount != 0 {
		declared = trace.RootSpanCount
	}
	response := &spanResponse{
		TraceID:    span.TraceID,
		SpanID:     span.SpanID,
		ParentID:   span.ParentID,
		Service:    span.Service,
		Operation:  span.Operation,
		Kind:       span.Kind,
		StartTime:  span.StartTime,
		DurationNS: span.DurationNS,
		Status:     span.Status,
		Attributes: map[string]string{},
		Accepted:   true,
		SpanCount:  declared,
		Complete:   trace.Complete,
		Valid:      trace.Valid,
		Violations: publicViolations(trace.Violations),
	}
	for attribute, value := range span.Attributes {
		response.Attributes[attribute] = value
	}
	return response, nil
}

// decodeSpanInput strictly decodes one span document and runs every check that
// does not need the stored state. It returns the parsed start instant and the
// trimmed identifiers, exactly as single-span ingestion uses them.
func decodeSpanInput(body []byte) (*SpanInput, time.Time, string, string, error) {
	var input SpanInput
	if err := decodeObject(body, &input); err != nil {
		return nil, time.Time{}, "", "", err
	}
	start, err := validateSpanInput(&input)
	if err != nil {
		return nil, time.Time{}, "", "", err
	}
	spanID, err := validateIdentifier(input.SpanID, "span_id")
	if err != nil {
		return nil, time.Time{}, "", "", err
	}
	traceID, err := validateIdentifier(input.TraceID, "trace_id")
	if err != nil {
		return nil, time.Time{}, "", "", err
	}
	return &input, start, spanID, traceID, nil
}

// applySpan stores one validated span in state. Every check that needs the
// other spans (parent existence across traces, duplicates, trace id agreement,
// time containment) runs here against the state the caller passes, so a batch
// sees the spans it has already applied exactly like sequential single-span
// ingestion would.
func applySpan(state *State, input *SpanInput, start time.Time, spanID string, traceID string) error {
	spans, found := state.Spans[traceID]
	if !found {
		if parentID := input.ParentID; parentID != nil {
			if _, elsewhere := findSpan(state, *parentID); elsewhere != nil {
				return ConflictError("span %s belongs to trace %s but its parent %s belongs to trace %s",
					spanID, traceID, *parentID, *elsewhere)
			}
		}
		spans = map[string]*SpanInput{}
		state.Spans[traceID] = spans
	}
	if _, duplicate := spans[spanID]; duplicate {
		return ConflictError("span %s already exists in trace %s", spanID, traceID)
	}
	resolved, err := lineOf(spans, input)
	if err != nil {
		return err
	}
	if resolved != traceID {
		return ConflictError("span %s belongs to trace %s but its parent chain resolves to trace %s",
			spanID, traceID, resolved)
	}
	if input.ParentID != nil {
		parent, found := spans[*input.ParentID]
		if found {
			parentStart, _ := parseTime(parent.StartTime, "start_time")
			parentEnd := parentStart.Add(time.Duration(parent.DurationNS))
			end := start.Add(time.Duration(input.DurationNS))
			if start.Before(parentStart) || end.After(parentEnd) {
				return ConflictError("span %s runs from %s to %s which is not contained in parent %s from %s to %s",
					spanID, formatTime(start), formatTime(end), parent.SpanID, parent.StartTime, formatTime(parentEnd))
			}
		}
	}
	clone := *input
	clone.Attributes = map[string]string{}
	for attribute, value := range input.Attributes {
		clone.Attributes[attribute] = value
	}
	spans[spanID] = &clone
	return nil
}

// IngestSpan validates and stores one span. Every check that needs the other
// spans of the trace (parent existence, trace id agreement, time containment)
// runs against the committed state under the store lock.
func (s *Service) IngestSpan(body []byte, key string) (any, error) {
	input, start, spanID, traceID, err := decodeSpanInput(body)
	if err != nil {
		return nil, err
	}
	operation := "ingest-span"
	identity := traceID + "/" + spanID

	if _, err := s.runIdempotent(key, operation, identity, func(state *State) (any, error) {
		return nil, applySpan(state, input, start, spanID, traceID)
	}); err != nil {
		return nil, err
	}
	return s.store.View(func(state *State) (any, error) {
		return renderSpan(state, traceID, spanID)
	})
}

// batchItem is one entry of the accepted array of a batch response. Complete,
// Valid and Violations snapshot the reassembly of the item's trace as it was
// right after that span landed.
type batchItem struct {
	TraceID    string       `json:"trace_id"`
	SpanID     string       `json:"span_id"`
	Accepted   bool         `json:"accepted"`
	Complete   bool         `json:"complete"`
	Valid      bool         `json:"valid"`
	Violations []Validation `json:"violations"`
}

// batchResponse is the body of a successful POST /spans/batch.
type batchResponse struct {
	Accepted []*batchItem `json:"accepted"`
	Count    int          `json:"count"`
}

// batchEnvelope is the request body of POST /spans/batch: nothing but the
// spans array. Each element stays raw so the spans are decoded and checked one
// by one in request order.
type batchEnvelope struct {
	Spans []json.RawMessage `json:"spans"`
}

// IngestSpanBatch validates and stores a whole batch atomically. The spans are
// applied in request order with exactly the rules of single-span ingestion; the
// first failure aborts the batch, nothing is committed and the idempotency key
// stays unused. A replay with the same key and a byte-identical body returns
// the stored first response, because the per-item reassembly snapshots cannot
// be re-rendered once later writes have changed the traces.
func (s *Service) IngestSpanBatch(body []byte, key string) (any, error) {
	if err := validateIdempotencyKey(key); err != nil {
		return nil, err
	}
	var envelope batchEnvelope
	if err := decodeObject(body, &envelope); err != nil {
		return nil, err
	}
	if len(envelope.Spans) == 0 {
		return nil, ValidationError("spans must contain at least 1 span")
	}
	if len(envelope.Spans) > maxBatchSpans {
		return nil, ValidationError("spans must contain at most %d spans", maxBatchSpans)
	}
	operation := "ingest-span-batch"
	identity := fmt.Sprintf("batch:%x", sha256.Sum256(body))

	var response any
	err := s.store.Update(func(state *State) error {
		if record, found := state.Idempotency[key]; found {
			if record.Operation != operation {
				return ConflictError("idempotency key was already used for another operation")
			}
			if record.Identity != identity {
				return ConflictError("idempotency key was already used for a different batch")
			}
			response = record.Response
			return nil
		}
		result := &batchResponse{Accepted: []*batchItem{}}
		for _, raw := range envelope.Spans {
			input, start, spanID, traceID, err := decodeSpanInput(raw)
			if err != nil {
				return err
			}
			if err := applySpan(state, input, start, spanID, traceID); err != nil {
				return err
			}
			trace := assembleTrace(traceID, state.Spans[traceID])
			result.Accepted = append(result.Accepted, &batchItem{
				TraceID:    traceID,
				SpanID:     spanID,
				Accepted:   true,
				Complete:   trace.Complete,
				Valid:      trace.Valid,
				Violations: publicViolations(trace.Violations),
			})
		}
		result.Count = len(result.Accepted)
		snapshot, err := json.Marshal(result)
		if err != nil {
			return InternalError("batch response could not be serialised: %s", err)
		}
		state.Idempotency[key] = &IdempotencyRecord{
			Key:       key,
			Operation: operation,
			Identity:  identity,
			CreatedAt: s.now(),
			Response:  snapshot,
		}
		response = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// findSpan locates a span id in any trace and returns its trace id.
func findSpan(state *State, spanID string) (*SpanInput, *string) {
	for traceID, spans := range state.Spans {
		if span, found := spans[spanID]; found {
			id := traceID
			return span, &id
		}
	}
	return nil, nil
}

func publicViolations(violations []Violation) []Validation {
	public := make([]Validation, 0, len(violations))
	for _, violation := range violations {
		public = append(public, Validation{
			Code:    violation.Code,
			TraceID: violation.TraceID,
			SpanID:  violation.SpanID,
			Message: violation.Message,
		})
	}
	return public
}

// GetTrace reassembles one trace and fails when it is incomplete or unusable.
func (s *Service) GetTrace(traceID string) (any, error) {
	value, err := s.store.View(func(state *State) (any, error) {
		spans, found := state.Spans[traceID]
		if !found {
			return nil, NotFoundError("trace %s does not exist", traceID)
		}
		return assembleTrace(traceID, spans), nil
	})
	if err != nil {
		return nil, err
	}
	trace := value.(*Trace)
	if !trace.Complete {
		return nil, InvalidTraceError(traceID, append([]Violation{{
			Code:    "trace_incomplete",
			TraceID: traceID,
			SpanID:  "",
			Message: "trace is not complete yet or its declared span_count does not match the ingested spans",
		}}, trace.Violations...))
	}
	if !trace.Valid {
		return nil, InvalidTraceError(traceID, trace.Violations)
	}
	return trace, nil
}

// TraceFilter narrows GET /traces. Every condition that is set must hold; a
// zero-valued filter keeps every trace. Time and duration fields are present as
// pointers so that an unset bound never accidentally matches as zero.
type TraceFilter struct {
	// Query is matched as a Unicode case-insensitive substring of the trace id
	// or of any span service or operation.
	Query string
	// Operation keeps traces that have at least one span with this exact op.
	Operation string
	// Service keeps traces that contain this service (the legacy semantics).
	Service string
	// Status is "", "error" (at least one error span) or "ok" (no error span).
	Status string
	// StartFrom is inclusive and StartTo is exclusive on the root start time.
	StartFrom *time.Time
	StartTo   *time.Time
	// MinDurationNS and MaxDurationNS are both inclusive on the root duration.
	MinDurationNS *int64
	MaxDurationNS *int64
	// Complete and Valid filter on the current reassembly result; nil = unset.
	Complete *bool
	Valid    *bool
	// ServicePath is an ordered chain of two or more service names. A trace
	// matches when it holds spans s1..sn, each a direct child of the previous
	// one, whose services equal the chain element by element. Nil means unset.
	ServicePath []string
}

// TraceFilterFromQuery parses and validates every documented GET /traces
// parameter apart from limit, which the HTTP layer keeps. Unknown parameters
// are rejected before this is called. All failures are validation errors.
func TraceFilterFromQuery(query url.Values) (*TraceFilter, error) {
	filter := &TraceFilter{
		Query:   query.Get("q"),
		Service: strings.TrimSpace(query.Get("service")),
	}
	if raw, present := query.Get("operation"), query.Has("operation"); present {
		operation := strings.TrimSpace(raw)
		if operation == "" {
			return nil, ValidationError("operation must not be empty")
		}
		filter.Operation = operation
	}
	if query.Has("status") {
		status := query.Get("status")
		if status != "ok" && status != "error" {
			return nil, ValidationError("status must be ok or error")
		}
		filter.Status = status
	}
	if raw, present := query.Get("start_from"), query.Has("start_from"); present {
		instant, err := parseTime(raw, "start_from")
		if err != nil {
			return nil, err
		}
		filter.StartFrom = &instant
	}
	if raw, present := query.Get("start_to"), query.Has("start_to"); present {
		instant, err := parseTime(raw, "start_to")
		if err != nil {
			return nil, err
		}
		filter.StartTo = &instant
	}
	if raw, present := query.Get("min_duration_ns"), query.Has("min_duration_ns"); present {
		value, err := parseDurationBound(raw, "min_duration_ns")
		if err != nil {
			return nil, err
		}
		filter.MinDurationNS = &value
	}
	if raw, present := query.Get("max_duration_ns"), query.Has("max_duration_ns"); present {
		value, err := parseDurationBound(raw, "max_duration_ns")
		if err != nil {
			return nil, err
		}
		filter.MaxDurationNS = &value
	}
	if filter.MinDurationNS != nil && filter.MaxDurationNS != nil &&
		*filter.MinDurationNS > *filter.MaxDurationNS {
		return nil, ValidationError("min_duration_ns must not be greater than max_duration_ns")
	}
	if raw, present := query.Get("complete"), query.Has("complete"); present {
		value, err := parseBoolean(raw, "complete")
		if err != nil {
			return nil, err
		}
		filter.Complete = &value
	}
	if raw, present := query.Get("valid"), query.Has("valid"); present {
		value, err := parseBoolean(raw, "valid")
		if err != nil {
			return nil, err
		}
		filter.Valid = &value
	}
	if query.Has("service_path") {
		path, err := parseServicePath(query["service_path"])
		if err != nil {
			return nil, err
		}
		filter.ServicePath = path
	}
	return filter, nil
}

const (
	// maxServicePathItems bounds the number of service_path elements and
	// maxServicePathName the length of each name.
	maxServicePathItems = 32
	maxServicePathName  = 128
)

// parseServicePath validates the repeated service_path parameter. Names are
// kept verbatim because matching is exact, so no whitespace is trimmed.
func parseServicePath(names []string) ([]string, error) {
	if len(names) < 2 || len(names) > maxServicePathItems {
		return nil, ValidationError("service_path must contain between 2 and %d service names", maxServicePathItems)
	}
	for index, name := range names {
		if name == "" {
			return nil, ValidationError("service_path names must not be empty")
		}
		if len(name) > maxServicePathName {
			return nil, ValidationError("service_path names must be at most %d characters", maxServicePathName)
		}
		if index > 0 && names[index-1] == name {
			return nil, ValidationError("service_path names must differ from their neighbour")
		}
	}
	return names, nil
}

// parseDurationBound accepts a decimal non-negative integer with no sign,
// exponent or surrounding space.
func parseDurationBound(raw string, field string) (int64, error) {
	if raw == "" || strings.TrimSpace(raw) == "" {
		return 0, ValidationError("%s must be a decimal non-negative integer", field)
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, ValidationError("%s must be a decimal non-negative integer", field)
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, ValidationError("%s must be a decimal non-negative integer", field)
	}
	return value, nil
}

// parseBoolean accepts exactly "true" or "false".
func parseBoolean(raw string, field string) (bool, error) {
	switch raw {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, ValidationError("%s must be true or false", field)
	}
}

// matches reports whether one reassembled trace satisfies every set condition.
func (f *TraceFilter) matches(trace *Trace) bool {
	if f.Service != "" {
		found := false
		for _, node := range trace.Spans {
			if node.Service == f.Service {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.Operation != "" {
		found := false
		for _, node := range trace.Spans {
			if node.Operation == f.Operation {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.Query != "" {
		if !containsFold(trace.TraceID, f.Query) {
			found := false
			for _, node := range trace.Spans {
				if containsFold(node.Service, f.Query) || containsFold(node.Operation, f.Query) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	switch f.Status {
	case "error":
		if trace.ErrorSpans == 0 {
			return false
		}
	case "ok":
		if trace.ErrorSpans != 0 {
			return false
		}
	}
	if f.Complete != nil && trace.Complete != *f.Complete {
		return false
	}
	if f.Valid != nil && trace.Valid != *f.Valid {
		return false
	}
	if len(f.ServicePath) > 0 && !matchServicePath(trace.Spans, f.ServicePath) {
		return false
	}
	if f.StartFrom != nil || f.StartTo != nil || f.MinDurationNS != nil || f.MaxDurationNS != nil {
		// Every time and duration bound refers to the root. A trace without a
		// root has neither a root instant nor a root duration, so any such
		// bound excludes it.
		if trace.Root == nil {
			return false
		}
	}
	if f.StartFrom != nil || f.StartTo != nil {
		rootStart, err := parseTime(trace.Root.StartTime, "start_time")
		if err != nil {
			return false
		}
		if f.StartFrom != nil && rootStart.Before(*f.StartFrom) {
			return false
		}
		if f.StartTo != nil && !rootStart.Before(*f.StartTo) {
			return false
		}
	}
	if f.MinDurationNS != nil && trace.DurationNS < *f.MinDurationNS {
		return false
	}
	if f.MaxDurationNS != nil && trace.DurationNS > *f.MaxDurationNS {
		return false
	}
	return true
}

// matchServicePath reports whether the reassembled span tree contains a chain
// of direct parent/child spans whose services equal path element by element.
// The chain may start at any span; only the stored parent/child links are
// consulted, so an incomplete or invalid trace is judged on the spans it has.
func matchServicePath(nodes []*TraceNode, path []string) bool {
	for _, node := range nodes {
		if matchServicePathFrom(node, path, 0) {
			return true
		}
	}
	return false
}

func matchServicePathFrom(node *TraceNode, path []string, index int) bool {
	if node.Service != path[index] {
		return false
	}
	if index == len(path)-1 {
		return true
	}
	for _, child := range node.Children {
		if matchServicePathFrom(child, path, index+1) {
			return true
		}
	}
	return false
}

// ListTraces summarises every stored trace with at least one span.
func (s *Service) ListTraces(filter *TraceFilter, limit int) (any, error) {
	if limit < 1 || limit > 400 {
		return nil, ValidationError("limit must be between 1 and 400")
	}
	if filter == nil {
		filter = &TraceFilter{}
	}
	value, err := s.store.View(func(state *State) (any, error) {
		summaries := []*TraceSummary{}
		for _, traceID := range state.traceIDs() {
			trace := assembleTrace(traceID, state.Spans[traceID])
			if !filter.matches(trace) {
				continue
			}
			summary := &TraceSummary{
				TraceID:      trace.TraceID,
				StartTime:    "",
				DurationNS:   trace.DurationNS,
				SpanCount:    trace.SpanCount,
				ServiceCount: trace.ServiceCount,
				ErrorSpans:   trace.ErrorSpans,
				Complete:     trace.Complete,
				Valid:        trace.Valid,
			}
			if trace.Root != nil {
				rootID := trace.Root.SpanID
				rootService := trace.Root.Service
				summary.RootSpanID = &rootID
				summary.RootService = &rootService
				summary.StartTime = trace.Root.StartTime
				summary.rootInstant, _ = parseTime(trace.Root.StartTime, "start_time")
			}
			summaries = append(summaries, summary)
		}
		sort.Slice(summaries, func(i, j int) bool {
			if !summaries[i].rootInstant.Equal(summaries[j].rootInstant) {
				return summaries[i].rootInstant.After(summaries[j].rootInstant)
			}
			return summaries[i].TraceID < summaries[j].TraceID
		})
		if len(summaries) > limit {
			summaries = summaries[:limit]
		}
		return map[string]any{"traces": summaries}, nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// CriticalPath walks the root-to-leaf path with the largest total self_time. It
// therefore needs time containment to hold, exactly like the invariant
// "critical path length equals the root span duration" does.
func (s *Service) CriticalPath(traceID string) (any, error) {
	value, err := s.store.View(func(state *State) (any, error) {
		spans, found := state.Spans[traceID]
		if !found {
			return nil, NotFoundError("trace %s does not exist", traceID)
		}
		return assembleTrace(traceID, spans), nil
	})
	if err != nil {
		return nil, err
	}
	trace := value.(*Trace)
	path := &CriticalPath{
		TraceID:         traceID,
		Method:          "longest chain below the root, weighted by self_time_ns; ties are broken by the longest sub-chain and then by the smallest span id sequence",
		TraceDurationNS: trace.DurationNS,
		Path:            []PathStep{},
		Complete:        trace.Complete,
		Valid:           trace.Valid || trace.Root == nil,
		Violations:      publicViolations(trace.Violations),
	}
	if !trace.Complete {
		path.Valid = false
	}
	if trace.Root == nil {
		return path, nil
	}
	_, steps := longestSelfTimePath(trace.Root)
	path.Path = steps
	for _, step := range steps {
		path.LengthNS += step.SelfTimeNS
	}
	return path, nil
}

type pathCandidate struct {
	// weight is the sum of self_time along the candidate: the value the response
	// reports as the length of the path.
	weight int64
	// path is the longest self_time-weighted path of the subtree, used to break a
	// tie between two branches that weigh the same.
	path  int64
	steps int
	ids   []string
}

// better ranks two candidates of the same node. More self_time wins. An equal
// weight is settled by the branch with the longer critical path, then by the
// shorter path, then by the lexicographically smallest span id sequence, so the
// output is deterministic.
func better(candidate pathCandidate, incumbent pathCandidate) bool {
	if candidate.weight != incumbent.weight {
		return candidate.weight > incumbent.weight
	}
	if candidate.path != incumbent.path {
		return candidate.path > incumbent.path
	}
	if candidate.steps != incumbent.steps {
		return candidate.steps < incumbent.steps
	}
	for index := 0; index < len(candidate.ids) && index < len(incumbent.ids); index++ {
		if candidate.ids[index] != incumbent.ids[index] {
			return candidate.ids[index] < incumbent.ids[index]
		}
	}
	return len(candidate.ids) < len(incumbent.ids)
}

// longestSelfTimePath returns the winning candidate of the subtree together
// with the steps of the critical path, both in pre-order. The tree builder has
// already computed maxPath for every span, so the winner is the child whose
// maxPath is the largest and the weight of the result is this node's maxPath.
func longestSelfTimePath(node *TraceNode) (pathCandidate, []PathStep) {
	self := PathStep{
		SpanID:     node.SpanID,
		ParentID:   node.ParentID,
		Service:    node.Service,
		Operation:  node.Operation,
		Depth:      node.Depth,
		SelfTimeNS: node.SelfTimeNS,
		StartTime:  node.StartTime,
		EndTime:    node.EndTime,
	}
	best := pathCandidate{weight: node.SelfTimeNS, path: node.maxPath, steps: 1, ids: []string{node.SpanID}}
	var bestSteps []PathStep
	for _, child := range node.Children {
		candidate, steps := longestSelfTimePath(child)
		candidate.weight += node.SelfTimeNS
		candidate.steps++
		candidate.ids = append([]string{node.SpanID}, candidate.ids...)
		if bestSteps == nil || better(candidate, best) {
			best = candidate
			bestSteps = steps
		}
	}
	if bestSteps == nil {
		bestSteps = []PathStep{}
	}
	steps := append([]PathStep{self}, bestSteps...)
	weight := int64(0)
	for _, item := range steps {
		weight += item.SelfTimeNS
	}
	best.weight = weight
	if best.weight < node.maxPath {
		best.weight = node.maxPath
	}
	return best, steps
}

// ServiceGraph aggregates the caller/callee graph. traceID limits the analysis
// to one trace; an empty traceID covers every stored trace.
func (s *Service) ServiceGraph(traceID string) (any, error) {
	value, err := s.store.View(func(state *State) (any, error) {
		graph := &ServiceGraph{
			Services: []*ServiceStat{},
			Edges:    []*ServiceEdge{},
			Stats:    &GraphStats{},
		}
		ids := state.traceIDs()
		if traceID != "" {
			if _, found := state.Spans[traceID]; !found {
				return nil, NotFoundError("trace %s does not exist", traceID)
			}
			ids = []string{traceID}
			// The response always echoes an explicitly requested trace.
			graph.TraceID = &ids[0]
		}
		services := map[string]*ServiceStat{}
		edges := map[string]*ServiceEdge{}
		graph.Stats.durations = []int64{}
		for _, id := range ids {
			trace := assembleTrace(id, state.Spans[id])
			usable := trace.Valid && trace.Complete && trace.Root != nil
			if !usable {
				if traceID != "" {
					// An explicitly requested trace is reported as-is, complete
					// or not.
					graph.Stats.PartialTraces = 1
					return graph, nil
				}
				graph.Stats.PartialTraces++
				continue
			}
			graph.Stats.TraceCount++
			graph.Stats.SpanCount += trace.SpanCount
			graph.Stats.ErrorSpans += trace.ErrorSpans
			graph.Stats.durations = append(graph.Stats.durations, trace.DurationNS)
			for _, node := range trace.Spans {
				stat := services[node.Service]
				if stat == nil {
					stat = &ServiceStat{Service: node.Service, Operations: []*OperationStat{}, durations: []int64{}}
					services[node.Service] = stat
				}
				stat.SpanCount++
				stat.SelfTimeNS += node.SelfTimeNS
				stat.durations = append(stat.durations, node.DurationNS)
				if node.Error {
					stat.ErrorSpans++
				}
				if node.ParentID == nil {
					stat.RootSpans++
				}
				operation := operationStat(stat, node.Operation)
				operation.SpanCount++
				operation.SelfTimeNS += node.SelfTimeNS
				operation.durations = append(operation.durations, node.DurationNS)
				if node.Error {
					operation.ErrorSpans++
				}
			}
			byID := map[string]*TraceNode{}
			for _, node := range trace.Spans {
				byID[node.SpanID] = node
			}
			for _, node := range trace.Spans {
				if node.ParentID == nil {
					continue
				}
				parent := byID[*node.ParentID]
				if parent == nil || parent.Service == node.Service {
					continue
				}
				key := parent.Service + "\x00" + node.Service
				edge := edges[key]
				if edge == nil {
					edge = &ServiceEdge{Caller: parent.Service, Callee: node.Service, durations: []int64{}}
					edges[key] = edge
				}
				edge.CallCount++
				edge.durations = append(edge.durations, node.DurationNS)
				if node.Error {
					edge.ErrorCount++
				}
			}
		}
		for _, stat := range services {
			finalizeService(stat)
			graph.Services = append(graph.Services, stat)
		}
		for _, edge := range edges {
			finalizeEdge(edge)
			graph.Edges = append(graph.Edges, edge)
		}
		sort.Slice(graph.Services, func(i, j int) bool { return graph.Services[i].Service < graph.Services[j].Service })
		sort.Slice(graph.Edges, func(i, j int) bool {
			if graph.Edges[i].Caller != graph.Edges[j].Caller {
				return graph.Edges[i].Caller < graph.Edges[j].Caller
			}
			return graph.Edges[i].Callee < graph.Edges[j].Callee
		})
		finalizeStats(graph.Stats)
		return graph, nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func operationStat(service *ServiceStat, operation string) *OperationStat {
	for _, stat := range service.Operations {
		if stat.Operation == operation {
			return stat
		}
	}
	stat := &OperationStat{Operation: operation, durations: []int64{}}
	service.Operations = append(service.Operations, stat)
	return stat
}

func finalizeService(stat *ServiceStat) {
	sort.Slice(stat.durations, func(i, j int) bool { return stat.durations[i] < stat.durations[j] })
	stat.MinDurationNS, stat.MaxDurationNS = bounds(stat.durations)
	stat.MeanDurationNS = mean(stat.durations)
	stat.P50DurationNS = percentile(stat.durations, 50)
	stat.P95DurationNS = percentile(stat.durations, 95)
	stat.durations = nil
	sort.Slice(stat.Operations, func(i, j int) bool { return stat.Operations[i].Operation < stat.Operations[j].Operation })
	for _, operation := range stat.Operations {
		finalizeOperation(operation)
	}
}

func finalizeOperation(stat *OperationStat) {
	sort.Slice(stat.durations, func(i, j int) bool { return stat.durations[i] < stat.durations[j] })
	stat.MinDurationNS, stat.MaxDurationNS = bounds(stat.durations)
	stat.MeanDurationNS = mean(stat.durations)
	stat.P50DurationNS = percentile(stat.durations, 50)
	stat.P95DurationNS = percentile(stat.durations, 95)
	stat.durations = nil
}

func finalizeEdge(edge *ServiceEdge) {
	sort.Slice(edge.durations, func(i, j int) bool { return edge.durations[i] < edge.durations[j] })
	edge.MinDurationNS, edge.MaxDurationNS = bounds(edge.durations)
	edge.MeanDurationNS = mean(edge.durations)
	edge.P50DurationNS = percentile(edge.durations, 50)
	edge.P95DurationNS = percentile(edge.durations, 95)
	edge.durations = nil
}

func finalizeStats(stats *GraphStats) {
	sort.Slice(stats.durations, func(i, j int) bool { return stats.durations[i] < stats.durations[j] })
	stats.DurationNS, _ = bounds(stats.durations)
	stats.MeanDurationNS = mean(stats.durations)
	stats.P50DurationNS = percentile(stats.durations, 50)
	stats.P95DurationNS = percentile(stats.durations, 95)
	stats.durations = nil
}

func bounds(sorted []int64) (int64, int64) {
	if len(sorted) == 0 {
		return 0, 0
	}
	return sorted[0], sorted[len(sorted)-1]
}

func mean(sorted []int64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	total := int64(0)
	for _, value := range sorted {
		total += value
	}
	return total / int64(len(sorted))
}

// percentile uses the nearest-rank definition on already sorted input:
// index = ceil(p/100 * n) - 1. Input is never mutated.
func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func sortStrings(values []string) {
	sort.Strings(values)
}
