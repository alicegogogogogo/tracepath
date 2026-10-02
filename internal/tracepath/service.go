package tracepath

import (
	"sort"
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

// runIdempotent executes action at most once per key. The first response is
// re-rendered from committed state for every later request that carries the
// same key and the same resource identity, so a replay is byte-identical by
// construction. Reusing a key for another operation, or for another resource,
// is a conflict.
func (s *Service) runIdempotent(key string, operation string, identity string, action func(state *State) (any, error)) (any, error) {
	if key == "" {
		return nil, ValidationError("Idempotency-Key header is required")
	}
	if len(key) > 200 {
		return nil, ValidationError("Idempotency-Key must be at most 200 characters")
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

// IngestSpan validates and stores one span. Every check that needs the other
// spans of the trace (parent existence, trace id agreement, time containment)
// runs against the committed state under the store lock.
func (s *Service) IngestSpan(body []byte, key string) (any, error) {
	var input SpanInput
	if err := decodeObject(body, &input); err != nil {
		return nil, err
	}
	start, err := validateSpanInput(&input)
	if err != nil {
		return nil, err
	}
	spanID, err := validateIdentifier(input.SpanID, "span_id")
	if err != nil {
		return nil, err
	}
	traceID, err := validateIdentifier(input.TraceID, "trace_id")
	if err != nil {
		return nil, err
	}
	operation := "ingest-span"
	identity := traceID + "/" + spanID

	if _, err := s.runIdempotent(key, operation, identity, func(state *State) (any, error) {
		spans, found := state.Spans[traceID]
		if !found {
			if parentID := input.ParentID; parentID != nil {
				if _, elsewhere := findSpan(state, *parentID); elsewhere != nil {
					return nil, ConflictError("span %s belongs to trace %s but its parent %s belongs to trace %s",
						spanID, traceID, *parentID, *elsewhere)
				}
			}
			spans = map[string]*SpanInput{}
			state.Spans[traceID] = spans
		}
		if _, duplicate := spans[spanID]; duplicate {
			return nil, ConflictError("span %s already exists in trace %s", spanID, traceID)
		}
		resolved, err := lineOf(spans, &input)
		if err != nil {
			return nil, err
		}
		if resolved != traceID {
			return nil, ConflictError("span %s belongs to trace %s but its parent chain resolves to trace %s",
				spanID, traceID, resolved)
		}
		if input.ParentID != nil {
			parent, found := spans[*input.ParentID]
			if found {
				parentStart, _ := parseTime(parent.StartTime, "start_time")
				parentEnd := parentStart.Add(time.Duration(parent.DurationNS))
				end := start.Add(time.Duration(input.DurationNS))
				if start.Before(parentStart) || end.After(parentEnd) {
					return nil, ConflictError("span %s runs from %s to %s which is not contained in parent %s from %s to %s",
						spanID, formatTime(start), formatTime(end), parent.SpanID, parent.StartTime, formatTime(parentEnd))
				}
			}
		}
		clone := input
		clone.Attributes = map[string]string{}
		for attribute, value := range input.Attributes {
			clone.Attributes[attribute] = value
		}
		spans[spanID] = &clone
		return nil, nil
	}); err != nil {
		return nil, err
	}
	return s.store.View(func(state *State) (any, error) {
		return renderSpan(state, traceID, spanID)
	})
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

// TraceFilter carries the validated GET /traces query parameters. A nil
// pointer or an empty string means the parameter was not given, so the zero
// value selects every trace.
type TraceFilter struct {
	Service       string
	Query         string
	Operation     string
	Status        string
	StartFrom     *time.Time
	StartTo       *time.Time
	MinDurationNS *int64
	MaxDurationNS *int64
	Complete      *bool
	Valid         *bool
	Limit         int
}

// matches reports whether one assembled trace survives every active filter.
// All filters combine with logical AND. Time and duration bounds read the root
// span, so a trace without a root cannot satisfy them.
func (f TraceFilter) matches(trace *Trace) bool {
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
	if f.Query != "" {
		needle := strings.ToLower(f.Query)
		found := strings.Contains(strings.ToLower(trace.TraceID), needle)
		for _, node := range trace.Spans {
			if found {
				break
			}
			found = strings.Contains(strings.ToLower(node.Service), needle) ||
				strings.Contains(strings.ToLower(node.Operation), needle)
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
	if f.StartFrom != nil || f.StartTo != nil {
		if trace.Root == nil {
			return false
		}
		start, err := parseTime(trace.Root.StartTime, "start_time")
		if err != nil {
			return false
		}
		if f.StartFrom != nil && start.Before(*f.StartFrom) {
			return false
		}
		if f.StartTo != nil && !start.Before(*f.StartTo) {
			return false
		}
	}
	if f.MinDurationNS != nil || f.MaxDurationNS != nil {
		if trace.Root == nil {
			return false
		}
		if f.MinDurationNS != nil && trace.Root.DurationNS < *f.MinDurationNS {
			return false
		}
		if f.MaxDurationNS != nil && trace.Root.DurationNS > *f.MaxDurationNS {
			return false
		}
	}
	if f.Complete != nil && trace.Complete != *f.Complete {
		return false
	}
	if f.Valid != nil && trace.Valid != *f.Valid {
		return false
	}
	return true
}

// ListTraces summarises every stored trace with at least one span. Filtering
// happens on the assembled traces, before the deterministic sort and the limit.
func (s *Service) ListTraces(filter TraceFilter) (any, error) {
	if filter.Limit < 1 || filter.Limit > 400 {
		return nil, ValidationError("limit must be between 1 and 400")
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
			}
			summaries = append(summaries, summary)
		}
		sort.Slice(summaries, func(i, j int) bool {
			left, _ := parseTime(summaries[i].StartTime, "start_time")
			right, _ := parseTime(summaries[j].StartTime, "start_time")
			if !left.Equal(right) {
				return left.After(right)
			}
			return summaries[i].TraceID < summaries[j].TraceID
		})
		if len(summaries) > filter.Limit {
			summaries = summaries[:filter.Limit]
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
