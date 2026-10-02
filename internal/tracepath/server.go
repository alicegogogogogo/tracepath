package tracepath

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Server adapts a Service onto net/http. It owns routing, request decoding and
// the documented error shape; every rule lives in the service layer.
type Server struct {
	service *Service
}

// NewServer builds the HTTP handler for one service.
func NewServer(service *Service) *Server {
	return &Server{service: service}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	status, response, err := s.dispatch(request, splitPath(request.URL.Path))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, status, response)
}

func (s *Server) dispatch(request *http.Request, parts []string) (int, any, error) {
	method := request.Method
	switch {
	case method == http.MethodGet && len(parts) == 1 && parts[0] == "health":
		if err := requireQuery(request); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"status": "ok"}, nil

	case method == http.MethodPost && len(parts) == 1 && parts[0] == "spans":
		body, err := readJSONBody(request)
		if err != nil {
			return 0, nil, err
		}
		response, err := s.service.IngestSpan(body, request.Header.Get("Idempotency-Key"))
		return http.StatusCreated, response, err

	case method == http.MethodGet && len(parts) == 1 && parts[0] == "traces":
		filter, err := parseTraceFilter(request)
		if err != nil {
			return 0, nil, err
		}
		response, err := s.service.ListTraces(filter)
		return http.StatusOK, response, err

	case method == http.MethodGet && len(parts) == 2 && parts[0] == "traces":
		if err := requireQuery(request); err != nil {
			return 0, nil, err
		}
		response, err := s.service.GetTrace(parts[1])
		return http.StatusOK, response, err

	case method == http.MethodGet && len(parts) == 3 && parts[0] == "traces" && parts[2] == "critical-path":
		if err := requireQuery(request); err != nil {
			return 0, nil, err
		}
		response, err := s.service.CriticalPath(parts[1])
		return http.StatusOK, response, err

	case method == http.MethodGet && len(parts) == 2 && parts[0] == "services" && parts[1] == "graph":
		if err := requireQuery(request, "trace_id"); err != nil {
			return 0, nil, err
		}
		response, err := s.service.ServiceGraph(strings.TrimSpace(request.URL.Query().Get("trace_id")))
		return http.StatusOK, response, err
	}
	return 0, nil, NotFoundError("route was not found")
}

// traceFilterParams is the closed set of query parameters GET /traces accepts.
var traceFilterParams = []string{
	"service", "limit", "q", "operation", "status",
	"start_from", "start_to", "min_duration_ns", "max_duration_ns",
	"complete", "valid",
}

// parseTraceFilter decodes and validates every GET /traces query parameter.
// A parameter that is present but malformed is a validation error; a parameter
// that is absent simply leaves the filter field unset.
func parseTraceFilter(request *http.Request) (TraceFilter, error) {
	filter := TraceFilter{Limit: 100}
	if err := requireQuery(request, traceFilterParams...); err != nil {
		return filter, err
	}
	query := request.URL.Query()
	filter.Service = strings.TrimSpace(query.Get("service"))
	filter.Query = query.Get("q")
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return filter, ValidationError("limit must be an integer")
		}
		filter.Limit = parsed
	}
	if _, present := query["operation"]; present {
		operation := strings.TrimSpace(query.Get("operation"))
		if operation == "" {
			return filter, ValidationError("operation must not be empty")
		}
		filter.Operation = operation
	}
	if _, present := query["status"]; present {
		status := query.Get("status")
		if status != "ok" && status != "error" {
			return filter, ValidationError("status must be ok or error")
		}
		filter.Status = status
	}
	if _, present := query["start_from"]; present {
		instant, err := parseTime(query.Get("start_from"), "start_from")
		if err != nil {
			return filter, err
		}
		filter.StartFrom = &instant
	}
	if _, present := query["start_to"]; present {
		instant, err := parseTime(query.Get("start_to"), "start_to")
		if err != nil {
			return filter, err
		}
		filter.StartTo = &instant
	}
	minimum, err := parseDurationBound(query, "min_duration_ns")
	if err != nil {
		return filter, err
	}
	filter.MinDurationNS = minimum
	maximum, err := parseDurationBound(query, "max_duration_ns")
	if err != nil {
		return filter, err
	}
	filter.MaxDurationNS = maximum
	if minimum != nil && maximum != nil && *minimum > *maximum {
		return filter, ValidationError("min_duration_ns must not exceed max_duration_ns")
	}
	complete, err := parseBoolParam(query, "complete")
	if err != nil {
		return filter, err
	}
	filter.Complete = complete
	valid, err := parseBoolParam(query, "valid")
	if err != nil {
		return filter, err
	}
	filter.Valid = valid
	return filter, nil
}

// parseDurationBound decodes one duration bound: a decimal non-negative
// integer of nanoseconds, nothing else.
func parseDurationBound(query map[string][]string, name string) (*int64, error) {
	if _, present := query[name]; !present {
		return nil, nil
	}
	raw := ""
	if values := query[name]; len(values) > 0 {
		raw = values[0]
	}
	if raw == "" {
		return nil, ValidationError("%s must be a non-negative decimal integer", name)
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return nil, ValidationError("%s must be a non-negative decimal integer", name)
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, ValidationError("%s must be a non-negative decimal integer", name)
	}
	return &value, nil
}

// parseBoolParam decodes one boolean switch that accepts only the literal
// strings true and false.
func parseBoolParam(query map[string][]string, name string) (*bool, error) {
	if _, present := query[name]; !present {
		return nil, nil
	}
	raw := ""
	if values := query[name]; len(values) > 0 {
		raw = values[0]
	}
	switch raw {
	case "true":
		value := true
		return &value, nil
	case "false":
		value := false
		return &value, nil
	}
	return nil, ValidationError("%s must be true or false", name)
}

func readJSONBody(request *http.Request) ([]byte, error) {
	contentType := request.Header.Get("Content-Type")
	mediaType, _, _ := strings.Cut(contentType, ";")
	if strings.TrimSpace(strings.ToLower(mediaType)) != "application/json" {
		return nil, ValidationError("Content-Type must be application/json")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxBodyBytes+1))
	if err != nil {
		return nil, ValidationError("request body could not be read")
	}
	if len(body) > maxBodyBytes {
		return nil, ValidationError("request body must be at most %d bytes", maxBodyBytes)
	}
	return body, nil
}

func splitPath(path string) []string {
	parts := []string{}
	for _, part := range strings.Split(path, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

// requireQuery rejects every query parameter that the route does not document.
func requireQuery(request *http.Request, allowed ...string) error {
	names := make([]string, 0, len(request.URL.Query()))
	for name := range request.URL.Query() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		known := false
		for _, candidate := range allowed {
			if name == candidate {
				known = true
				break
			}
		}
		if !known {
			return ValidationError("unknown query parameter %s", name)
		}
	}
	return nil
}

// writeError maps a service error onto the documented error body. A trace that
// is stored but violates span invariants additionally carries its violations.
func writeError(writer http.ResponseWriter, err error) {
	body := map[string]any{
		"error": map[string]any{"code": "internal_error", "message": "internal server error"},
	}
	status := http.StatusInternalServerError
	var apiError *Error
	var invalid *TraceInvalidError
	switch {
	case errors.As(err, &invalid):
		status = http.StatusConflict
		body = map[string]any{
			"error": map[string]any{
				"code":       "trace_invalid",
				"message":    invalid.Message,
				"trace_id":   invalid.TraceID,
				"violations": publicViolations(invalid.Violations),
			},
		}
	case errors.As(err, &apiError):
		status = apiError.Status
		body = map[string]any{
			"error": map[string]any{"code": apiError.Code, "message": apiError.Message},
		}
	}
	writeJSON(writer, status, body)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		encoded = []byte(`{"error":{"code":"internal_error","message":"internal server error"}}`)
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	writer.WriteHeader(status)
	writer.Write(encoded)
}
