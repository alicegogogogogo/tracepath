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

	case method == http.MethodPost && len(parts) == 2 && parts[0] == "spans" && parts[1] == "batch":
		body, err := readJSONBody(request)
		if err != nil {
			return 0, nil, err
		}
		response, err := s.service.IngestSpanBatch(body, request.Header.Get("Idempotency-Key"))
		return http.StatusCreated, response, err

	case method == http.MethodPost && len(parts) == 2 && parts[0] == "traces" && parts[1] == "retention":
		body, err := readJSONBody(request)
		if err != nil {
			return 0, nil, err
		}
		response, err := s.service.ApplyRetention(body)
		return http.StatusOK, response, err

	case method == http.MethodGet && len(parts) == 1 && parts[0] == "traces":
		if err := requireQuery(request, "service", "limit", "q", "operation", "status",
			"start_from", "start_to", "min_duration_ns", "max_duration_ns", "complete", "valid",
			"service_path"); err != nil {
			return 0, nil, err
		}
		filter, err := TraceFilterFromQuery(request.URL.Query())
		if err != nil {
			return 0, nil, err
		}
		limit := 100
		if raw := request.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				return 0, nil, ValidationError("limit must be an integer")
			}
			limit = parsed
		}
		response, err := s.service.ListTraces(filter, limit)
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
