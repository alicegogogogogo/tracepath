package tracepath

import "fmt"

// Error is the single error type that leaves the service layer. The HTTP layer
// maps its Code and Status directly onto the documented error body.
type Error struct {
	Code    string
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Message }

// ValidationError reports a rejected request (HTTP 400).
func ValidationError(format string, arguments ...any) *Error {
	return &Error{Code: "validation_error", Status: 400, Message: fmt.Sprintf(format, arguments...)}
}

// NotFoundError reports a missing resource (HTTP 404).
func NotFoundError(format string, arguments ...any) *Error {
	return &Error{Code: "not_found", Status: 404, Message: fmt.Sprintf(format, arguments...)}
}

// ConflictError reports a state conflict (HTTP 409).
func ConflictError(format string, arguments ...any) *Error {
	return &Error{Code: "conflict", Status: 409, Message: fmt.Sprintf(format, arguments...)}
}

// TraceInvalidError reports a trace that is stored but violates the structural
// invariants of this service (HTTP 409, code trace_invalid). Callers may type
// assert on it to read Violations.
type TraceInvalidError struct {
	TraceID    string
	Message    string
	Violations []Violation
}

func (e *TraceInvalidError) Error() string { return e.Message }

// Validation is a violation as it leaves the service layer.
type Validation struct {
	Code    string `json:"code"`
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	Message string `json:"message"`
}

// InvalidTraceError builds the 409 error returned for a stored but unusable trace.
func InvalidTraceError(traceID string, violations []Violation) *TraceInvalidError {
	if len(violations) == 0 {
		violations = []Violation{{Code: "invalid_trace", TraceID: traceID, Message: "trace is invalid"}}
	}
	return &TraceInvalidError{
		TraceID:    traceID,
		Message:    fmt.Sprintf("trace %s violates span invariants: %s", traceID, violations[0].Message),
		Violations: violations,
	}
}

// InternalError reports a broken internal invariant (HTTP 500).
func InternalError(format string, arguments ...any) *Error {
	return &Error{Code: "internal_error", Status: 500, Message: fmt.Sprintf(format, arguments...)}
}
