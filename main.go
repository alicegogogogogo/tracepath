package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"

	"tracepath/internal/tracepath"
)

func main() {
	host := flag.String("host", "127.0.0.1", "interface to bind")
	port := flag.Int("port", 8080, "TCP port to bind")
	database := flag.String("database", "tracepath.db", "path of the JSON database file")
	clockSkewTolerance := flag.String("clock-skew-tolerance-ns", "0",
		"nanoseconds of parent/child clock skew tolerated when judging time containment (non-negative decimal int64)")
	flag.Parse()

	tolerance, err := parseClockSkewTolerance(*clockSkewTolerance)
	if err != nil {
		fail(err)
	}

	store, err := tracepath.OpenStore(*database)
	if err != nil {
		fail(err)
	}
	service, err := tracepath.NewServiceWithClockSkewTolerance(store, nil, tolerance)
	if err != nil {
		fail(err)
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		fail(err)
	}

	fmt.Printf("TracePath listening on http://%s:%d\n", *host, *port)
	server := &http.Server{Handler: tracepath.NewServer(service)}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "tracepath: %s\n", err)
	os.Exit(1)
}

// parseClockSkewTolerance accepts a non-negative decimal int64 of nanoseconds.
// Anything else (a sign-only string, a non-decimal value, a fraction, an
// out-of-range number, a negative number) stops the process before it listens.
func parseClockSkewTolerance(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("clock-skew-tolerance-ns must be a non-negative decimal integer: %q", raw)
	}
	if value < 0 {
		return 0, errors.New("clock-skew-tolerance-ns must be non-negative")
	}
	return value, nil
}
