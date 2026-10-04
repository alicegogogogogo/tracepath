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
	skewTolerance := flag.String("clock-skew-tolerance-ns", "0",
		"fixed parent/child clock skew tolerance in nanoseconds; a non-negative decimal int64 that widens every containment check by the same amount at each end, 0 keeps strict containment")
	flag.Parse()

	// The tolerance is validated before the store is opened and before the
	// port is bound, so an invalid value terminates without a listener or any
	// database side effect.
	tolerance, err := parseClockSkewTolerance(*skewTolerance)
	if err != nil {
		fail(err)
	}

	store, err := tracepath.OpenStore(*database)
	if err != nil {
		fail(err)
	}
	service, err := tracepath.NewServiceWithConfig(store, tracepath.ServiceConfig{
		ClockSkewToleranceNS: tolerance,
	})
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

// parseClockSkewTolerance accepts exactly a decimal int64. A value that parses
// as negative is reported with the dedicated message; non-numeric input and
// values outside the int64 range are rejected as malformed.
func parseClockSkewTolerance(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errors.New("clock-skew-tolerance-ns must be a decimal non-negative int64 nanosecond value")
	}
	if value < 0 {
		return 0, errors.New("clock-skew-tolerance-ns must be non-negative")
	}
	return value, nil
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "tracepath: %s\n", err)
	os.Exit(1)
}
