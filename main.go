package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"tracepath/internal/tracepath"
)

func main() {
	host := flag.String("host", "127.0.0.1", "interface to bind")
	port := flag.Int("port", 8080, "TCP port to bind")
	database := flag.String("database", "tracepath.db", "path of the JSON database file")
	flag.Parse()

	store, err := tracepath.OpenStore(*database)
	if err != nil {
		fail(err)
	}
	service, err := tracepath.NewService(store, nil)
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
