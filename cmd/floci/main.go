// floci-go: client-side GO port of the Floci AWS emulator (S3, SQS, DynamoDB).
// One static binary, loopback-only, embeddable as a library.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LynnColeArt/flocigo/internal/lambda"
	"github.com/LynnColeArt/flocigo/internal/s3"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "loopback address to serve on")
	data := flag.String("data-dir", "", "data directory (default: ./floci-go-data)")
	flag.Parse()
	if *data == "" {
		wd, _ := os.Getwd()
		*data = wd + "/floci-go-data"
	}
	store := s3.NewStore(*data)
	lambdaSvc := lambda.NewService()
	mux := http.NewServeMux()
	mux.Handle("/2015-03-31/", lambdaSvc) // Lambda API (in-process runner execution model)
	mux.Handle("/", store.Router())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: *addr, Handler: mux, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second}
	fmt.Fprintf(os.Stderr, "floci-go: serving on %s (data: %s)\n", *addr, *data)
	srv.RegisterOnShutdown(func() { _ = lambdaSvc.Close() })
	e := srv.ListenAndServe()
	if e == http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, "floci-go: stopped")
		return
	}
	_ = ctx
	if e != nil {
		fmt.Fprintln(os.Stderr, "floci-go:", e)
		os.Exit(1)
	}
	_ = stop
}
