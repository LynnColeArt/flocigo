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

	ddb "github.com/LynnColeArt/flocigo/internal/ddb"
	"github.com/LynnColeArt/flocigo/internal/eventbridge"
	"github.com/LynnColeArt/flocigo/internal/kinesis"
	"github.com/LynnColeArt/flocigo/internal/lambda"
	"github.com/LynnColeArt/flocigo/internal/s3"
	"github.com/LynnColeArt/flocigo/internal/s3vectors"
	"github.com/LynnColeArt/flocigo/internal/sqs"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "loopback address to serve on")
	data := flag.String("data-dir", "", "data directory (default: ./floci-go-data)")
	flag.Parse()
	store := s3.NewStoreAt(*data)
	lambdaSvc := lambda.NewService()
	sqsServer := sqs.NewServer(sqs.NewManager())
	vectorSvc := s3vectors.NewService()
	ddbSvc := ddb.NewService(string(*data) + "/ddb/store.json")
	ebSvc := eventbridge.NewService(string(*data) + "/eventbridge/store.json")
	kinSvc := kinesis.NewService(*data)

	mux := http.NewServeMux()
	mux.Handle("/2015-03-31/", lambdaSvc)   // Lambda API (in-process runner execution model)
	mux.Handle("/ddb/", ddbSvc)             // DynamoDB awsjson1.0 surface
	mux.Handle("/eventbridge/", ebSvc)      // EventBridge wire surface (awsjson 1.0)
	mux.Handle("/kinesis/", kinSvc)         // Kinesis awsjson1.1 surface
	mux.Handle("/sqs/", sqsServer)          // SQS query-protocol service
	mux.Handle("/s3vectors/", vectorSvc)    // S3 Vectors (awsJson 1.0)
	mux.Handle("/", store.Router())         // S3 REST/XML
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: *addr, Handler: mux, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second}
	fmt.Fprintf(os.Stderr, "floci-go: serving on %s (data: %s)\n", *addr, *data)
	srv.RegisterOnShutdown(func() {
		_ = lambdaSvc.Close()
		_ = ddbSvc.Close()
		_ = ebSvc.Close()
	})
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
