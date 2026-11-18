// Command floci-go is a single-binary DynamoDB emulator speaking the DynamoDB
// JSON protocol (x-amz-json-1.0, requests routed by the X-Amz-Target header).
//
// ticket-012: CreateTable persists a table's pk/sk key schema to a JSON store
// file and responds with a TableDescription whose TableStatus is ACTIVE.
// Re-creating an existing table name fails with ResourceInUseException
// (HTTP 400), including across process restarts.
package main

import (
    "log"
    "net/http"
    "os"
)

const defaultStorePath = "floci.json"

func envOr(keys []string, fallback string) string {
    for _, k := range keys {
        if v := os.Getenv(k); v != "" {
            return v
        }
    }
    return fallback
}

func main() {
    port := envOr([]string{"FLOCI_PORT", "PORT"}, "8000")
    storePath := envOr([]string{"FLOCI_STORE", "FLOCI_STORE_PATH"}, defaultStorePath)
    region := envOr([]string{"FLOCI_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"}, "us-east-1")
    accountID := envOr([]string{"FLOCI_ACCOUNT_ID"}, "000000000000")

    store, err := OpenStore(storePath)
    if err != nil {
        log.Fatalf("floci-go: %v", err)
    }

    srv := NewServer(store, region, accountID)
    log.Printf("floci-go: listening on http://localhost:%s/ (region: %s, account: %s, store: %s)", port, region, accountID, storePath)
    if err := http.ListenAndServe(":"+port, srv); err != nil {
        log.Fatal(err)
    }
}
