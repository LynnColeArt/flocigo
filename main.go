// Command floci-go is a small S3-compatible server.
//
// All state lives in the file system: every bucket is a directory under a
// data root. Requests are routed path-style (/bucket and /bucket/key).
package main

import (
    "flag"
    "log"
    "net/http"
    "os"
    "strings"
)

func main() {
    root := flag.String("root", envOr("FLOCI_ROOT", "data"), "directory under which buckets are stored")
    addr := flag.String("addr", defaultAddr(), "HTTP listen address")
    flag.Parse()

    log.Printf("floci-go: data root %q, listening on %q", *root, *addr)
    log.Fatal(http.ListenAndServe(*addr, NewServer(*root)))
}

// defaultAddr resolves the listen address from the environment, defaulting
// to :8080.
func defaultAddr() string {
    if v := envOr("FLOCI_ADDR", ""); v != "" {
        return v
    }
    if p := envOr("PORT", ""); p != "" && !strings.Contains(p, ":") {
        return ":" + p
    }
    return ":8080"
}

// envOr returns the value of key (trimmed), or def when unset or empty.
func envOr(key, def string) string {
    if v := strings.TrimSpace(os.Getenv(key)); v != "" {
        return v
    }
    return def
}
