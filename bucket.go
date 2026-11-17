package main

import (
    "errors"
    "io/fs"
    "net/http"
    "os"
    "strings"
)

// --- bucket name validation -----------------------------------------------

// validateBucketName reports whether name satisfies the S3 bucket naming
// rules floci enforces:
//
//   - 3 to 63 characters long,
//   - only lowercase letters, digits, dots and hyphens,
//   - starts and ends with a letter or digit,
//   - no two adjacent dots,
//   - not formatted like an IPv4 address.
func validateBucketName(name string) bool {
    if len(name) < 3 || len(name) > 63 {
        return false
    }
    for i := 0; i < len(name); i++ {
        if !isLowerAlnum(name[i]) && name[i] != '-' && name[i] != '.' {
            return false
        }
    }
    if !isLowerAlnum(name[0]) || !isLowerAlnum(name[len(name)-1]) {
        return false
    }
    if strings.Contains(name, "..") {
        return false
    }
    return !ipv4Shaped(name)
}

// isLowerAlnum reports whether c is a lowercase letter or a digit.
func isLowerAlnum(c byte) bool {
    return ('a' <= c && c <= 'z') || ('0' <= c && c <= '9')
}

// ipv4Shaped reports whether name is formatted like an IPv4 address
// ("1.2.3.4"), which S3 forbids as a bucket name.
func ipv4Shaped(name string) bool {
    parts := strings.Split(name, ".")
    if len(parts) != 4 {
        return false
    }
    for _, p := range parts {
        if n, ok := ipv4Part(p); !ok || n > 255 || (len(p) == 3 && p[0] == '0') {
            return false
        }
    }
    return true
}

// ipv4Part parses one dotted-quad component and reports whether it is
// IP-shaped at all.
func ipv4Part(p string) (int, bool) {
    if len(p) == 0 || len(p) > 3 {
        return 0, false
    }
    n := 0
    for i := 0; i < len(p); i++ {
        if p[i] < '0' || p[i] > '9' {
            return 0, false
        }
        n = n*10 + int(p[i]-'0')
    }
    return n, true
}

// ValidateBucketName is the exported form used outside this file: nil when
// name is valid, the InvalidBucketName S3 error otherwise.
func ValidateBucketName(name string) error {
    if validateBucketName(name) {
        return nil
    }
    return ErrInvalidBucketName
}

// --- CreateBucket (PUT /{bucket}) ------------------------------------------

// CreateBucket backs a bucket with a directory under the data root.
//
// floci is single-tenant: every bucket that already exists is owned by you,
// which maps to the idempotent-success code BucketAlreadyOwnedByYou.
func (s *Server) CreateBucket(name string) error {
    if !validateBucketName(name) {
        return ErrInvalidBucketName
    }
    if err := s.ensureRoot(); err != nil {
        return ErrInternalError
    }
    if err := os.Mkdir(s.bucketDir(name), 0o755); err != nil {
        if errors.Is(err, fs.ErrExist) {
            return ErrBucketAlreadyOwnedByYou
        }
        return ErrInternalError
    }
    return nil
}

// handlePutBucket implements PUT /{bucket} (CreateBucket).
//
// Responses:
//
//    200  bucket created, or it already existed (BucketAlreadyOwnedByYou is
//         folded into an idempotent success); Location points at the bucket
//    400  InvalidBucketName      XML error document
//    500  InternalError          XML error document
func (s *Server) handlePutBucket(w http.ResponseWriter, r *http.Request) {
    bucket, rest := splitRequestPath(r.URL.Path)
    if rest != "" {
        replyError(w, r, ErrMethodNotAllowed)
        return
    }

    switch err := s.CreateBucket(bucket); {
    case err == nil, errors.Is(err, ErrBucketAlreadyOwnedByYou):
        setBasicHeaders(w)
        w.Header().Set("Location", "/"+bucket)
        w.WriteHeader(http.StatusOK)
    case errors.Is(err, ErrInvalidBucketName):
        replyError(w, r, ErrInvalidBucketName)
    default:
        replyError(w, r, ErrInternalError)
    }
}
