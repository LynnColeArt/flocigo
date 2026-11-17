package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Errors returned by Verify.
var (
	ErrNoAuthorization      = errors.New("sigv4: missing Authorization header")
	ErrUnsupportedAlgorithm = errors.New("sigv4: unsupported signing scheme")
	ErrMalformedCredential  = errors.New("sigv4: malformed Credential element")
	ErrMissingXAmzDate      = errors.New("sigv4: missing x-amz-date")
	ErrUnknownAccessKey     = errors.New("sigv4: unknown access key id")
	ErrSignatureMismatch    = errors.New("sigv4: signature mismatch")
)

// Resolver returns the secret access key for the access key id taken from a
// request's Credential element.
type Resolver func(accessKeyID string) (secret string, ok bool)

// Verify authenticates a header-based SigV4-signed request.
//
// The declared SignedHeaders list in the Authorization header is
// authoritative: the canonical request is rebuilt from exactly those header
// names by the same builder Sign uses, so header inclusion (e.g. content-type)
// can never diverge between the signing and verification paths.
//
// body is the raw payload; it is only hashed when the request does not carry
// an x-amz-content-sha256 header (S3 requests always carry one, possibly
// UNSIGNED-PAYLOAD or a streaming constant, which are used verbatim).
func Verify(r *http.Request, body []byte, resolve Resolver) error {
	credential, signedHeaders, signature, err := parseAuthorization(r.Header.Get("Authorization"))
	if err != nil {
		return err
	}

	accessKey, scope, err := splitCredential(credential)
	if err != nil {
		return err
	}
	secret, ok := resolve(accessKey)
	if !ok {
		return ErrUnknownAccessKey
	}
	date, region, service, err := splitScope(scope)
	if err != nil {
		return err
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = basicISODate(r.Header.Get("Date"))
	}
	if amzDate == "" {
		return ErrMissingXAmzDate
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = HashPayload(body)
	}

	canonical := CanonicalRequest(r, strings.Split(signedHeaders, ";"), payloadHash)
	stringToSign := StringToSign(amzDate, scope, canonical)
	want := HMACSHA256(SigningKey(secret, date, region, service), []byte(stringToSign))

	got, err := hex.DecodeString(signature)
	if err != nil || len(got) != sha256.Size {
		return fmt.Errorf("sigv4: malformed signature: %w", ErrMalformedCredential)
	}
	if !hmac.Equal(got, want) {
		return ErrSignatureMismatch
	}
	return nil
}

// parseAuthorization extracts the Credential, SignedHeaders and Signature
// elements from an "AWS4-HMAC-SHA256 K=V, K=V, K=V" header, tolerating
// arbitrary spacing.
func parseAuthorization(header string) (credential, signedHeaders, signature string, err error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", "", "", ErrNoAuthorization
	}
	sp := strings.IndexByte(header, ' ')
	if sp < 0 || !strings.EqualFold(strings.TrimSpace(header[:sp]), Algorithm) {
		return "", "", "", ErrUnsupportedAlgorithm
	}
	for _, element := range strings.Split(header[sp+1:], ",") {
		element = strings.TrimSpace(element)
		if element == "" {
			continue
		}
		eq := strings.IndexByte(element, '=')
		if eq < 0 {
			return "", "", "", ErrMalformedCredential
		}
		key := strings.ToLower(strings.TrimSpace(element[:eq]))
		value := strings.TrimSpace(element[eq+1:])
		switch key {
		case "credential":
			credential = value
		case "signedheaders":
			signedHeaders = value
		case "signature":
			signature = value
		}
	}
	if credential == "" || signedHeaders == "" || signature == "" {
		return "", "", "", ErrMalformedCredential
	}
	return credential, signedHeaders, signature, nil
}

// splitCredential splits "<access-key-id>/<scope>" at the first '/', which
// cannot occur inside an access key id.
func splitCredential(credential string) (accessKey, scope string, err error) {
	i := strings.IndexByte(credential, '/')
	if i <= 0 || i == len(credential)-1 {
		return "", "", ErrMalformedCredential
	}
	return credential[:i], credential[i+1:], nil
}

// splitScope validates and splits "date/region/service/aws4_request".
func splitScope(scope string) (date, region, service string, err error) {
	parts := strings.Split(scope, "/")
	if len(parts) != 4 || parts[3] != Terminator || parts[0] == "" || parts[2] == "" {
		return "", "", "", ErrMalformedCredential
	}
	return parts[0], parts[1], parts[2], nil
}

// basicISODate normalizes a Date header to the SigV4 basic ISO 8601 form;
// it returns "" when the value is absent or unparsable.
func basicISODate(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if t, err := time.Parse(TimeFormat, v); err == nil {
		return t.UTC().Format(TimeFormat)
	}
	if t, err := http.ParseTime(v); err == nil {
		return t.UTC().Format(TimeFormat)
	}
	return ""
}
