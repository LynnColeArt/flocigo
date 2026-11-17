package sigv4

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Sign adds a SigV4 Authorization header to r.
//
// payloadHash is the value for x-amz-content-sha256: HashPayload(data),
// EmptySHA256, or UnsignedPayload. An empty value means "empty body".
// If the request already carries X-Amz-Date it is reused; otherwise one is
// generated, so signing is deterministic for a fixed date.
//
// The SignedHeaders list handed to the shared canonical builder is derived
// here (host always; x-amz-* always; content-type/content-md5 only when
// present), and the identical list is written into the Authorization header —
// so the verifier's reconstruction from that list can never disagree with the
// canonicalized headers the signature was computed over (ticket-001).
func Sign(r *http.Request, creds Credentials, region, service, payloadHash string) error {
	if payloadHash == "" {
		payloadHash = EmptySHA256
	}
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = time.Now().UTC().Format(TimeFormat)
		r.Header.Set("X-Amz-Date", amzDate)
	}
	if _, err := time.Parse(TimeFormat, amzDate); err != nil || len(amzDate) != len(TimeFormat) {
		return fmt.Errorf("sigv4: invalid x-amz-date %q", amzDate)
	}

	signedHeaders := deriveSignedHeaders(r)
	canonical := CanonicalRequest(r, signedHeaders, payloadHash)

	scope := strings.Join([]string{amzDate[:8], region, service, Terminator}, "/")
	stringToSign := StringToSign(amzDate, scope, canonical)
	key := SigningKey(creds.SecretAccessKey, amzDate[:8], region, service)
	sig := hex.EncodeToString(HMACSHA256(key, []byte(stringToSign)))

	r.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		Algorithm, creds.AccessKeyID, scope, strings.Join(signedHeaders, ";"), sig))
	return nil
}

var _ = sha256.Size // referenced by verify.go for signature-length checks
