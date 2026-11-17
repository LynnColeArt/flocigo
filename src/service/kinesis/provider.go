// Package kinesis implements an in-memory emulation of Amazon Kinesis
// Data Streams for floci-go.
//
// It speaks the AWS "x-amz-json 1.0" wire protocol (the Kinesis_20131202
// X-Amz-Target namespace, see protocol.go) and models streams, shards,
// MD5 partition key routing, sequence numbers, re-sharding and shard
// iterators, closely following the behaviour of the real service and of
// kinesalite (which LocalStack compatible tooling relies on).
//
// The Provider is an http.Handler: mount it at the Kinesis endpoint of
// the floci-go service registry (e.g. under /_aws/kinesis).
package kinesis

import (
	"sync"
	"time"
)

// Provider is the in-memory Kinesis backend. It is safe for concurrent
// use. Construct with NewProvider; the zero value is not usable.
type Provider struct {
	Region    string
	AccountID string

	// Now is the service clock used for stream creation timestamps and
	// record arrival times. Replace it (e.g. in tests) to travel in time.
	Now func() time.Time

	mu      sync.Mutex
	streams map[string]*Stream
	order   []string // stream names in creation order
}

// NewProvider returns a Provider with default region/account settings.
func NewProvider() *Provider {
	return &Provider{
		Region:    "us-east-1",
		AccountID: "000000000000",
		Now:       time.Now,
		streams:   make(map[string]*Stream),
	}
}

// getStream looks up a stream by name. Callers must hold p.mu.
func (p *Provider) getStream(name string) *Stream {
	return p.streams[name]
}

// orderList returns the names of all live streams in creation order.
func (p *Provider) orderList() []string {
	names := make([]string, 0, len(p.streams))
	for _, name := range p.order {
		if _, ok := p.streams[name]; ok {
			names = append(names, name)
		}
	}
	return names
}

// notFound builds the ResourceNotFoundException of the real service.
func (p *Provider) notFound(stream string) *apiError {
	return newErr("ResourceNotFoundException", "Stream %s under account %s not found.", stream, p.AccountID)
}
