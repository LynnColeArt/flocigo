package sqs

// The CreateQueue action:
//
//   - QueueName is required; request attributes are parsed and
//     validated against the settable vocabulary;
//   - the first creation writes the queue file (atomic create) and
//     answers with the queue's QueueUrl;
//   - re-creating an existing queue is idempotent when its full,
//     normalized attribute set matches: the same QueueUrl is returned
//     and the queue file is left untouched;
//   - differing attributes fail with QueueNameExists and modify
//     nothing.

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// fifoSuffix is the queue-name suffix reserved for FIFO queues.
	fifoSuffix = ".fifo"
	// maxQueueNameLen is the SQS queue-name length limit.
	maxQueueNameLen = 80
)

// CreateQueue implements the CreateQueue action.
func (s *Server) CreateQueue(w http.ResponseWriter, r *http.Request, v url.Values) {
	name := v.Get("QueueName")
	if name == "" {
		writeError(w, errMissingParameter("QueueName"))
		return
	}

	attrs, paramsErr := parseAttributes(v)
	if paramsErr != nil {
		writeError(w, paramsErr)
		return
	}
	tags := parseTags(v)

	if nameErr := validateQueueName(name, attrs); nameErr != nil {
		writeError(w, nameErr)
		return
	}

	now := time.Now().UTC()
	newQueue := &Queue{
		Name:         name,
		Arn:          QueueArn(s.region(), s.accountID(), name),
		AccountID:    s.accountID(),
		Region:       s.region(),
		Attributes:   fillQueueAttributes(attrs),
		Tags:         tags,
		CreatedAt:    now,
		LastModified: now,
	}

	existing, created, err := s.Store.GetOrCreate(newQueue)
	if err != nil {
		writeError(w, errInternalError(err))
		return
	}
	if !created {
		// Idempotency check: the request's full attribute map must
		// match the queue file's state (omitted request attributes
		// already count as their service defaults).
		if diff := queueAttributeDiff(existing.Attributes, newQueue.Attributes); len(diff) > 0 {
			writeError(w, errQueueNameExists(diff))
			return
		}
	}

	writeXML(w, http.StatusOK, &createQueueDocument{
		Xmlns:    sqsNamespace,
		Result:   createQueueResult{QueueURL: s.queueURL(r, name)},
		Metadata: responseMetadata{RequestID: newRequestID()},
	})
}

// validateQueueName applies the SQS queue-name rules: at most 80
// characters of letters, digits, hyphens and underscores, with the
// ".fifo" suffix reserved for queues created with FifoQueue=true.
func validateQueueName(name string, attrs map[string]string) *apiError {
	if len(name) > maxQueueNameLen {
		return errQueueFullNameLengthLimitExceeded(name)
	}

	fifoName := strings.HasSuffix(name, fifoSuffix)
	base := strings.TrimSuffix(name, fifoSuffix)
	if base == "" || !isPlainQueueCharset(base) {
		return errInvalidParameterValue("QueueName",
			"Can only include alphanumeric characters, hyphens, and underscores in queue names.")
	}

	nameWantsFifo := fifoName
	attrWantsFifo := attrs[attrFifoQueue] == "true"
	switch {
	case nameWantsFifo && !attrWantsFifo:
		return errInvalidParameterValue("QueueName",
			fmt.Sprintf("The %s suffix can only be used when the FifoQueue attribute is true.", fifoSuffix))
	case !nameWantsFifo && attrWantsFifo:
		return errInvalidParameterValue("QueueName",
			"FIFO queues must have a queue name that ends in .fifo.")
	}
	return nil
}

// isPlainQueueCharset enforces the queue-name character set.
func isPlainQueueCharset(name string) bool {
	for _, r := range name {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z',
			r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
