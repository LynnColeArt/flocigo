package eventbridge

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// busItem is the ListEventBuses result item shape.
type busItem struct {
	Arn              string `json:"Arn"`
	CreationTime     int64  `json:"CreationTime,omitempty"`
	LastModifiedTime int64  `json:"LastModifiedTime,omitempty"`
	Name             string `json:"Name"`
}

// opCreateEventBus implements CreateEventBus.
func (s *Service) opCreateEventBus(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		Name string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	if !validBusName(in.Name) {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", "Parameter Name is not valid.")
		return
	}
	if _, ok := s.buses[in.Name]; ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceAlreadyExistsException", fmt.Sprintf("Event bus %s already exists.", in.Name))
		return
	}
	now := time.Now().Unix()
	rec := &busRec{
		Name:     in.Name,
		Arn:      s.busARN(in.Name),
		Created:  now,
		Modified: now,
		Rules:    map[string]*ruleRec{},
	}
	s.buses[in.Name] = rec
	s.save()
	writeWireJSON(w, ct, struct {
		EventBusArn string `json:"EventBusArn"`
	}{EventBusArn: rec.Arn})
}

// opDescribeEventBus implements DescribeEventBus; an empty Name selects the
// default event bus.
func (s *Service) opDescribeEventBus(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		Name string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	name := in.Name
	if name == "" {
		name = defaultBusName
	}
	rec, ok := s.buses[name]
	if !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", name))
		return
	}
	writeWireJSON(w, ct, struct {
		Arn              string `json:"Arn"`
		CreationTime     int64  `json:"CreationTime,omitempty"`
		LastModifiedTime int64  `json:"LastModifiedTime,omitempty"`
		Name             string `json:"Name"`
	}{
		Arn:              rec.Arn,
		CreationTime:     rec.Created,
		LastModifiedTime: rec.Modified,
		Name:             rec.Name,
	})
}

// opDeleteEventBus implements DeleteEventBus; deleting a bus drops its
// rules table. The default event bus cannot be deleted.
func (s *Service) opDeleteEventBus(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		Name string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	if !validBusName(in.Name) {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidParameterException", "Parameter Name is not valid.")
		return
	}
	if in.Name == defaultBusName {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidOperationException", "The default event bus cannot be deleted.")
		return
	}
	if _, ok := s.buses[in.Name]; !ok {
		writeWireError(w, ct, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("Event bus %s does not exist.", in.Name))
		return
	}
	delete(s.buses, in.Name)
	s.save()
	writeWireJSON(w, ct, struct{}{})
}

// opListEventBuses implements ListEventBuses with Limit/NextToken paging
// over buses sorted by name.
func (s *Service) opListEventBuses(w http.ResponseWriter, ct string, body []byte) {
	var in struct {
		Limit      float64
		NamePrefix string
		NextToken  string
	}
	if err := decodeWireBody(body, &in); err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "SerializationException", "Failed to parse the request body.")
		return
	}
	limit := pageLimit(in.Limit)
	start, err := decodeNextToken(in.NextToken)
	if err != nil {
		writeWireError(w, ct, http.StatusBadRequest, "InvalidNextTokenException", "NextToken is not valid.")
		return
	}
	names := make([]string, 0, len(s.buses))
	for name := range s.buses {
		if in.NamePrefix == "" || strings.HasPrefix(name, in.NamePrefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if start != "" {
		idx := sort.Search(len(names), func(i int) bool { return names[i] > start })
		names = names[idx:]
	}
	end := limit
	if end > len(names) {
		end = len(names)
	}
	resp := struct {
		EventBuses []busItem `json:"EventBuses"`
		NextToken  string    `json:"NextToken,omitempty"`
	}{EventBuses: make([]busItem, 0, end)}
	for _, name := range names[:end] {
		rec := s.buses[name]
		resp.EventBuses = append(resp.EventBuses, busItem{
			Arn:              rec.Arn,
			CreationTime:     rec.Created,
			LastModifiedTime: rec.Modified,
			Name:             rec.Name,
		})
	}
	if len(names) > limit {
		resp.NextToken = encodeNextToken(names[limit-1])
	}
	writeWireJSON(w, ct, resp)
}

// pageLimit applies the shared ListX Limit semantics: default page size of
// 100 when unset and a hard cap of 100 entries per page.
func pageLimit(limit float64) int {
	if limit < 1 {
		return maxPageSize
	}
	if limit > maxPageSize {
		return maxPageSize
	}
	return int(limit)
}

// NextToken values are opaque base64url continuations naming the last
// returned entry.
func encodeNextToken(after string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(after))
}

func decodeNextToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
