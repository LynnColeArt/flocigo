package sqs

// SQS queue attributes: the settable vocabulary, request-side parsing
// and validation, default fill, and the attribute diff that drives
// CreateQueue's idempotency.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Queue attribute names (the settable portion of the SQS vocabulary).
const (
	attrDelaySeconds                  = "DelaySeconds"
	attrMaximumMessageSize            = "MaximumMessageSize"
	attrMessageRetentionPeriod        = "MessageRetentionPeriod"
	attrReceiveMessageWaitTimeSeconds = "ReceiveMessageWaitTimeSeconds"
	attrVisibilityTimeout             = "VisibilityTimeout"
	attrContentBasedDeduplication     = "ContentBasedDeduplication"
	attrFifoQueue                     = "FifoQueue"
	attrSqsManagedSseEnabled          = "SqsManagedSseEnabled"
	attrDeduplicationScope            = "DeduplicationScope"
	attrFifoThroughputLimit           = "FifoThroughputLimit"
	attrKmsDataKeyReusePeriodSeconds  = "KmsDataKeyReusePeriodSeconds"
	attrKmsMasterKeyId                = "KmsMasterKeyId"
	attrRedrivePolicy                 = "RedrivePolicy"
	attrRedriveAllowPolicy            = "RedriveAllowPolicy"
)

// attrSpec describes one settable queue attribute: its default value
// ("" = absent by default) and how request values are validated and
// normalized.
type attrSpec struct {
	def   string
	parse func(attr, value string) (string, *apiError)
}

var attrSpecs = map[string]attrSpec{
	attrDelaySeconds:                  {def: "0", parse: boundedInt(0, 900)},
	attrMaximumMessageSize:            {def: "262144", parse: boundedInt(1024, 262144)},
	attrMessageRetentionPeriod:        {def: "345600", parse: boundedInt(60, 1209600)},
	attrReceiveMessageWaitTimeSeconds: {def: "0", parse: boundedInt(0, 20)},
	attrVisibilityTimeout:             {def: "30", parse: boundedInt(0, 43200)},
	attrKmsDataKeyReusePeriodSeconds:  {def: "300", parse: boundedInt(60, 86400)},
	attrFifoQueue:                     {def: "false", parse: parseBoolValue},
	attrContentBasedDeduplication:     {def: "false", parse: parseBoolValue},
	attrSqsManagedSseEnabled:          {def: "true", parse: parseBoolValue},
	attrDeduplicationScope:            {def: "queue", parse: enumOf("queue", "messageGroup")},
	attrFifoThroughputLimit:           {def: "perMessageGroupId", parse: enumOf("queue", "perMessageGroupId")},
	attrKmsMasterKeyId:                {parse: passthrough},
	attrRedrivePolicy:                 {parse: parseRedrivePolicy},
	attrRedriveAllowPolicy:            {parse: parseRedriveAllowPolicy},
}

// boundedInt validates an integer attribute with inclusive bounds and
// normalizes it to canonical decimal form.
func boundedInt(min, max int) func(attr, value string) (string, *apiError) {
	return func(attr, value string) (string, *apiError) {
		n, err := strconv.Atoi(value)
		if err != nil || n < min || n > max {
			return "", errInvalidParameterValue(attr,
				fmt.Sprintf("%s must be an integer from %d to %d.", attr, min, max))
		}
		return strconv.Itoa(n), nil
	}
}

// parseBoolValue accepts the SQS boolean literals.
func parseBoolValue(attr, value string) (string, *apiError) {
	switch value {
	case "true", "false":
		return value, nil
	}
	return "", errInvalidParameterValue(attr,
		fmt.Sprintf("%s must be \"true\" or \"false\".", attr))
}

// enumOf validates an enum-typed attribute.
func enumOf(allowed ...string) func(attr, value string) (string, *apiError) {
	return func(attr, value string) (string, *apiError) {
		for _, candidate := range allowed {
			if value == candidate {
				return value, nil
			}
		}
		quoted := make([]string, 0, len(allowed))
		for _, candidate := range allowed {
			quoted = append(quoted, fmt.Sprintf("%q", candidate))
		}
		return "", errInvalidParameterValue(attr,
			fmt.Sprintf("%s must be one of %s.", attr, strings.Join(quoted, ", ")))
	}
}

// passthrough accepts any string value.
func passthrough(attr, value string) (string, *apiError) {
	return value, nil
}

// parseRedrivePolicy validates and canonicalizes the RedrivePolicy
// document so two equal policies compare equal.
func parseRedrivePolicy(attr, value string) (string, *apiError) {
	doc, perr := unmarshalJSONObject(attr, value)
	if perr != nil {
		return "", perr
	}
	if _, ok := doc["deadLetterTargetArn"]; !ok {
		return "", errInvalidParameterValue(attr, "RedrivePolicy must contain deadLetterTargetArn.")
	}
	if _, ok := doc["maxReceiveCount"]; !ok {
		return "", errInvalidParameterValue(attr, "RedrivePolicy must contain maxReceiveCount.")
	}
	return canonicalJSON(doc), nil
}

// parseRedriveAllowPolicy accepts "true"/"false" or a JSON document.
func parseRedriveAllowPolicy(attr, value string) (string, *apiError) {
	switch value {
	case "true", "false":
		return value, nil
	}
	doc, perr := unmarshalJSONObject(attr, value)
	if perr != nil {
		return "", perr
	}
	return canonicalJSON(doc), nil
}

func unmarshalJSONObject(attr, value string) (map[string]interface{}, *apiError) {
	doc := map[string]interface{}{}
	if err := json.Unmarshal([]byte(value), &doc); err != nil {
		return nil, errInvalidParameterValue(attr, attr+" is not a valid JSON document.")
	}
	return doc, nil
}

// canonicalJSON re-encodes a JSON object with sorted keys (Go marshals
// map keys deterministically) so equal policies stringify identically.
func canonicalJSON(doc map[string]interface{}) string {
	b, err := json.Marshal(doc)
	if err != nil {
		return fmt.Sprint(doc)
	}
	return string(b)
}

var (
	attrParamRe       = regexp.MustCompile(`^Attribute\.([0-9]+)\.(name|value)$`)
	legacyAttrParamRe = regexp.MustCompile(`^Attribute\.entry\.([0-9]+)\.(key|value)$`)
	tagParamRe        = regexp.MustCompile(`^Tag\.([0-9]+)\.(key|value)$`)
)

// parseAttributes extracts and validates a queue action's attribute
// pairs. Both the current AWS encoding (Attribute.N.name /
// Attribute.N.value) and the legacy encoding (Attribute.entry.N.key /
// Attribute.entry.N.value) are accepted, covering every AWS SDK.
func parseAttributes(v url.Values) (map[string]string, *apiError) {
	type pair struct {
		name  string
		value string
	}
	byIndex := map[int]*pair{}

	for key, values := range v {
		if len(values) == 0 {
			continue
		}
		index, part := attributeParam(key)
		if index < 0 {
			continue
		}
		if byIndex[index] == nil {
			byIndex[index] = &pair{}
		}
		if part == "name" || part == "key" {
			byIndex[index].name = values[0]
		} else {
			byIndex[index].value = values[0]
		}
	}

	indexes := make([]int, 0, len(byIndex))
	for index := range byIndex {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)

	attrs := make(map[string]string, len(indexes))
	for _, index := range indexes {
		p := byIndex[index]
		if p.name == "" && p.value == "" {
			continue
		}
		if p.name == "" {
			return nil, errMissingParameter(fmt.Sprintf("Attribute.%d.name", index))
		}
		spec, ok := attrSpecs[p.name]
		if !ok {
			return nil, errInvalidAttributeName(p.name)
		}
		normalized, perr := spec.parse(p.name, p.value)
		if perr != nil {
			return nil, perr
		}
		attrs[p.name] = normalized
	}
	return attrs, nil
}

// attributeParam deconstructs an "Attribute.*.*" request key, handling
// the legacy ".entry." encoding; index < 0 marks a non-attribute key.
func attributeParam(key string) (int, string) {
	if m := attrParamRe.FindStringSubmatch(key); m != nil {
		index, _ := strconv.Atoi(m[1])
		return index, m[2]
	}
	if m := legacyAttrParamRe.FindStringSubmatch(key); m != nil {
		index, _ := strconv.Atoi(m[1])
		return index, m[2]
	}
	return -1, ""
}

// parseTags extracts CreateQueue's Tag.N.key / Tag.N.value parameters.
func parseTags(v url.Values) map[string]string {
	type tag struct {
		key   string
		value string
	}
	byIndex := map[int]*tag{}

	for key, values := range v {
		if len(values) == 0 {
			continue
		}
		m := tagParamRe.FindStringSubmatch(key)
		if m == nil {
			continue
		}
		index, _ := strconv.Atoi(m[1])
		if byIndex[index] == nil {
			byIndex[index] = &tag{}
		}
		if m[2] == "key" {
			byIndex[index].key = values[0]
		} else {
			byIndex[index].value = values[0]
		}
	}
	if len(byIndex) == 0 {
		return nil
	}

	tags := make(map[string]string, len(byIndex))
	for _, t := range byIndex {
		if t.key == "" {
			continue
		}
		tags[t.key] = t.value
	}
	return tags
}

// fillQueueAttributes completes validated, explicit attributes with
// the service defaults, producing the queue's full attribute map, the
// baseline against which idempotent re-creates are compared.
func fillQueueAttributes(explicit map[string]string) map[string]string {
	full := make(map[string]string, len(attrSpecs)+len(explicit))
	for name, spec := range attrSpecs {
		if spec.def != "" {
			full[name] = spec.def
		}
	}
	for name, value := range explicit {
		full[name] = value
	}
	return full
}

// sortedAttributeNames lists the settable attributes in stable order.
var sortedAttributeNames = func() []string {
	names := make([]string, 0, len(attrSpecs))
	for name := range attrSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}()

// queueAttributeDiff compares two full attribute maps and returns the
// sorted names of every attribute whose values differ. An attribute
// present in only one map counts as differing.
func queueAttributeDiff(a, b map[string]string) []string {
	var diffs []string
	for _, name := range sortedAttributeNames {
		av, aok := a[name]
		bv, bok := b[name]
		switch {
		case !aok && !bok:
			continue
		case aok != bok, aok && bok && av != bv:
			diffs = append(diffs, name)
		}
	}
	sort.Strings(diffs)
	return diffs
}
