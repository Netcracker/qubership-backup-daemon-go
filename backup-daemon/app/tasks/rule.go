package tasks

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type RuleType int

const (
	LimitType RuleType = iota
	IntervalType
)

type Rule struct {
	Type   RuleType
	First  int64
	Second interface{}
}

var reLimit = regexp.MustCompile(`^(\d+)$`)

var reInterval = regexp.MustCompile(`^(\d+)(min|h|d|m|y)$`)

// magnifiers: time unit → seconds. Note: "m" = month (30d), "min" = minute.
var magnifiers = map[string]int64{
	"min": 60,
	"h":   60 * 60,
	"d":   60 * 60 * 24,
	"m":   60 * 60 * 24 * 30,
	"y":   60 * 60 * 24 * 30 * 12,
}

// NewRule parses a single "first/second" eviction rule.
// Type is determined by the second part; if absent, by the first; default is IntervalType.
// "0" is a zero-anchor (no type) used in interval rules like "0/1d".
// A bare integer N (e.g. "3/delete") is count-based (LimitType).
func NewRule(rule string) (Rule, error) {
	parts := strings.Split(strings.TrimSpace(rule), "/")
	if len(parts) != 2 {
		return Rule{}, fmt.Errorf("invalid rule format: %s", rule)
	}

	first, t1, hasT1, err := parseSpec(parts[0])
	if err != nil {
		return Rule{}, err
	}

	typ := IntervalType
	if hasT1 {
		typ = t1
	}

	var second interface{}
	if parts[1] == "delete" {
		second = "delete"
	} else {
		s, t2, hasT2, err := parseSpec(parts[1])
		if err != nil {
			return Rule{}, err
		}
		second = s
		if hasT2 {
			typ = t2
		}
	}

	return Rule{
		Type:   typ,
		First:  first,
		Second: second,
	}, nil
}

// parseSpec parses one side of a rule spec: (value, type, hasType, error).
// "0" → zero-anchor (hasType=false). "N" → LimitType. "Nunit" → IntervalType (ms).
func parseSpec(spec string) (int64, RuleType, bool, error) {
	if spec == "0" {
		return 0, 0, false, nil
	}

	if reLimit.MatchString(spec) {
		n, err := strconv.Atoi(reLimit.FindStringSubmatch(spec)[1])
		if err != nil {
			return 0, LimitType, true, fmt.Errorf("invalid rule format: %s", spec)
		}
		return int64(n), LimitType, true, nil
	}

	if reInterval.MatchString(spec) {
		m := reInterval.FindStringSubmatch(spec)
		digit, err := strconv.ParseInt(m[1], 0, 64)
		if err != nil {
			return 0, IntervalType, true, fmt.Errorf("invalid rule format: %s", spec)
		}
		unit := m[2]
		return digit * magnifiers[unit] * 1000, IntervalType, true, nil
	}
	return 0, 0, false, fmt.Errorf("invalid spec: %s", spec)
}

func parseRules(rules string) ([]Rule, error) {
	parts := strings.Split(rules, ",")
	result := make([]Rule, 0, len(parts))
	for _, part := range parts {
		rule, err := NewRule(part)
		if err != nil {
			return nil, err
		}
		result = append(result, rule)
	}
	return result, nil
}
