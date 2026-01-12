package controller

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
	First  int
	Second interface{}
}

var magnifiers = map[string]int{
	"min": 60,
	"h":   60 * 60,
	"d":   60 * 60 * 24,
	"m":   60 * 60 * 24 * 30,
	"y":   60 * 60 * 24 * 30 * 12,
}

func NewRule(rule string) (Rule, error) {
	parts := strings.Split(strings.TrimSpace(rule), "/")
	if len(parts) != 2 {
		return Rule{}, fmt.Errorf("invalid rule format: %s", rule)
	}

	first, t1, err := parseSpec(parts[0])
	if err != nil {
		return Rule{}, err
	}

	var second interface{}
	if parts[1] == "delete" {
		second = "delete"
	} else {
		s, _, err := parseSpec(parts[1])
		if err != nil {
			return Rule{}, err
		}
		second = s
	}
	return Rule{
		Type: t1,
		First: first,
		Second: second,
	}, nil
}

func parseSpec(spec string) (int, RuleType, error) {
	if spec == "0" {
		return 0, LimitType, nil
	}
	reLimit := regexp.MustCompile(`^(\d+)$`)
	if reLimit.MatchString(spec) {
		n, err := strconv.Atoi(reLimit.FindStringSubmatch(spec)[1])
		if err != nil {
			return 0, LimitType, fmt.Errorf("invalid rule format: %s", spec)
		}
		return n, IntervalType, nil
	}

	reInterval := regexp.MustCompile(`^(\d+)(min|h|d|m|y)$`)
	if reInterval.MatchString(spec) {
		m := reInterval.FindStringSubmatch(spec)
		digit, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, LimitType, fmt.Errorf("invalid rule format: %s", spec)
		}
		unit := m[2]
		return digit * magnifiers[unit], IntervalType, nil
	}
	return 0, 0, fmt.Errorf("invalid spec: %s", spec)
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
