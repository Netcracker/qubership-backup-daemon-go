package tasks

import (
	"testing"
)

func TestNewRule_parse(t *testing.T) {
	sec := int64(1000)

	tests := []struct {
		input      string
		wantType   RuleType
		wantFirst  int64
		wantSecond interface{}
	}{
		// zero-anchor interval rules
		{"0/1d", IntervalType, 0, int64(24 * 60 * 60 * sec)},
		{"0/1h", IntervalType, 0, int64(60 * 60 * sec)},
		{"0/1min", IntervalType, 0, int64(60 * sec)},
		// interval rules
		{"3d/1d", IntervalType, int64(3 * 24 * 60 * 60 * sec), int64(24 * 60 * 60 * sec)},
		{"7d/7d", IntervalType, int64(7 * 24 * 60 * 60 * sec), int64(7 * 24 * 60 * 60 * sec)},
		{"1m/1m", IntervalType, int64(30 * 24 * 60 * 60 * sec), int64(30 * 24 * 60 * 60 * sec)},
		// interval delete
		{"7d/delete", IntervalType, int64(7 * 24 * 60 * 60 * sec), "delete"},
		{"1y/delete", IntervalType, int64(12 * 30 * 24 * 60 * 60 * sec), "delete"},
		{"0/delete", IntervalType, 0, "delete"},
		// count-based (LimitType)
		{"5/delete", LimitType, 5, "delete"},
		{"3/delete", LimitType, 3, "delete"},
		{"1/delete", LimitType, 1, "delete"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := NewRule(tt.input)
			if err != nil {
				t.Fatalf("NewRule(%q) error = %v", tt.input, err)
			}
			if got.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", got.Type, tt.wantType)
			}
			if got.First != tt.wantFirst {
				t.Errorf("First = %v, want %v", got.First, tt.wantFirst)
			}
			if got.Second != tt.wantSecond {
				t.Errorf("Second = %v, want %v", got.Second, tt.wantSecond)
			}
		})
	}
}

func TestNewRule_compound(t *testing.T) {
	sec := int64(1000)
	rules, err := parseRules("0/1h,3d/1d,7d/7d,1m/1m,1y/delete")
	if err != nil {
		t.Fatalf("parseRules error = %v", err)
	}
	if len(rules) != 5 {
		t.Fatalf("got %d rules, want 5", len(rules))
	}

	type want struct {
		typ    RuleType
		first  int64
		second interface{}
	}
	expected := []want{
		{IntervalType, 0, int64(60 * 60 * sec)},
		{IntervalType, int64(3 * 24 * 60 * 60 * sec), int64(24 * 60 * 60 * sec)},
		{IntervalType, int64(7 * 24 * 60 * 60 * sec), int64(7 * 24 * 60 * 60 * sec)},
		{IntervalType, int64(30 * 24 * 60 * 60 * sec), int64(30 * 24 * 60 * 60 * sec)},
		{IntervalType, int64(12 * 30 * 24 * 60 * 60 * sec), "delete"},
	}
	for i, w := range expected {
		if rules[i].Type != w.typ {
			t.Errorf("rule[%d].Type = %v, want %v", i, rules[i].Type, w.typ)
		}
		if rules[i].First != w.first {
			t.Errorf("rule[%d].First = %v, want %v", i, rules[i].First, w.first)
		}
		if rules[i].Second != w.second {
			t.Errorf("rule[%d].Second = %v, want %v", i, rules[i].Second, w.second)
		}
	}
}

func TestNewRule_invalid(t *testing.T) {
	bad := []string{"", "noslash", "bad/rule", "1x/delete", "abc/1d"}
	for _, s := range bad {
		_, err := NewRule(s)
		if err == nil {
			t.Errorf("NewRule(%q) expected error, got nil", s)
		}
	}
}
