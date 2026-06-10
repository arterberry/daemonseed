package cron

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr string) *Schedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	return s
}

func at(t *testing.T, value string) time.Time {
	t.Helper()
	ts, err := time.Parse("2006-01-02 15:04", value)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestParse_Invalid(t *testing.T) {
	for _, expr := range []string{
		"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *",
		"* * 0 * *", "* * 32 * *", "* * * 13 *", "* * * * 8",
		"a * * * *", "1-0 * * * *", "*/0 * * * *", "1--2 * * * *",
	} {
		if _, err := Parse(expr); err == nil {
			t.Errorf("%q must be rejected", expr)
		}
	}
}

func TestNext_Table(t *testing.T) {
	tests := []struct {
		expr  string
		after string
		want  string
	}{
		// Nightly at 02:00 (the spec's motivating example).
		{"0 2 * * *", "2026-06-09 17:00", "2026-06-10 02:00"},
		{"0 2 * * *", "2026-06-10 01:59", "2026-06-10 02:00"},
		{"0 2 * * *", "2026-06-10 02:00", "2026-06-11 02:00"}, // strictly after
		// Every 15 minutes.
		{"*/15 * * * *", "2026-06-09 10:07", "2026-06-09 10:15"},
		{"*/15 * * * *", "2026-06-09 10:45", "2026-06-09 11:00"},
		// Ranges and lists.
		{"30 9-17 * * *", "2026-06-09 17:31", "2026-06-10 09:30"},
		{"0 0 1,15 * *", "2026-06-02 00:00", "2026-06-15 00:00"},
		// Month rollover and names.
		{"0 0 1 jan *", "2026-06-09 12:00", "2027-01-01 00:00"},
		{"0 12 * * mon", "2026-06-09 13:00", "2026-06-15 12:00"}, // Jun 9 2026 is a Tuesday
		// dow 7 == Sunday.
		{"0 8 * * 7", "2026-06-09 09:00", "2026-06-14 08:00"},
		// Standard quirk: both dom and dow restricted → either matches.
		{"0 0 13 * 5", "2026-06-09 00:00", "2026-06-12 00:00"}, // Fri Jun 12 before the 13th
		// Step with explicit start.
		{"5/20 * * * *", "2026-06-09 10:00", "2026-06-09 10:05"},
		{"5/20 * * * *", "2026-06-09 10:06", "2026-06-09 10:25"},
	}
	for _, tt := range tests {
		s := mustParse(t, tt.expr)
		got := s.Next(at(t, tt.after))
		want := at(t, tt.want)
		if !got.Equal(want) {
			t.Errorf("%q after %s: got %s, want %s", tt.expr, tt.after, got, want)
		}
	}
}

func TestNext_Unsatisfiable(t *testing.T) {
	s := mustParse(t, "0 0 30 2 *") // February 30th never exists
	if got := s.Next(at(t, "2026-06-09 00:00")); !got.IsZero() {
		t.Errorf("unsatisfiable schedule must return zero time, got %s", got)
	}
}

func TestNext_MinimumGap(t *testing.T) {
	// "* * * * *" fires every minute; the gap between consecutive fires is 1m.
	s := mustParse(t, "* * * * *")
	first := s.Next(at(t, "2026-06-09 10:00"))
	second := s.Next(first)
	if second.Sub(first) != time.Minute {
		t.Errorf("gap = %s, want 1m", second.Sub(first))
	}
}
