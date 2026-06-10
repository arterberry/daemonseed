// Package cron implements a minimal standard 5-field cron parser
// (minute hour day-of-month month day-of-week) supporting *, numbers,
// names (jan–dec, sun–sat), ranges, lists, and steps.
//
// Spec §20.8 calls for an established library (robfig/cron); this package
// exists because the build environment had no network access to fetch one.
// It implements the same standard semantics — including the classic quirk
// that when both day-of-month and day-of-week are restricted, a time
// matches if EITHER matches — and can be swapped out without changing the
// scheduler's interface.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron expression.
type Schedule struct {
	minutes [60]bool
	hours   [24]bool
	dom     [32]bool // 1–31
	months  [13]bool // 1–12
	dow     [7]bool  // 0=Sunday
	domStar bool
	dowStar bool
	expr    string
}

// String returns the original expression.
func (s *Schedule) String() string { return s.expr }

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// Parse parses a standard 5-field cron expression.
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields (minute hour dom month dow), got %d in %q",
			len(fields), expr)
	}
	s := &Schedule{expr: expr}
	specs := []struct {
		field string
		min   int
		max   int
		names map[string]int
		set   func(int)
		star  *bool
	}{
		{fields[0], 0, 59, nil, func(i int) { s.minutes[i] = true }, nil},
		{fields[1], 0, 23, nil, func(i int) { s.hours[i] = true }, nil},
		{fields[2], 1, 31, nil, func(i int) { s.dom[i] = true }, &s.domStar},
		{fields[3], 1, 12, monthNames, func(i int) { s.months[i] = true }, nil},
		{fields[4], 0, 7, dowNames, func(i int) { s.dow[i%7] = true }, &s.dowStar}, // 7 = Sunday
	}
	for _, sp := range specs {
		star, err := parseField(sp.field, sp.min, sp.max, sp.names, sp.set)
		if err != nil {
			return nil, fmt.Errorf("cron %q: %w", expr, err)
		}
		if sp.star != nil {
			*sp.star = star
		}
	}
	return s, nil
}

// parseField fills set() for every value matched by a single field. Returns
// whether the field was an unrestricted "*".
func parseField(field string, min, max int, names map[string]int, set func(int)) (bool, error) {
	if field == "*" {
		for i := min; i <= max; i++ {
			set(i)
		}
		return true, nil
	}
	for _, item := range strings.Split(field, ",") {
		spec, stepStr, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepStr)
			if err != nil || n <= 0 {
				return false, fmt.Errorf("invalid step %q", item)
			}
			step = n
		}
		lo, hi := min, max
		if spec != "*" {
			loStr, hiStr, isRange := strings.Cut(spec, "-")
			var err error
			if lo, err = parseValue(loStr, names); err != nil {
				return false, fmt.Errorf("invalid value %q: %w", item, err)
			}
			if isRange {
				if hi, err = parseValue(hiStr, names); err != nil {
					return false, fmt.Errorf("invalid range %q: %w", item, err)
				}
			} else if hasStep {
				hi = max // "N/step" means "from N to max by step"
			} else {
				hi = lo
			}
		}
		if lo < min || hi > max || lo > hi {
			return false, fmt.Errorf("value %q out of range %d-%d", item, min, max)
		}
		for i := lo; i <= hi; i += step {
			set(i)
		}
	}
	return false, nil
}

func parseValue(s string, names map[string]int) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("not a number or name: %q", s)
	}
	return v, nil
}

// matchesDay implements the standard cron day rule: if both day-of-month
// and day-of-week are restricted, either may match; otherwise the
// restricted one (or both stars) must match.
func (s *Schedule) matchesDay(t time.Time) bool {
	domMatch := s.dom[t.Day()]
	dowMatch := s.dow[int(t.Weekday())]
	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dowMatch
	case s.dowStar:
		return domMatch
	default:
		return domMatch || dowMatch
	}
}

// Next returns the first time strictly after t that matches the schedule,
// or the zero time if none exists within five years (unsatisfiable
// expressions like "0 0 30 2 *").
func (s *Schedule) Next(t time.Time) time.Time {
	// Work at minute granularity in t's location.
	cur := t.Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(5, 0, 0)
	for cur.Before(limit) {
		if !s.months[int(cur.Month())] {
			// Jump to the first instant of the next month.
			cur = time.Date(cur.Year(), cur.Month(), 1, 0, 0, 0, 0, cur.Location()).AddDate(0, 1, 0)
			continue
		}
		if !s.matchesDay(cur) {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day(), 0, 0, 0, 0, cur.Location()).AddDate(0, 0, 1)
			continue
		}
		if !s.hours[cur.Hour()] {
			cur = cur.Truncate(time.Hour).Add(time.Hour)
			continue
		}
		if !s.minutes[cur.Minute()] {
			cur = cur.Add(time.Minute)
			continue
		}
		return cur
	}
	return time.Time{}
}
