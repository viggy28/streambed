package main

import (
	"testing"
	"time"
)

func TestParseDateOrTimestamp(t *testing.T) {
	date, err := parseDateOrTimestamp("2024-10-08")
	if err != nil || date.Format(time.RFC3339) != "2024-10-08T00:00:00Z" {
		t.Fatalf("date = %s, err = %v", date, err)
	}
	timestamp, err := parseDateOrTimestamp("2024-10-08T12:30:00-04:00")
	if err != nil || timestamp.Format(time.RFC3339) != "2024-10-08T16:30:00Z" {
		t.Fatalf("timestamp = %s, err = %v", timestamp, err)
	}
}
