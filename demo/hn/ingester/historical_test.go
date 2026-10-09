package ingester

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistoricalClientRetriesTruncatedResponse(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"nbHits":1,"hits":[`))
			return
		}
		_, _ = w.Write([]byte(`{"nbHits":1,"hits":[{"objectID":"42","created_at_i":100,"title":"Postgres"}]}`))
	}))
	defer server.Close()
	client, err := NewHistoricalClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	items, err := client.Stories(context.Background(), time.Unix(100, 0), time.Unix(101, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != 42 || requests.Load() != 2 {
		t.Fatalf("items=%+v requests=%d", items, requests.Load())
	}
}

func TestHistoricalClientSplitsDenseRanges(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := time.Unix(104, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filters := r.URL.Query().Get("numericFilters")
		parts := strings.Split(filters, ",")
		from, _ := strconv.ParseInt(strings.TrimPrefix(parts[0], "created_at_i>="), 10, 64)
		to, _ := strconv.ParseInt(strings.TrimPrefix(parts[1], "created_at_i<"), 10, 64)
		w.Header().Set("Content-Type", "application/json")
		if to-from > 2 {
			_ = json.NewEncoder(w).Encode(map[string]any{"nbHits": 1001, "hits": []any{}})
			return
		}
		points := int64(10 + from)
		comments := int64(2)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"nbHits": 1,
			"hits": []any{map[string]any{
				"objectID": strconv.FormatInt(from, 10), "created_at_i": from,
				"title": "Postgres story", "url": "https://example.com", "author": "demo",
				"points": &points, "num_comments": &comments,
			}},
		})
	}))
	defer server.Close()

	client, err := NewHistoricalClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	items, err := client.Stories(context.Background(), start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ID != 100 || items[1].ID != 102 {
		t.Fatalf("unexpected items: %+v", items)
	}
	if items[0].Title != "Postgres story" || items[0].Descendants != 2 {
		t.Fatalf("unexpected mapped item: %+v", items[0])
	}
}

type fakeHistoricalSource struct {
	windows [][2]time.Time
}

func (f *fakeHistoricalSource) Stories(_ context.Context, start, end time.Time) ([]Item, error) {
	f.windows = append(f.windows, [2]time.Time{start, end})
	return []Item{{ID: start.Unix(), Type: "story", Time: start.Unix()}}, nil
}

type fakeBackfillSink struct {
	next    time.Time
	applied int
}

func (f *fakeBackfillSink) PrepareBackfill(context.Context, string, time.Time, time.Time) (time.Time, error) {
	return f.next, nil
}

func (f *fakeBackfillSink) ApplyHistoricalWindow(_ context.Context, _ string, _, _ time.Time, items []Item, _ time.Time) (int64, error) {
	f.applied++
	return int64(len(items)), nil
}

func TestContainsASCIIWord(t *testing.T) {
	tests := []struct {
		value string
		word  string
		want  bool
	}{
		{value: "PostgreSQL 18 released", word: "postgresql", want: true},
		{value: "Using Postgres with Go", word: "postgres", want: true},
		{value: "postgresql", word: "postgres", want: false},
		{value: "mysql-compatible database", word: "mysql", want: true},
		{value: "rustic furniture", word: "rust", want: false},
		{value: "Why AI?", word: "ai", want: true},
	}
	for _, test := range tests {
		if got := containsASCIIWord(test.value, test.word); got != test.want {
			t.Errorf("containsASCIIWord(%q, %q) = %v, want %v", test.value, test.word, got, test.want)
		}
	}
}

func TestBackfillerResumesFromCheckpoint(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(72 * time.Hour)
	source := &fakeHistoricalSource{}
	sink := &fakeBackfillSink{next: start.Add(24 * time.Hour)}
	backfiller := NewBackfiller(source, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	backfiller.now = func() time.Time { return end }

	result, err := backfiller.RunRange(context.Background(), "test", start, end, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.Windows != 2 || result.Inserted != 2 || sink.applied != 2 {
		t.Fatalf("unexpected result: %+v, applied=%d", result, sink.applied)
	}
	if !source.windows[0][0].Equal(start.Add(24*time.Hour)) || !result.Next.Equal(end) {
		t.Fatalf("backfill did not resume: windows=%v result=%+v", source.windows, result)
	}
}
