package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const historicalPageSize = 1000

// HistoricalSource returns HN stories created in [start, end).
type HistoricalSource interface {
	Stories(context.Context, time.Time, time.Time) ([]Item, error)
}

// BackfillSink persists one completed historical window and its checkpoint in
// the same transaction, making the backfill resumable without duplicate rows.
type BackfillSink interface {
	PrepareBackfill(context.Context, string, time.Time, time.Time) (time.Time, error)
	ApplyHistoricalWindow(context.Context, string, time.Time, time.Time, []Item, time.Time) (int64, error)
}

// HistoricalClient reads the public Algolia HN Search index. The official HN
// API has no time-range endpoint, so this source is used only for the demo's
// reproducible historical seed. Live updates continue to use the official API.
type HistoricalClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewHistoricalClient(baseURL string) (*HistoricalClient, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("invalid historical API base URL: %w", err)
	}
	return &HistoricalClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (c *HistoricalClient) Stories(ctx context.Context, start, end time.Time) ([]Item, error) {
	if !start.Before(end) {
		return nil, fmt.Errorf("historical range start must be before end")
	}
	items := make(map[int64]Item)
	if err := c.fetchRange(ctx, start.UTC(), end.UTC(), items); err != nil {
		return nil, err
	}
	result := make([]Item, 0, len(items))
	for _, item := range items {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (c *HistoricalClient) fetchRange(ctx context.Context, start, end time.Time, items map[int64]Item) error {
	response, err := c.search(ctx, start, end)
	if err != nil {
		return err
	}
	if response.NbHits > len(response.Hits) {
		if end.Sub(start) <= time.Second {
			return fmt.Errorf("historical API returned %d stories in an indivisible one-second window", response.NbHits)
		}
		middle := start.Add(end.Sub(start) / 2).Truncate(time.Second)
		if !middle.After(start) {
			middle = start.Add(time.Second)
		}
		if !middle.Before(end) {
			return fmt.Errorf("cannot split dense historical range %s to %s", start, end)
		}
		if err := c.fetchRange(ctx, start, middle, items); err != nil {
			return err
		}
		return c.fetchRange(ctx, middle, end, items)
	}

	for _, hit := range response.Hits {
		id, err := strconv.ParseInt(hit.ObjectID, 10, 64)
		if err != nil {
			return fmt.Errorf("parse historical story id %q: %w", hit.ObjectID, err)
		}
		item := Item{
			ID:          id,
			Type:        "story",
			By:          hit.Author,
			Time:        hit.CreatedAtI,
			Title:       hit.Title,
			URL:         hit.URL,
			Score:       valueOrZero(hit.Points),
			Descendants: valueOrZero(hit.NumComments),
		}
		items[id] = item
	}
	return nil
}

type historicalSearchResponse struct {
	NbHits int `json:"nbHits"`
	Hits   []struct {
		ObjectID    string `json:"objectID"`
		CreatedAtI  int64  `json:"created_at_i"`
		Title       string `json:"title"`
		URL         string `json:"url"`
		Author      string `json:"author"`
		Points      *int64 `json:"points"`
		NumComments *int64 `json:"num_comments"`
	} `json:"hits"`
}

func (c *HistoricalClient) search(ctx context.Context, start, end time.Time) (historicalSearchResponse, error) {
	endpoint, err := url.Parse(c.baseURL + "/search_by_date")
	if err != nil {
		return historicalSearchResponse{}, err
	}
	query := endpoint.Query()
	query.Set("tags", "story")
	query.Set("hitsPerPage", strconv.Itoa(historicalPageSize))
	query.Set("page", "0")
	query.Set("numericFilters", fmt.Sprintf("created_at_i>=%d,created_at_i<%d", start.Unix(), end.Unix()))
	endpoint.RawQuery = query.Encode()

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return historicalSearchResponse{}, err
		}
		request.Header.Set("User-Agent", "streambed-hn-demo/1.0")
		response, err := c.httpClient.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			var result historicalSearchResponse
			decodeErr := json.NewDecoder(response.Body).Decode(&result)
			response.Body.Close()
			if decodeErr == nil {
				return result, nil
			}
			lastErr = fmt.Errorf("decode historical API response: %w", decodeErr)
			if err := waitContext(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
				return historicalSearchResponse{}, err
			}
			continue
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("historical API returned %s", response.Status)
			response.Body.Close()
			if response.StatusCode != http.StatusTooManyRequests && response.StatusCode < 500 {
				return historicalSearchResponse{}, lastErr
			}
		}
		if err := waitContext(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			return historicalSearchResponse{}, err
		}
	}
	return historicalSearchResponse{}, lastErr
}

func valueOrZero(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Backfiller imports fixed windows in chronological order. Each window and its
// checkpoint commit atomically through BackfillSink.
type Backfiller struct {
	source HistoricalSource
	sink   BackfillSink
	logger *slog.Logger
	now    func() time.Time
}

type BackfillResult struct {
	Windows  int
	Seen     int
	Inserted int64
	Next     time.Time
}

func NewBackfiller(source HistoricalSource, sink BackfillSink, logger *slog.Logger) *Backfiller {
	return &Backfiller{source: source, sink: sink, logger: logger, now: time.Now}
}

// RunRange imports [start, end) using fixed-size windows.
func (b *Backfiller) RunRange(ctx context.Context, name string, start, end time.Time, window time.Duration) (BackfillResult, error) {
	if name == "" {
		return BackfillResult{}, fmt.Errorf("backfill name is required")
	}
	if !start.Before(end) {
		return BackfillResult{}, fmt.Errorf("backfill start must be before end")
	}
	if window <= 0 {
		return BackfillResult{}, fmt.Errorf("backfill window must be positive")
	}

	next, err := b.sink.PrepareBackfill(ctx, name, start.UTC(), end.UTC())
	if err != nil {
		return BackfillResult{}, err
	}
	result := BackfillResult{Next: next}
	for next.Before(end) {
		windowEnd := next.Add(window)
		if windowEnd.After(end) {
			windowEnd = end
		}
		items, err := b.source.Stories(ctx, next, windowEnd)
		if err != nil {
			return result, fmt.Errorf("fetch historical stories %s to %s: %w", next, windowEnd, err)
		}
		inserted, err := b.sink.ApplyHistoricalWindow(ctx, name, next, windowEnd, items, b.now().UTC())
		if err != nil {
			return result, fmt.Errorf("store historical stories %s to %s: %w", next, windowEnd, err)
		}
		result.Windows++
		result.Seen += len(items)
		result.Inserted += inserted
		result.Next = windowEnd
		b.logger.Info("HN historical window completed",
			"start", next,
			"end", windowEnd,
			"stories", len(items),
			"inserted", inserted,
		)
		next = windowEnd
	}
	return result, nil
}
