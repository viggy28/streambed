package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

var ListEndpoints = map[string]string{
	"top":  "topstories",
	"best": "beststories",
	"new":  "newstories",
	"ask":  "askstories",
	"show": "showstories",
	"jobs": "jobstories",
}

type Item struct {
	ID          int64  `json:"id"`
	Deleted     bool   `json:"deleted"`
	Type        string `json:"type"`
	By          string `json:"by"`
	Time        int64  `json:"time"`
	Dead        bool   `json:"dead"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	Score       int64  `json:"score"`
	Descendants int64  `json:"descendants"`
}

type Source interface {
	Lists(context.Context, int) (map[string][]int64, error)
	Updates(context.Context) ([]int64, error)
	Items(context.Context, []int64) ([]Item, error)
}

type Client struct {
	baseURL     string
	httpClient  *http.Client
	concurrency int
}

func NewClient(baseURL string, concurrency int) (*Client, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("item concurrency must be positive")
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("invalid HN API base URL: %w", err)
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		concurrency: concurrency,
	}, nil
}

func (c *Client) Lists(ctx context.Context, maxItems int) (map[string][]int64, error) {
	if maxItems < 1 {
		return nil, fmt.Errorf("max items must be positive")
	}

	type result struct {
		name string
		ids  []int64
		err  error
	}
	results := make(chan result, len(ListEndpoints))
	var wg sync.WaitGroup
	for name, endpoint := range ListEndpoints {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ids []int64
			err := c.getJSON(ctx, endpoint+".json", &ids)
			if len(ids) > maxItems {
				ids = ids[:maxItems]
			}
			results <- result{name: name, ids: ids, err: err}
		}()
	}
	wg.Wait()
	close(results)

	lists := make(map[string][]int64, len(ListEndpoints))
	var errs []error
	for result := range results {
		if result.err != nil {
			errs = append(errs, fmt.Errorf("fetch %s stories: %w", result.name, result.err))
			continue
		}
		lists[result.name] = result.ids
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return lists, nil
}

func (c *Client) Updates(ctx context.Context) ([]int64, error) {
	var response struct {
		Items []int64 `json:"items"`
	}
	if err := c.getJSON(ctx, "updates.json", &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (c *Client) Items(ctx context.Context, ids []int64) ([]Item, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	jobs := make(chan int64)
	items := make(chan Item, len(ids))
	errs := make(chan error, len(ids))
	var wg sync.WaitGroup
	workerCount := min(c.concurrency, len(ids))
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				var item *Item
				if err := c.getJSON(ctx, fmt.Sprintf("item/%d.json", id), &item); err != nil {
					errs <- fmt.Errorf("fetch item %d: %w", id, err)
					continue
				}
				if item == nil {
					errs <- fmt.Errorf("fetch item %d: API returned null", id)
					continue
				}
				items <- *item
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, id := range ids {
			select {
			case jobs <- id:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	close(items)
	close(errs)

	result := make([]Item, 0, len(ids))
	for item := range items {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	var fetchErrs []error
	for err := range errs {
		fetchErrs = append(fetchErrs, err)
	}
	if ctx.Err() != nil {
		fetchErrs = append(fetchErrs, ctx.Err())
	}
	if len(fetchErrs) > 0 {
		return nil, errors.Join(fetchErrs...)
	}
	return result, nil
}

func (c *Client) getJSON(ctx context.Context, relativePath string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/"+relativePath, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "streambed-hn-demo/1.0")

	response, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
