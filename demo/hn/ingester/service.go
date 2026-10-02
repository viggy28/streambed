package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

type Service struct {
	source        Source
	sink          Sink
	logger        *slog.Logger
	maxItems      int
	frontPageSize int
	tracked       map[int64]struct{}
	initialized   bool
	now           func() time.Time
}

type PollResult struct {
	Lists        int
	TrackedItems int
	FetchedItems int
	ObservedAt   time.Time
}

func NewService(source Source, sink Sink, logger *slog.Logger, maxItems, frontPageSize int) (*Service, error) {
	if maxItems < 1 {
		return nil, fmt.Errorf("max items must be positive")
	}
	if frontPageSize < 1 || frontPageSize > maxItems {
		return nil, fmt.Errorf("front page size must be between 1 and max items")
	}
	return &Service{
		source:        source,
		sink:          sink,
		logger:        logger,
		maxItems:      maxItems,
		frontPageSize: frontPageSize,
		tracked:       make(map[int64]struct{}),
		now:           time.Now,
	}, nil
}

func (s *Service) Poll(ctx context.Context) (PollResult, error) {
	observedAt := s.now().UTC()
	lists, err := s.source.Lists(ctx, s.maxItems)
	if err != nil {
		return PollResult{}, fmt.Errorf("fetch HN lists: %w", err)
	}

	current := listItemSet(lists)
	fetch := make(map[int64]struct{})
	if !s.initialized {
		for id := range current {
			fetch[id] = struct{}{}
		}
	} else {
		for id := range current {
			if _, seen := s.tracked[id]; !seen {
				fetch[id] = struct{}{}
			}
		}
	}
	for index, id := range lists["top"] {
		if index >= s.frontPageSize {
			break
		}
		fetch[id] = struct{}{}
	}

	updatedIDs, updateErr := s.source.Updates(ctx)
	if updateErr != nil {
		s.logger.Warn("HN updates endpoint unavailable; continuing with list and front-page refresh", "error", updateErr)
	} else {
		for _, id := range updatedIDs {
			if _, isTracked := current[id]; isTracked {
				fetch[id] = struct{}{}
			}
		}
	}

	fetchIDs := make([]int64, 0, len(fetch))
	for id := range fetch {
		fetchIDs = append(fetchIDs, id)
	}
	sort.Slice(fetchIDs, func(i, j int) bool { return fetchIDs[i] < fetchIDs[j] })
	items, err := s.source.Items(ctx, fetchIDs)
	if err != nil {
		return PollResult{}, fmt.Errorf("fetch HN items: %w", err)
	}
	if err := s.sink.Apply(ctx, lists, items, observedAt); err != nil {
		return PollResult{}, fmt.Errorf("reconcile Postgres state: %w", err)
	}

	s.tracked = current
	s.initialized = true
	return PollResult{
		Lists:        len(lists),
		TrackedItems: len(current),
		FetchedItems: len(items),
		ObservedAt:   observedAt,
	}, nil
}

func (s *Service) Run(ctx context.Context, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	poll := func() {
		result, err := s.Poll(ctx)
		if err != nil {
			s.logger.Error("HN poll failed", "error", err)
			return
		}
		s.logger.Info("HN poll completed",
			"lists", result.Lists,
			"tracked_items", result.TrackedItems,
			"fetched_items", result.FetchedItems,
			"observed_at", result.ObservedAt,
		)
	}

	poll()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			poll()
		}
	}
}

func listItemSet(lists map[string][]int64) map[int64]struct{} {
	items := make(map[int64]struct{})
	for _, ids := range lists {
		for _, id := range ids {
			items[id] = struct{}{}
		}
	}
	return items
}
