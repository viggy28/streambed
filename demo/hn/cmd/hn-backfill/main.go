package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/viggy28/streambed/demo/hn/ingester"
)

func main() {
	if err := run(); err != nil {
		slog.Error("HN historical backfill stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		databaseURL = flag.String("database-url", os.Getenv("HN_DATABASE_URL"), "Postgres connection URL (or HN_DATABASE_URL)")
		apiBaseURL  = flag.String("api-base-url", "https://hn.algolia.com/api/v1", "Algolia HN Search API base URL")
		sinceText   = flag.String("since", "", "inclusive UTC start date or timestamp (required)")
		untilText   = flag.String("until", "", "exclusive UTC end date or timestamp (required)")
		window      = flag.Duration("window", 24*time.Hour, "checkpoint window size")
		checkpoint  = flag.String("checkpoint-name", "algolia-hn-stories-v1", "persistent checkpoint name")
	)
	flag.Parse()
	if *databaseURL == "" {
		return fmt.Errorf("--database-url or HN_DATABASE_URL is required")
	}
	start, err := parseDateOrTimestamp(*sinceText)
	if err != nil {
		return fmt.Errorf("parse --since: %w", err)
	}
	end, err := parseDateOrTimestamp(*untilText)
	if err != nil {
		return fmt.Errorf("parse --until: %w", err)
	}
	if !start.Before(end) {
		return fmt.Errorf("--since must be before --until")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	store, err := ingester.OpenExistingStore(ctx, *databaseURL, 30)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Close(closeCtx); err != nil {
			logger.Warn("close Postgres connection", "error", err)
		}
	}()

	client, err := ingester.NewHistoricalClient(*apiBaseURL)
	if err != nil {
		return err
	}
	logger.Info("HN historical backfill starting",
		"source", *apiBaseURL,
		"start", start,
		"end", end,
		"window", *window,
	)
	result, err := ingester.NewBackfiller(client, store, logger).RunRange(
		ctx, *checkpoint, start, end, *window,
	)
	if err != nil {
		return err
	}
	logger.Info("HN historical backfill completed",
		"windows", result.Windows,
		"stories_seen", result.Seen,
		"stories_inserted", result.Inserted,
		"next", result.Next,
	)
	return nil
}

func parseDateOrTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("value is required")
	}
	for _, layout := range []string{time.RFC3339, time.DateOnly} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("use YYYY-MM-DD or RFC3339")
}
