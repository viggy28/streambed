package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/viggy28/streambed/demo/hn/ingester"
)

func main() {
	if err := run(); err != nil {
		slog.Error("HN ingester stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		databaseURL          = flag.String("database-url", "", "Postgres connection URL (defaults to HN_DATABASE_URL or the local demo database)")
		apiBaseURL           = flag.String("api-base-url", env("HN_API_BASE_URL", "https://hacker-news.firebaseio.com/v0"), "Hacker News API base URL")
		pollInterval         = flag.Duration("poll-interval", envDuration("HN_POLL_INTERVAL", 30*time.Second), "poll interval")
		maxItems             = flag.Int("max-items", envInt("HN_LIST_LIMIT", 100), "maximum items retained from each HN list")
		concurrency          = flag.Int("item-concurrency", envInt("HN_ITEM_CONCURRENCY", 16), "maximum concurrent item requests")
		frontPageSize        = flag.Int("front-page-size", envInt("HN_FRONT_PAGE_SIZE", 30), "number of top stories materialized in front_page")
		once                 = flag.Bool("once", false, "perform one poll and exit")
		migrateOnly          = flag.Bool("migrate-only", false, "create the local database schema and exit without calling HN")
		verifySupabaseSchema = flag.Bool("verify-supabase-schema", false, "verify that the versioned Supabase migration was applied, then exit")
		dropReplicationSlot  = flag.String("drop-replication-slot", "", "drop an inactive Postgres replication slot, then exit")
	)
	flag.Parse()
	if *databaseURL == "" {
		*databaseURL = env("HN_DATABASE_URL", "postgres://postgres:test@localhost:55432/hn?sslmode=disable")
	}

	adminModes := 0
	for _, enabled := range []bool{*migrateOnly, *verifySupabaseSchema, *dropReplicationSlot != ""} {
		if enabled {
			adminModes++
		}
	}
	if adminModes > 1 || (*once && adminModes > 0) {
		return fmt.Errorf("choose only one of --once, --migrate-only, --verify-supabase-schema, or --drop-replication-slot")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var store *ingester.Store
	var err error
	if *verifySupabaseSchema || *dropReplicationSlot != "" {
		store, err = ingester.OpenExistingStore(ctx, *databaseURL, *frontPageSize)
	} else {
		store, err = ingester.OpenStore(ctx, *databaseURL, *frontPageSize)
	}
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
	if *migrateOnly {
		logger.Info("HN demo schema is ready")
		return nil
	}
	if *verifySupabaseSchema {
		if err := store.VerifySupabaseSchema(ctx); err != nil {
			return err
		}
		logger.Info("Supabase HN demo schema and publication are ready")
		return nil
	}
	if *dropReplicationSlot != "" {
		dropped, err := store.DropReplicationSlot(ctx, *dropReplicationSlot)
		if err != nil {
			return err
		}
		logger.Info("replication slot cleanup completed", "slot", *dropReplicationSlot, "dropped", dropped)
		return nil
	}

	client, err := ingester.NewClient(*apiBaseURL, *concurrency)
	if err != nil {
		return err
	}
	service, err := ingester.NewService(client, store, logger, *maxItems, *frontPageSize)
	if err != nil {
		return err
	}
	if *once {
		result, err := service.Poll(ctx)
		if err != nil {
			return err
		}
		logger.Info("HN poll completed",
			"lists", result.Lists,
			"tracked_items", result.TrackedItems,
			"fetched_items", result.FetchedItems,
			"observed_at", result.ObservedAt,
		)
		return nil
	}
	logger.Info("HN ingester started", "poll_interval", *pollInterval, "max_items_per_list", *maxItems)
	return service.Run(ctx, *pollInterval)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ignoring invalid %s=%q: %v\n", name, value, err)
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ignoring invalid %s=%q: %v\n", name, value, err)
		return fallback
	}
	return parsed
}
