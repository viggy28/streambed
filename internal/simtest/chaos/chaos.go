// Package chaos introduces deliberate failures during a simtest run so that
// recovery paths get exercised unattended. Jepsen's lesson: bugs live in
// failure modes, not the happy path.
package chaos

import (
	"context"
	"log/slog"
	"time"
)

// Killer is the interface the chaos loop calls to terminate streambed.
// Implemented by supervisor.Streambed via its Kill method.
type Killer interface {
	Kill() error
}

// Config tunes the chaos loop. Zero intervals disable that nemesis.
type Config struct {
	Sup            Killer
	KillEvery      time.Duration
	PauseMinioFn   func(ctx context.Context) error
	PauseEvery     time.Duration
}

// Run blocks until ctx is cancelled. It alternates between the configured
// nemeses based on their intervals. Safe to call with both intervals zero —
// it simply waits until ctx is done.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) {
	if cfg.KillEvery == 0 && (cfg.PauseEvery == 0 || cfg.PauseMinioFn == nil) {
		<-ctx.Done()
		return
	}

	// Stagger start so the first event is not immediate.
	select {
	case <-ctx.Done():
		return
	case <-time.After(cfg.startDelay()):
	}

	var killTimer, pauseTimer <-chan time.Time
	if cfg.KillEvery > 0 && cfg.Sup != nil {
		t := time.NewTicker(cfg.KillEvery)
		defer t.Stop()
		killTimer = t.C
	}
	if cfg.PauseEvery > 0 && cfg.PauseMinioFn != nil {
		t := time.NewTicker(cfg.PauseEvery)
		defer t.Stop()
		pauseTimer = t.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-killTimer:
			logger.Info("chaos: killing streambed")
			if err := cfg.Sup.Kill(); err != nil {
				logger.Warn("chaos: kill failed", "error", err)
			}
		case <-pauseTimer:
			logger.Info("chaos: pausing MinIO")
			if err := cfg.PauseMinioFn(ctx); err != nil {
				logger.Warn("chaos: pause minio failed", "error", err)
			}
		}
	}
}

// startDelay gives streambed a minute of runway before the first chaos event
// so we don't kill it mid-initial-connection. Override by setting a small
// interval if aggressive test coverage is desired.
func (c Config) startDelay() time.Duration {
	delay := 30 * time.Second
	// For heavy chaos (kill interval < 2 min), shorten the startup delay
	// so we still see events in a short run.
	if c.KillEvery > 0 && c.KillEvery < 2*time.Minute {
		delay = 20 * time.Second
	}
	return delay
}
