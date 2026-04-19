// Package supervisor runs Streambed as a long-lived subprocess, restarts it
// when it exits, and routes its stderr to a per-run log file. The supervisor
// also exposes a Kill hook so the chaos injector can force-restart streambed
// without racing with the normal restart loop.
package supervisor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Config describes how to launch streambed.
type Config struct {
	Binary    string   // path to streambed binary
	Args      []string // args after the binary (e.g., "sync", "--source-url=...")
	LogPath   string   // per-run file to append streambed's stderr into
	OnRestart func(reason string)
	Logger    *slog.Logger
}

// Streambed is a supervisor for a single streambed sync subprocess.
type Streambed struct {
	cfg Config

	mu      sync.Mutex
	cmd     *exec.Cmd
	started bool
	stopped bool
}

// New returns a Streambed supervisor ready to Run.
func New(cfg Config) *Streambed {
	return &Streambed{cfg: cfg}
}

// Run blocks until ctx is cancelled. It launches streambed, waits for it to
// exit, records the reason, and restarts with an increasing backoff. The
// backoff resets to 1s on a clean restart that runs for >30s.
func (s *Streambed) Run(ctx context.Context) error {
	logFile, err := os.OpenFile(s.cfg.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open streambed log %s: %w", s.cfg.LogPath, err)
	}
	defer logFile.Close()

	backoff := 1 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		if ctx.Err() != nil {
			return nil
		}

		start := time.Now()
		exitErr := s.runOnce(ctx, logFile)
		ran := time.Since(start)

		if ctx.Err() != nil {
			return nil
		}

		reason := "exit-ok"
		if exitErr != nil {
			reason = fmt.Sprintf("exit-error: %v", exitErr)
		}

		// Reset backoff if the process ran long enough to suggest it was
		// actually healthy — a crash loop won't get to 30s, a normal SIGKILL
		// from the chaos injector almost certainly will.
		if ran > 30*time.Second {
			backoff = 1 * time.Second
		}

		s.cfg.Logger.Warn("streambed subprocess exited",
			"reason", reason, "ran", ran.Truncate(time.Millisecond),
			"next_restart_in", backoff)
		if s.cfg.OnRestart != nil {
			s.cfg.OnRestart(reason)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// runOnce launches one invocation of streambed and waits for it to exit.
func (s *Streambed) runOnce(ctx context.Context, logFile io.Writer) error {
	cmd := exec.CommandContext(ctx, s.cfg.Binary, s.cfg.Args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Send signals to the whole process group so a SIGKILL from Kill()
	// cleans up any subprocesses streambed may have spawned.
	cmd.SysProcAttr = procAttr()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start streambed: %w", err)
	}

	s.mu.Lock()
	s.cmd = cmd
	s.started = true
	s.mu.Unlock()

	s.cfg.Logger.Info("streambed subprocess started",
		"pid", cmd.Process.Pid, "binary", s.cfg.Binary)

	err := cmd.Wait()

	s.mu.Lock()
	s.cmd = nil
	s.mu.Unlock()

	return err
}

// Kill sends SIGKILL to the running streambed process (and its group).
// Safe to call when no process is running — it simply does nothing.
// The supervisor's main loop will observe the exit and restart.
func (s *Streambed) Kill() error {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	// Kill the whole process group.
	if err := killProcessGroup(cmd.Process.Pid); err != nil {
		// Fall back to killing just the process if group kill is unsupported.
		return cmd.Process.Signal(syscall.SIGKILL)
	}
	return nil
}
