package storage

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"time"

	"github.com/aws/smithy-go"
)

const (
	maxRetries     = 5
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 10 * time.Second
)

// retryWithBackoff retries op on transient errors with exponential backoff
// and jitter. Non-retryable errors and context cancellations are returned
// immediately.
func retryWithBackoff(ctx context.Context, op func() error) error {
	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if attempt < maxRetries {
			jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}
	return lastErr
}

// isRetryable returns true for transient errors worth retrying: server
// errors (5xx), throttling, timeouts, and temporary network errors.
func isRetryable(err error) bool {
	// Network errors (connection refused, DNS, timeouts).
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// AWS SDK API errors with HTTP status codes.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		// Throttling / rate limit codes.
		switch code {
		case "SlowDown", "Throttling", "ThrottlingException",
			"RequestThrottled", "TooManyRequestsException",
			"ServiceUnavailable", "InternalError":
			return true
		}
	}

	// Smithy-level HTTP response errors.
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) {
		status := respErr.HTTPStatusCode()
		return status >= 500 || status == 429
	}

	return false
}
