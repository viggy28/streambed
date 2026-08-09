package failpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

var ErrInjected = errors.New("failpoint injected failure")

type action struct {
	err      error
	blockCh  chan struct{}
	hitCh    chan struct{}
	hitOnce  sync.Once
	released bool
}

type Handle struct {
	name string
}

var registry = struct {
	sync.Mutex
	actions map[string]*action
}{actions: map[string]*action{}}

func EnableError(name string, err error) {
	if err == nil {
		err = ErrInjected
	}
	registry.Lock()
	defer registry.Unlock()
	registry.actions[name] = &action{err: err, hitCh: make(chan struct{})}
}

func EnableBlock(name string) *Handle {
	registry.Lock()
	defer registry.Unlock()
	registry.actions[name] = &action{blockCh: make(chan struct{}), hitCh: make(chan struct{})}
	return &Handle{name: name}
}

func Disable(name string) {
	registry.Lock()
	defer registry.Unlock()
	if a := registry.actions[name]; a != nil && a.blockCh != nil && !a.released {
		close(a.blockCh)
	}
	delete(registry.actions, name)
}

func DisableAll() {
	registry.Lock()
	defer registry.Unlock()
	for _, a := range registry.actions {
		if a.blockCh != nil && !a.released {
			close(a.blockCh)
		}
	}
	registry.actions = map[string]*action{}
}

func (h *Handle) Release() {
	if h == nil {
		return
	}
	registry.Lock()
	defer registry.Unlock()
	a := registry.actions[h.name]
	if a == nil || a.blockCh == nil || a.released {
		return
	}
	a.released = true
	close(a.blockCh)
}

func (h *Handle) WaitHit(ctx context.Context) error {
	if h == nil {
		return nil
	}
	registry.Lock()
	a := registry.actions[h.name]
	registry.Unlock()
	if a == nil {
		return fmt.Errorf("failpoint %q is not enabled", h.name)
	}
	select {
	case <-a.hitCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func Check(ctx context.Context, name string) error {
	registry.Lock()
	a := registry.actions[name]
	registry.Unlock()

	if a == nil && envEnabled(name) {
		return fmt.Errorf("%w: %s", ErrInjected, name)
	}
	if a == nil {
		return nil
	}
	a.hitOnce.Do(func() { close(a.hitCh) })
	if a.blockCh != nil {
		select {
		case <-a.blockCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if a.err != nil {
		return fmt.Errorf("%w: %s", a.err, name)
	}
	return nil
}

func envEnabled(name string) bool {
	for _, raw := range strings.Split(os.Getenv("STREAMBED_FAILPOINTS"), ",") {
		if strings.TrimSpace(raw) == name {
			return true
		}
	}
	return false
}
