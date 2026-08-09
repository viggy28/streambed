package state

import (
	"context"
	"testing"
	"time"
)

func TestTableCommitLockAcquireRelease(t *testing.T) {
	store, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lock, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Minute, 0); err == nil {
		t.Fatal("expected second acquire to time out")
	}
	if err := store.ReleaseTableCommitLock(ctx, lock); err != nil {
		t.Fatal(err)
	}
	lock2, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.ReleaseTableCommitLock(ctx, lock2)
}

func TestTableCommitLockExpiredCanBeStolen(t *testing.T) {
	store, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lock, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Nanosecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	stolen, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if stolen.Owner == lock.Owner {
		t.Fatal("expected new owner after stealing expired lock")
	}
	_ = store.ReleaseTableCommitLock(ctx, stolen)
}

func TestRefreshTableCommitLockRequiresCurrentOwner(t *testing.T) {
	store, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lock, err := store.AcquireTableCommitLock(ctx, "public", "t", "sync", time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := lock.Owner; len(got) < len("sync:") || got[:len("sync:")] != "sync:" {
		t.Fatalf("owner %q missing prefix", got)
	}
	ok, err := store.RefreshTableCommitLock(ctx, lock, time.Minute)
	if err != nil || !ok {
		t.Fatalf("refresh owner ok=%v err=%v", ok, err)
	}
	wrong := *lock
	wrong.Owner = "someone-else"
	ok, err = store.RefreshTableCommitLock(ctx, &wrong, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("wrong owner refreshed lock")
	}
	_ = store.ReleaseTableCommitLock(ctx, lock)
}

func TestReleaseTableCommitLockRequiresOwner(t *testing.T) {
	store, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lock, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	wrong := *lock
	wrong.Owner = "someone-else"
	if err := store.ReleaseTableCommitLock(ctx, &wrong); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireTableCommitLock(ctx, "public", "t", "test", time.Minute, 0); err == nil {
		t.Fatal("wrong owner released lock")
	}
	_ = store.ReleaseTableCommitLock(ctx, lock)
}
