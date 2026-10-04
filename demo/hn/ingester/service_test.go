package ingester

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

type fakeSource struct {
	lists        []map[string][]int64
	updates      [][]int64
	updateErrors []error
	poll         int
	itemRequests [][]int64
}

func (f *fakeSource) Lists(context.Context, int) (map[string][]int64, error) {
	return f.lists[f.poll], nil
}

func (f *fakeSource) Updates(context.Context) ([]int64, error) {
	updates := f.updates[f.poll]
	var err error
	if f.poll < len(f.updateErrors) {
		err = f.updateErrors[f.poll]
	}
	return updates, err
}

func (f *fakeSource) Items(_ context.Context, ids []int64) ([]Item, error) {
	copied := append([]int64(nil), ids...)
	f.itemRequests = append(f.itemRequests, copied)
	items := make([]Item, 0, len(ids))
	for _, id := range ids {
		items = append(items, Item{ID: id, Type: "story"})
	}
	f.poll++
	return items, nil
}

type fakeSink struct {
	applied int
}

func (f *fakeSink) Apply(context.Context, map[string][]int64, []Item, time.Time) error {
	f.applied++
	return nil
}

func TestServiceFetchesInitialThenChangedAndFrontPageItems(t *testing.T) {
	source := &fakeSource{
		lists: []map[string][]int64{
			{"top": {1, 2}, "best": {2, 3}},
			{"top": {2, 4}, "best": {3}},
		},
		updates: [][]int64{{2}, {3}},
	}
	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, err := NewService(source, sink, logger, 10, 2)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := [][]int64{{1, 2, 3}, {2, 3, 4}}
	if !reflect.DeepEqual(source.itemRequests, want) {
		t.Fatalf("item requests = %v, want %v", source.itemRequests, want)
	}
	if sink.applied != 2 {
		t.Fatalf("sink applied %d polls, want 2", sink.applied)
	}
}

func TestServiceContinuesWhenUpdatesEndpointFails(t *testing.T) {
	source := &fakeSource{
		lists:        []map[string][]int64{{"top": {1}}},
		updates:      [][]int64{nil},
		updateErrors: []error{errors.New("temporary failure")},
	}
	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, err := NewService(source, sink, logger, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source.itemRequests, [][]int64{{1}}) {
		t.Fatalf("item requests = %v, want [[1]]", source.itemRequests)
	}
}
