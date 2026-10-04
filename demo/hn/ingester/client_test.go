package ingester

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestClientFetchesListsUpdatesAndItems(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "stories.json"):
			fmt.Fprint(w, `[3,2,1]`)
		case r.URL.Path == "/updates.json":
			fmt.Fprint(w, `{"items":[2],"profiles":[],"future_field":true}`)
		case r.URL.Path == "/item/1.json":
			fmt.Fprint(w, `{"id":1,"type":"story","title":"one","score":10,"descendants":2,"unknown":"ignored"}`)
		case r.URL.Path == "/item/2.json":
			fmt.Fprint(w, `{"id":2,"type":"story","title":"two","score":20,"descendants":4}`)
		default:
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := NewClient(server.URL, 2)
	if err != nil {
		t.Fatal(err)
	}
	lists, err := client.Lists(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lists) != len(ListEndpoints) {
		t.Fatalf("got %d lists, want %d", len(lists), len(ListEndpoints))
	}
	if got, want := lists["top"], []int64{3, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("top list = %v, want %v", got, want)
	}

	updates, err := client.Updates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(updates, []int64{2}) {
		t.Fatalf("updates = %v, want [2]", updates)
	}

	items, err := client.Items(context.Background(), []int64{2, 1})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if len(items) != 2 || items[0].Title != "one" || items[1].Score != 20 {
		t.Fatalf("unexpected items: %+v", items)
	}
}
