package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/viggy28/streambed/internal/iceberg"
)

func newHTTPTestServer(t *testing.T) *Server {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Server{
		cfg: ServerConfig{
			TargetFormat:   "iceberg",
			QueryTimeout:   time.Second,
			MaxResultRows:  10,
			MaxResultBytes: 1 << 20,
		},
		duckDB: db,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestHTTPQuery(t *testing.T) {
	srv := newHTTPTestServer(t)
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"sql":"SELECT 1 AS answer"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	srv.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result QueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.RowCount != 1 || len(result.Columns) != 1 || result.Columns[0].Name != "answer" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := result.Rows[0][0]; got != float64(1) {
		t.Fatalf("answer = %#v, want 1", got)
	}
}

func TestHTTPQueryRejectsUnsafeSQL(t *testing.T) {
	srv := newHTTPTestServer(t)
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"sql":"DROP TABLE stories"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	srv.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "only SELECT") {
		t.Fatalf("unexpected response: %s", response.Body.String())
	}
}

func TestHTTPQueryRejectsMalformedRequests(t *testing.T) {
	srv := newHTTPTestServer(t)
	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "content type", contentType: "text/plain", body: `{}`, wantStatus: http.StatusUnsupportedMediaType},
		{name: "unknown field", contentType: "application/json", body: `{"sql":"SELECT 1","extra":true}`, wantStatus: http.StatusBadRequest},
		{name: "two objects", contentType: "application/json", body: `{"sql":"SELECT 1"}{"sql":"SELECT 2"}`, wantStatus: http.StatusBadRequest},
		{name: "empty query", contentType: "application/json", body: `{"sql":""}`, wantStatus: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(test.body))
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			srv.HTTPHandler().ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.wantStatus, response.Body.String())
			}
		})
	}
}

type fakeSnapshotLister struct {
	snapshots []iceberg.SnapshotInfo
	err       error
}

func (f fakeSnapshotLister) ListSnapshots(context.Context, string, string) ([]iceberg.SnapshotInfo, error) {
	return f.snapshots, f.err
}

func TestHTTPSnapshots(t *testing.T) {
	srv := newHTTPTestServer(t)
	timestamp := time.Date(2026, time.October, 8, 12, 30, 0, 0, time.UTC)
	srv.snapshotLister = fakeSnapshotLister{snapshots: []iceberg.SnapshotInfo{{
		SnapshotID: 42, SequenceNumber: 7, Timestamp: timestamp,
	}}}
	request := httptest.NewRequest(http.MethodGet, "/snapshots?table=public.front_page", nil)
	response := httptest.NewRecorder()

	srv.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result struct {
		Table     string `json:"table"`
		Snapshots []struct {
			SnapshotID int64     `json:"snapshot_id"`
			Timestamp  time.Time `json:"timestamp"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Table != "public.front_page" || len(result.Snapshots) != 1 || result.Snapshots[0].SnapshotID != 42 || !result.Snapshots[0].Timestamp.Equal(timestamp) {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHTTPSnapshotsRejectsUnsafeTable(t *testing.T) {
	srv := newHTTPTestServer(t)
	srv.snapshotLister = fakeSnapshotLister{}
	for _, table := range []string{"front_page", "public.front-page", "public.front_page;DROP TABLE stories"} {
		request := httptest.NewRequest(http.MethodGet, "/snapshots?table="+url.QueryEscape(table), nil)
		response := httptest.NewRecorder()
		srv.HTTPHandler().ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("table %q status = %d, body = %s", table, response.Code, response.Body.String())
		}
	}
}

func TestHTTPHealth(t *testing.T) {
	srv := newHTTPTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()
	srv.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
}

func TestExecuteHonorsCancelledContext(t *testing.T) {
	srv := newHTTPTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := srv.Execute(ctx, "SELECT sum(i) FROM range(1000000000) AS values(i)")
	if err == nil {
		t.Fatal("cancelled query unexpectedly succeeded")
	}
}
