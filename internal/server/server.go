package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/storage"
)

// ServerConfig holds configuration for the query server.
type ServerConfig struct {
	ListenAddr           string
	S3Bucket             string
	S3Prefix             string
	S3Endpoint           string
	S3Region             string
	TargetFormat         string
	DuckLakeCatalog      string
	DuckLakeCatalogStore string
	DuckLakeDataPath     string
	DuckLakeExtension    string
	QueryTimeout         time.Duration
	MaxResultRows        int
	MaxResultBytes       int64
	QueryMemoryLimitMB   int
}

const (
	defaultQueryTimeout   = 10 * time.Second
	defaultMaxResultRows  = 1000
	defaultMaxResultBytes = 8 << 20
	maxQueryBytes         = 64 << 10
)

// Server implements a Postgres-wire-compatible query interface backed by DuckDB.
// It serves Iceberg views or tables from an attached DuckLake catalog.
type Server struct {
	cfg      ServerConfig
	catalog  *TableCatalog
	duckDB   *sql.DB
	duckDBMu sync.Mutex
	logger   *slog.Logger
}

// QueryColumn describes one column in an HTTP query result.
type QueryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryResult is the transport-neutral result of a guarded DuckDB query.
type QueryResult struct {
	Columns  []QueryColumn `json:"columns"`
	Rows     [][]any       `json:"rows"`
	RowCount int           `json:"row_count"`
}

// NewServer creates a query server and initializes DuckDB for the selected
// lakehouse target.
func NewServer(cfg ServerConfig, s3Client storage.ObjectStorage, logger *slog.Logger) (*Server, error) {
	if cfg.TargetFormat == "" {
		cfg.TargetFormat = "iceberg"
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = defaultQueryTimeout
	}
	if cfg.MaxResultRows <= 0 {
		cfg.MaxResultRows = defaultMaxResultRows
	}
	if cfg.MaxResultBytes <= 0 {
		cfg.MaxResultBytes = defaultMaxResultBytes
	}
	if cfg.QueryMemoryLimitMB <= 0 {
		cfg.QueryMemoryLimitMB = 256
	}
	var db *sql.DB
	var err error
	var catalog *TableCatalog
	if cfg.TargetFormat == "ducklake" {
		// The query server owns an isolated, read-only DuckDB session so client
		// SQL cannot mutate the writer's DuckLake attachment.
		db, err = ducklake.Open(context.Background(), ducklake.Config{
			CatalogPath:   cfg.DuckLakeCatalog,
			CatalogStore:  cfg.DuckLakeCatalogStore,
			DataPath:      cfg.DuckLakeDataPath,
			S3Endpoint:    cfg.S3Endpoint,
			ExtensionPath: cfg.DuckLakeExtension,
			S3Region:      cfg.S3Region,
			ReadOnly:      true,
		})
		if err != nil {
			return nil, fmt.Errorf("configure ducklake: %w", err)
		}
		if err := configureDuckLakeQuerySession(db); err != nil {
			db.Close()
			return nil, err
		}
	} else {
		db, err = sql.Open("duckdb", "")
		if err != nil {
			return nil, fmt.Errorf("open duckdb: %w", err)
		}
		// Query execution is serialized, so keep one configured DuckDB connection.
		db.SetMaxOpenConns(1)
		if err := configureDuckDB(db, cfg); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure duckdb: %w", err)
		}
		catalog = NewTableCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix, logger)
	}

	return &Server{
		cfg:     cfg,
		catalog: catalog,
		duckDB:  db,
		logger:  logger,
	}, nil
}

func (s *Server) resetDuckLakeQueryDB(ctx context.Context) error {
	db, err := ducklake.Open(ctx, ducklake.Config{
		CatalogPath:   s.cfg.DuckLakeCatalog,
		CatalogStore:  s.cfg.DuckLakeCatalogStore,
		DataPath:      s.cfg.DuckLakeDataPath,
		S3Endpoint:    s.cfg.S3Endpoint,
		ExtensionPath: s.cfg.DuckLakeExtension,
		S3Region:      s.cfg.S3Region,
		ReadOnly:      true,
	})
	if err != nil {
		return fmt.Errorf("configure fresh duckdb query session: %w", err)
	}
	if err := configureDuckLakeQuerySession(db); err != nil {
		db.Close()
		return err
	}
	oldDB := s.duckDB
	s.duckDB = db
	return oldDB.Close()
}

func configureDuckLakeQuerySession(db *sql.DB) error {
	if _, err := db.Exec("USE streambed"); err != nil {
		return fmt.Errorf("use ducklake catalog: %w", err)
	}

	// A new DuckLake catalog contains main, but public is only created when the
	// writer first sees a PostgreSQL relation. DuckDB rejects a search path that
	// names a missing catalog schema, so include public only after it exists.
	// DuckLake query sessions are recreated before every client query, which
	// makes public the preferred schema as soon as the writer creates it.
	var publicExists bool
	if err := db.QueryRow(`
		SELECT count(*) > 0
		FROM information_schema.schemata
		WHERE catalog_name = 'streambed' AND schema_name = 'public'
	`).Scan(&publicExists); err != nil {
		return fmt.Errorf("check ducklake public schema: %w", err)
	}
	searchPath := "streambed.main"
	if publicExists {
		searchPath = "streambed.public,streambed.main"
	}
	if _, err := db.Exec("SET search_path = '" + searchPath + "'"); err != nil {
		return fmt.Errorf("configure ducklake search path: %w", err)
	}
	return nil
}

// configureDuckDB installs the required extensions, configures scoped S3
// access, and locks down the engine before it receives untrusted client SQL.
func configureDuckDB(db *sql.DB, cfg ServerConfig) error {
	for i, stmt := range duckDBConfigStatements(cfg) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("configure duckdb statement %d: %w", i+1, err)
		}
	}
	return nil
}

func duckDBConfigStatements(cfg ServerConfig) []string {
	memoryLimitMB := cfg.QueryMemoryLimitMB
	if memoryLimitMB <= 0 {
		memoryLimitMB = 256
	}
	stmts := []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
		// icu provides timezone-aware operators like TIMESTAMPTZ - INTERVAL,
		// which Postgres clients expect (e.g. NOW() - INTERVAL '7 days').
		"INSTALL icu",
		"LOAD icu",
		"SET TimeZone = 'UTC'",
		fmt.Sprintf("SET memory_limit = '%dMB'", memoryLimitMB),
		"SET threads = 2",
		"SET allow_community_extensions = false",
		"SET allow_unsigned_extensions = false",
	}

	key := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" && cfg.S3Endpoint != "" {
		key = "minioadmin"
	}
	if secret == "" && cfg.S3Endpoint != "" {
		secret = "minioadmin"
	}
	if key != "" && secret != "" {
		options := []string{
			"TYPE S3",
			"KEY_ID " + sqlString(key),
			"SECRET " + sqlString(secret),
		}
		if token := os.Getenv("AWS_SESSION_TOKEN"); token != "" {
			options = append(options, "SESSION_TOKEN "+sqlString(token))
		}
		if cfg.S3Region != "" {
			options = append(options, "REGION "+sqlString(cfg.S3Region))
		}
		if cfg.S3Endpoint != "" {
			useSSL := strings.HasPrefix(strings.ToLower(cfg.S3Endpoint), "https://")
			endpoint := strings.TrimPrefix(cfg.S3Endpoint, "http://")
			endpoint = strings.TrimPrefix(endpoint, "https://")
			options = append(options,
				"ENDPOINT "+sqlString(strings.TrimSuffix(endpoint, "/")),
				"URL_STYLE 'path'",
				fmt.Sprintf("USE_SSL %t", useSSL),
			)
		}
		stmts = append(stmts, "CREATE OR REPLACE SECRET streambed_s3 ("+strings.Join(options, ", ")+")")
	}

	allowedPrefix := "s3://" + cfg.S3Bucket + "/" + strings.TrimPrefix(cfg.S3Prefix, "/")
	if !strings.HasSuffix(allowedPrefix, "/") {
		allowedPrefix += "/"
	}
	allowedDirectories := []string{sqlString(allowedPrefix)}
	if home, err := os.UserHomeDir(); err == nil {
		allowedDirectories = append(allowedDirectories, sqlString(strings.TrimSuffix(home, "/")+"/.duckdb/extensions/"))
	}
	stmts = append(stmts,
		"SET allowed_directories = ["+strings.Join(allowedDirectories, ", ")+"]",
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	)
	return stmts
}

func sqlString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// Start begins listening for Postgres client connections and serving queries.
// Iceberg mode refreshes discovered views in the background; DuckLake mode uses
// its attached catalog directly. Start blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	s.startCatalogRefresh(ctx)

	// Create psql-wire server
	srv, err := wire.NewServer(s.handleParse,
		wire.Logger(s.logger),
		// pgx requires this Postgres ParameterStatus before it will use the
		// simple query protocol. DuckDB also follows standard string escaping.
		wire.GlobalParameters(wire.Parameters{
			wire.ParameterStatus("standard_conforming_strings"): "on",
		}),
	)
	if err != nil {
		return fmt.Errorf("create wire server: %w", err)
	}

	// Shut down the wire server when context is cancelled
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	s.logger.Info("query server starting", "addr", s.cfg.ListenAddr)
	if err := srv.ListenAndServe(s.cfg.ListenAddr); err != nil {
		// Ignore errors from shutdown
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}

func (s *Server) startCatalogRefresh(ctx context.Context) {
	// Iceberg tables must be discovered in S3 and exposed as views. DuckLake
	// tables are already visible through the directly attached catalog.
	if s.cfg.TargetFormat == "ducklake" {
		return
	}
	if err := s.refreshAndRegister(ctx); err != nil {
		s.logger.Warn("initial catalog refresh failed (will retry)", "error", err)
	}
	go s.refreshLoop(ctx)
}

// StartHTTP starts the guarded JSON-over-HTTP query API. It blocks until the
// context is cancelled or the HTTP server fails.
func (s *Server) StartHTTP(ctx context.Context, addr string) error {
	s.startCatalogRefresh(ctx)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           s.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	s.logger.Info("HTTP query server starting", "addr", addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}

// HTTPHandler returns the public HTTP query API handler.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /query", s.handleHTTPQuery)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleHTTPQuery(w http.ResponseWriter, r *http.Request) {
	if contentType := r.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxQueryBytes+1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		SQL string `json:"sql"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request must contain one JSON object"})
		return
	}
	if err := validateReadOnlyQuery(request.SQL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := s.Execute(r.Context(), request.SQL)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "query timed out"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("extra JSON value")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// handleParse is the psql-wire ParseFn. It receives a SQL query string and
// returns prepared statements that stream a guarded query result.
func (s *Server) handleParse(ctx context.Context, query string) (wire.PreparedStatements, error) {
	if strings.TrimSpace(query) == "" {
		return wire.Prepared(wire.NewStatement(
			func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
				return writer.Complete("OK")
			},
		)), nil
	}

	result, err := s.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	columns := make(wire.Columns, len(result.Columns))
	for i, column := range result.Columns {
		columns[i] = wire.Column{
			Table: 0,
			Name:  column.Name,
			Oid:   duckDBTypeToOID(column.Type),
			Width: 256,
		}
	}

	handle := func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
		for rowIdx, row := range result.Rows {
			if err := writer.Row(row); err != nil {
				s.logger.Error("write row failed", "row_index", rowIdx, "error", err)
				return fmt.Errorf("write row: %w", err)
			}
		}
		return writer.Complete(fmt.Sprintf("SELECT %d", result.RowCount))
	}
	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(columns))), nil
}

// Execute validates and runs one bounded, read-only query.
func (s *Server) Execute(ctx context.Context, query string) (*QueryResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}
	if err := validateReadOnlyQuery(query); err != nil {
		return nil, err
	}

	s.logger.Debug("query received", "query", query)
	queryCtx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()

	preparedQuery, err := prepareTimeTravelQuery(queryCtx, query, s.cfg.TargetFormat, s.catalog)
	if err != nil {
		return nil, fmt.Errorf("time travel query: %w", err)
	}

	s.duckDBMu.Lock()
	defer s.duckDBMu.Unlock()
	if s.cfg.TargetFormat == "ducklake" {
		// Give every client query an isolated, read-only attachment. Besides
		// containing session mutations such as DETACH, reopening refreshes the
		// snapshot cached by DuckDB-backed metadata catalogs.
		if err := s.resetDuckLakeQueryDB(queryCtx); err != nil {
			return nil, err
		}
	}

	rows, err := s.duckDB.QueryContext(queryCtx, preparedQuery)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}
	columns := make([]QueryColumn, len(colTypes))
	for i, column := range colTypes {
		columns[i] = QueryColumn{Name: column.Name(), Type: column.DatabaseTypeName()}
	}

	resultRows := make([][]any, 0)
	var resultBytes int64
	for rows.Next() {
		if len(resultRows) >= s.cfg.MaxResultRows {
			return nil, fmt.Errorf("query result exceeds the %d row limit", s.cfg.MaxResultRows)
		}
		values := make([]any, len(colTypes))
		pointers := make([]any, len(colTypes))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		for i, value := range values {
			values[i] = normalizeValue(value, colTypes[i].DatabaseTypeName())
			resultBytes += approximateValueBytes(values[i])
		}
		if resultBytes > s.cfg.MaxResultBytes {
			return nil, fmt.Errorf("query result exceeds the %d byte limit", s.cfg.MaxResultBytes)
		}
		resultRows = append(resultRows, values)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return &QueryResult{Columns: columns, Rows: resultRows, RowCount: len(resultRows)}, nil
}

func validateReadOnlyQuery(query string) error {
	if len(query) > maxQueryBytes {
		return fmt.Errorf("query exceeds the %d byte limit", maxQueryBytes)
	}
	trimmed, err := trimLeadingSQLComments(query)
	if err != nil {
		return err
	}
	keywordEnd := 0
	for keywordEnd < len(trimmed) {
		c := trimmed[keywordEnd]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			break
		}
		keywordEnd++
	}
	firstKeyword := strings.ToUpper(trimmed[:keywordEnd])
	if firstKeyword != "SELECT" && firstKeyword != "WITH" {
		return fmt.Errorf("only SELECT statements are allowed")
	}
	forbidden := map[string]struct{}{
		"ALTER": {}, "ATTACH": {}, "CALL": {}, "COPY": {}, "CREATE": {},
		"DELETE": {}, "DETACH": {}, "DROP": {}, "EXPORT": {}, "IMPORT": {},
		"INSERT": {}, "INSTALL": {}, "LOAD": {}, "MERGE": {}, "PRAGMA": {},
		"SET": {}, "TRUNCATE": {}, "UPDATE": {}, "VACUUM": {},
	}
	for _, keyword := range unquotedSQLKeywords(query) {
		if _, blocked := forbidden[keyword]; blocked {
			return fmt.Errorf("only read-only SELECT statements are allowed")
		}
	}
	if err := rejectMultipleStatements(query); err != nil {
		return err
	}
	return nil
}

func unquotedSQLKeywords(query string) []string {
	const (
		sqlNormal = iota
		sqlSingleQuote
		sqlDoubleQuote
		sqlLineComment
		sqlBlockComment
	)
	state := sqlNormal
	var keywords []string
	for i := 0; i < len(query); {
		switch state {
		case sqlNormal:
			switch {
			case query[i] == '\'':
				state = sqlSingleQuote
				i++
			case query[i] == '"':
				state = sqlDoubleQuote
				i++
			case query[i] == '-' && i+1 < len(query) && query[i+1] == '-':
				state = sqlLineComment
				i += 2
			case query[i] == '/' && i+1 < len(query) && query[i+1] == '*':
				state = sqlBlockComment
				i += 2
			case (query[i] >= 'a' && query[i] <= 'z') || (query[i] >= 'A' && query[i] <= 'Z'):
				start := i
				for i < len(query) && ((query[i] >= 'a' && query[i] <= 'z') || (query[i] >= 'A' && query[i] <= 'Z') || query[i] == '_') {
					i++
				}
				keywords = append(keywords, strings.ToUpper(query[start:i]))
			default:
				i++
			}
		case sqlSingleQuote:
			if query[i] == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					i += 2
				} else {
					state = sqlNormal
					i++
				}
			} else {
				i++
			}
		case sqlDoubleQuote:
			if query[i] == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					i += 2
				} else {
					state = sqlNormal
					i++
				}
			} else {
				i++
			}
		case sqlLineComment:
			if query[i] == '\n' {
				state = sqlNormal
			}
			i++
		case sqlBlockComment:
			if query[i] == '*' && i+1 < len(query) && query[i+1] == '/' {
				state = sqlNormal
				i += 2
			} else {
				i++
			}
		}
	}
	return keywords
}

func trimLeadingSQLComments(query string) (string, error) {
	for {
		query = strings.TrimLeft(query, " \t\r\n\f\v")
		switch {
		case strings.HasPrefix(query, "--"):
			newline := strings.IndexByte(query, '\n')
			if newline == -1 {
				return "", fmt.Errorf("only SELECT statements are allowed")
			}
			query = query[newline+1:]
		case strings.HasPrefix(query, "/*"):
			end := strings.Index(query[2:], "*/")
			if end == -1 {
				return "", fmt.Errorf("unterminated SQL comment")
			}
			query = query[end+4:]
		default:
			return query, nil
		}
	}
}

func rejectMultipleStatements(query string) error {
	const (
		sqlNormal = iota
		sqlSingleQuote
		sqlDoubleQuote
		sqlLineComment
		sqlBlockComment
	)
	state := sqlNormal
	for i := 0; i < len(query); i++ {
		switch state {
		case sqlNormal:
			switch {
			case query[i] == '\'':
				state = sqlSingleQuote
			case query[i] == '"':
				state = sqlDoubleQuote
			case query[i] == '-' && i+1 < len(query) && query[i+1] == '-':
				state = sqlLineComment
				i++
			case query[i] == '/' && i+1 < len(query) && query[i+1] == '*':
				state = sqlBlockComment
				i++
			case query[i] == ';':
				rest, err := trimLeadingSQLComments(query[i+1:])
				if err != nil || strings.TrimSpace(rest) != "" {
					return fmt.Errorf("multiple SQL statements are not allowed")
				}
				return nil
			}
		case sqlSingleQuote:
			if query[i] == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
				} else {
					state = sqlNormal
				}
			}
		case sqlDoubleQuote:
			if query[i] == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
				} else {
					state = sqlNormal
				}
			}
		case sqlLineComment:
			if query[i] == '\n' {
				state = sqlNormal
			}
		case sqlBlockComment:
			if query[i] == '*' && i+1 < len(query) && query[i+1] == '/' {
				state = sqlNormal
				i++
			}
		}
	}
	return nil
}

func approximateValueBytes(value any) int64 {
	switch value := value.(type) {
	case nil:
		return 0
	case string:
		return int64(len(value))
	case []byte:
		return int64(len(value))
	default:
		return int64(len(fmt.Sprint(value)))
	}
}

// refreshAndRegister refreshes the table catalog and re-registers DuckDB views.
// If DuckDB's engine was invalidated by a FATAL error (e.g., corrupt Iceberg
// table), it re-opens the DuckDB instance and retries view registration.
func (s *Server) refreshAndRegister(ctx context.Context) error {
	s.duckDBMu.Lock()
	defer s.duckDBMu.Unlock()
	if err := s.catalog.Refresh(ctx); err != nil {
		return err
	}
	err := s.catalog.RegisterViews(s.duckDB)
	if err != ErrDuckDBFatal {
		return err
	}

	// DuckDB engine was fatally invalidated — re-open it.
	s.logger.Warn("re-opening DuckDB after fatal error")
	s.duckDB.Close()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("re-open duckdb: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := configureDuckDB(db, s.cfg); err != nil {
		db.Close()
		return fmt.Errorf("re-configure duckdb: %w", err)
	}
	s.duckDB = db

	// Retry view registration with the fresh engine.
	return s.catalog.RegisterViews(s.duckDB)
}

// refreshLoop periodically refreshes the catalog and re-registers views.
func (s *Server) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.refreshAndRegister(ctx); err != nil {
				s.logger.Warn("catalog refresh failed", "error", err)
			}
		}
	}
}

// Close shuts down the DuckDB connection.
func (s *Server) Close() error {
	s.duckDBMu.Lock()
	defer s.duckDBMu.Unlock()
	return s.duckDB.Close()
}

// duckDBTypeToOID maps DuckDB type names to Postgres type OIDs.
// These OIDs tell the Postgres client how to interpret column values.
func duckDBTypeToOID(typeName string) uint32 {
	switch strings.ToUpper(typeName) {
	case "BOOLEAN", "BOOL":
		return 16 // bool
	case "SMALLINT", "INT2", "TINYINT":
		return 21 // int2
	case "INTEGER", "INT4", "INT":
		return 23 // int4
	case "BIGINT", "INT8":
		return 20 // int8
	case "REAL", "FLOAT", "FLOAT4":
		return 700 // float4
	case "DOUBLE", "FLOAT8":
		return 701 // float8
	case "DATE":
		return 1082 // date
	case "TIMESTAMP":
		return 1114 // timestamp
	case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ":
		return 1184 // timestamptz
	case "UUID":
		return 2950 // uuid
	case "BLOB", "BYTEA":
		return 17 // bytea
	case "VARCHAR", "TEXT", "STRING":
		return 25 // text
	case "DECIMAL", "NUMERIC":
		return 1700 // numeric
	default:
		return 25 // default to text
	}
}

// normalizeValue converts DuckDB-specific value types into plain Go types
// that psql-wire's text encoder can handle. Anything it doesn't recognize
// is returned unchanged.
func normalizeValue(v any, databaseType string) any {
	switch x := v.(type) {
	case duckdb.Decimal:
		return x.String()
	case *duckdb.Decimal:
		if x == nil {
			return nil
		}
		return x.String()
	case []byte:
		if strings.EqualFold(databaseType, "UUID") && len(x) == 16 {
			if id, err := uuid.FromBytes(x); err == nil {
				return id.String()
			}
		}
		return x
	case duckdb.UUID:
		return uuid.UUID(x).String()
	case *duckdb.UUID:
		if x == nil {
			return nil
		}
		return uuid.UUID(*x).String()
	default:
		return v
	}
}
