package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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
}

// Server implements a Postgres-wire-compatible query interface backed by DuckDB.
// It serves Iceberg views or tables from an attached DuckLake catalog.
type Server struct {
	cfg      ServerConfig
	catalog  *TableCatalog
	duckDB   *sql.DB
	duckDBMu sync.Mutex
	logger   *slog.Logger
}

// NewServer creates a query server and initializes DuckDB for the selected
// lakehouse target.
func NewServer(cfg ServerConfig, s3Client storage.ObjectStorage, logger *slog.Logger) (*Server, error) {
	if cfg.TargetFormat == "" {
		cfg.TargetFormat = "iceberg"
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	var catalog *TableCatalog
	if cfg.TargetFormat == "ducklake" {
		// The query server owns an isolated, read-only DuckDB session so client
		// SQL cannot mutate the writer's DuckLake attachment.
		db.SetMaxOpenConns(1)
		if err := ducklake.Configure(context.Background(), db, ducklake.Config{
			CatalogPath:  cfg.DuckLakeCatalog,
			CatalogStore: cfg.DuckLakeCatalogStore,
			DataPath:     cfg.DuckLakeDataPath,
			S3Endpoint:   cfg.S3Endpoint,
			S3Region:     cfg.S3Region,
			ReadOnly:     true,
		}); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure ducklake: %w", err)
		}
		if err := configureDuckLakeQuerySession(db); err != nil {
			db.Close()
			return nil, err
		}
	} else {
		conn, _ := db.Conn(context.Background())
		// configure DuckDB on a specific connection
		configureDuckDBPerConn(context.Background(), conn, cfg)
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
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open fresh duckdb query session: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := ducklake.Configure(ctx, db, ducklake.Config{
		CatalogPath:  s.cfg.DuckLakeCatalog,
		CatalogStore: s.cfg.DuckLakeCatalogStore,
		DataPath:     s.cfg.DuckLakeDataPath,
		S3Endpoint:   s.cfg.S3Endpoint,
		S3Region:     s.cfg.S3Region,
		ReadOnly:     true,
	}); err != nil {
		db.Close()
		return fmt.Errorf("configure fresh ducklake query session: %w", err)
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
	// Match PostgreSQL's usual default schema while retaining DuckLake's main
	// schema as a fallback. Missing schemas in the search path are allowed.
	if _, err := db.Exec("SET search_path = 'streambed.public,streambed.main'"); err != nil {
		return fmt.Errorf("configure ducklake search path: %w", err)
	}
	return nil
}

// configureDuckDBPerConn configures DuckDB on a specific connection -- delete it
func configureDuckDBPerConn(ctx context.Context, con *sql.Conn, cfg ServerConfig) error {
	stmts := []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
		// icu provides timezone-aware operators like TIMESTAMPTZ - INTERVAL,
		// which Postgres clients expect (e.g. NOW() - INTERVAL '7 days').
		"INSTALL icu",
		"LOAD icu",
	}

	// Configure S3 access. Use GLOBAL scope so settings apply to every
	// connection in the sql.DB pool, not just the one that ran the SET.
	if cfg.S3Region != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_region = '%s'", cfg.S3Region))
	}
	if cfg.S3Endpoint != "" {
		// Strip protocol prefix for DuckDB — it expects host:port only
		endpoint := cfg.S3Endpoint
		endpoint = strings.TrimPrefix(endpoint, "http://")
		endpoint = strings.TrimPrefix(endpoint, "https://")
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_endpoint = '%s'", endpoint))
		stmts = append(stmts, "SET GLOBAL s3_url_style = 'path'")
		stmts = append(stmts, "SET GLOBAL s3_use_ssl = false")
	}

	// Use AWS credentials from environment.
	// For custom endpoints (MinIO), fall back to minioadmin defaults
	// to match the S3 client behavior in storage/s3.go.
	key := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" && cfg.S3Endpoint != "" {
		key = "minioadmin"
	}
	if secret == "" && cfg.S3Endpoint != "" {
		secret = "minioadmin"
	}
	if key != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_access_key_id = '%s'", key))
	}
	if secret != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_secret_access_key = '%s'", secret))
	}
	for _, stmt := range stmts {
		if _, err := con.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil

}

// configureDuckDB installs and loads the iceberg and httpfs extensions,
// then configures S3 credentials for accessing Iceberg data.
func configureDuckDB(db *sql.DB, cfg ServerConfig) error {
	stmts := []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
		// icu provides timezone-aware operators like TIMESTAMPTZ - INTERVAL,
		// which Postgres clients expect (e.g. NOW() - INTERVAL '7 days').
		"INSTALL icu",
		"LOAD icu",
	}

	// Configure S3 access. Use GLOBAL scope so settings apply to every
	// connection in the sql.DB pool, not just the one that ran the SET.
	if cfg.S3Region != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_region = '%s'", cfg.S3Region))
	}
	if cfg.S3Endpoint != "" {
		// Strip protocol prefix for DuckDB — it expects host:port only
		endpoint := cfg.S3Endpoint
		endpoint = strings.TrimPrefix(endpoint, "http://")
		endpoint = strings.TrimPrefix(endpoint, "https://")
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_endpoint = '%s'", endpoint))
		stmts = append(stmts, "SET GLOBAL s3_url_style = 'path'")
		stmts = append(stmts, "SET GLOBAL s3_use_ssl = false")
	}

	// Use AWS credentials from environment.
	// For custom endpoints (MinIO), fall back to minioadmin defaults
	// to match the S3 client behavior in storage/s3.go.
	key := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" && cfg.S3Endpoint != "" {
		key = "minioadmin"
	}
	if secret == "" && cfg.S3Endpoint != "" {
		secret = "minioadmin"
	}
	if key != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_access_key_id = '%s'", key))
	}
	if secret != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_secret_access_key = '%s'", secret))
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}

// Start begins listening for Postgres client connections and serving queries.
// Iceberg mode refreshes discovered views in the background; DuckLake mode uses
// its attached catalog directly. Start blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	// Iceberg tables must be discovered in S3 and exposed as views. DuckLake
	// tables are already visible through the directly attached catalog.
	if s.cfg.TargetFormat != "ducklake" {
		if err := s.refreshAndRegister(ctx); err != nil {
			s.logger.Warn("initial catalog refresh failed (will retry)", "error", err)
		}
		go s.refreshLoop(ctx)
	}

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

// handleParse is the psql-wire ParseFn. It receives a SQL query string and
// returns prepared statements that execute the query against DuckDB.
func (s *Server) handleParse(ctx context.Context, query string) (wire.PreparedStatements, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return wire.Prepared(wire.NewStatement(
			func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
				return writer.Complete("OK")
			},
		)), nil
	}

	s.logger.Debug("query received", "query", query)

	s.duckDBMu.Lock()
	defer s.duckDBMu.Unlock()
	if s.cfg.TargetFormat == "ducklake" {
		// Give every client query an isolated, read-only attachment. Besides
		// containing session mutations such as DETACH, reopening refreshes the
		// snapshot cached by DuckDB-backed metadata catalogs.
		if err := s.resetDuckLakeQueryDB(ctx); err != nil {
			return nil, err
		}
	}

	// Execute query against DuckDB
	rows, err := s.duckDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	// Read column metadata
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("column types: %w", err)
	}

	columns := make(wire.Columns, len(colTypes))
	for i, ct := range colTypes {
		columns[i] = wire.Column{
			Table: 0,
			Name:  ct.Name(),
			Oid:   duckDBTypeToOID(ct.DatabaseTypeName()),
			Width: 256,
		}
	}

	// Read all rows into memory so we can close the DuckDB result set
	// before streaming to the client.
	colCount := len(colTypes)
	var resultRows [][]any
	for rows.Next() {
		vals := make([]any, colCount)
		ptrs := make([]any, colCount)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan row: %w", err)
		}
		// Normalize DuckDB-specific value types that psql-wire can't encode directly.
		for i, v := range vals {
			vals[i] = normalizeValue(v, colTypes[i].DatabaseTypeName())
		}
		resultRows = append(resultRows, vals)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	rowCount := len(resultRows)
	handle := func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
		for rowIdx, row := range resultRows {
			if err := writer.Row(row); err != nil {
				s.logger.Error("write row failed",
					"row_index", rowIdx,
					"row", fmt.Sprintf("%v", row),
					"error", err,
				)
				return fmt.Errorf("write row: %w", err)
			}
		}
		return writer.Complete(fmt.Sprintf("SELECT %d", rowCount))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(columns))), nil
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
