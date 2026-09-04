//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/storage"
)

type cdcOracleTarget string

const (
	cdcOracleIceberg  cdcOracleTarget = "iceberg"
	cdcOracleDuckLake cdcOracleTarget = "ducklake"
)

type oracleTableRef struct {
	Schema     string
	Table      string
	KeyColumns []string
}

type oracleColumn struct {
	Name string
	OID  uint32
}

type oracleValue struct {
	Null      bool
	Canonical string
}

func (v oracleValue) String() string {
	if v.Null {
		return "NULL"
	}
	return strconv.Quote(v.Canonical)
}

type oracleRows map[string]map[string]oracleValue

type oracleRowSet struct {
	Columns []oracleColumn
	Rows    oracleRows
}

type oracleValueSource uint8

const (
	oraclePostgresText oracleValueSource = iota
	oracleTargetValue
)

// cdcTargetReader reads a lakehouse target directly. It deliberately does not
// use Streambed's Postgres-wire query server or its registered views.
type cdcTargetReader interface {
	FlushLSN(context.Context, oracleTableRef) (pglogrepl.LSN, error)
	ReadRows(context.Context, oracleTableRef, []oracleColumn) (oracleRows, error)
	Close() error
}

type directTargetReader struct {
	name      cdcOracleTarget
	db        *sql.DB
	tableExpr func(oracleTableRef) string
	readLSN   func(context.Context, oracleTableRef) (string, bool, error)
}

func (r *directTargetReader) Close() error { return r.db.Close() }

func (r *directTargetReader) FlushLSN(ctx context.Context, table oracleTableRef) (pglogrepl.LSN, error) {
	raw, found, err := r.readLSN(ctx, table)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("%s table %s.%s has no durable flush LSN", r.name, table.Schema, table.Table)
	}
	lsn, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s table %s.%s flush LSN %q: %w", r.name, table.Schema, table.Table, raw, err)
	}
	return lsn, nil
}

func (r *directTargetReader) ReadRows(ctx context.Context, table oracleTableRef, columns []oracleColumn) (oracleRows, error) {
	return queryDirectTargetRows(ctx, r.db, r.name, r.tableExpr(table), table.KeyColumns, columns)
}

type cdcProcessOutput struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *cdcProcessOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *cdcProcessOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type cdcSyncProcess struct {
	cmd     *exec.Cmd
	output  *cdcProcessOutput
	done    chan struct{}
	waitErr error
}

func startCDCSyncProcess(t *testing.T, binary string, args []string) *cdcSyncProcess {
	t.Helper()
	output := &cdcProcessOutput{}
	cmd := exec.Command(binary, args...)
	cmd.Env = append(os.Environ(), "STREAMBED_QUERY_ADDR=")
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start Streambed sync: %v", err)
	}
	process := &cdcSyncProcess{cmd: cmd, output: output, done: make(chan struct{})}
	go func() {
		process.waitErr = cmd.Wait()
		close(process.done)
	}()
	return process
}

func (p *cdcSyncProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		if p.waitErr != nil {
			t.Fatalf("Streambed sync exited unexpectedly: %v\n%s", p.waitErr, p.output.String())
		}
		return
	default:
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("stop Streambed sync: %v", err)
	}
	select {
	case <-p.done:
		if p.waitErr != nil {
			t.Fatalf("Streambed sync shutdown failed: %v\n%s", p.waitErr, p.output.String())
		}
	case <-time.After(20 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Fatalf("Streambed sync did not stop within 20s\n%s", p.output.String())
	}
}

func (p *cdcSyncProcess) kill() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}

func TestCDCOracleNormalization(t *testing.T) {
	nullValue, err := normalizeOracleValue(25, nil, oraclePostgresText)
	if err != nil {
		t.Fatal(err)
	}
	literalValue, err := normalizeOracleValue(25, []byte("<NULL>"), oraclePostgresText)
	if err != nil {
		t.Fatal(err)
	}
	if nullValue == literalValue {
		t.Fatal("SQL NULL must differ from the literal text <NULL>")
	}

	postgresBytes, err := normalizeOracleValue(17, []byte(`\x5c783431`), oraclePostgresText)
	if err != nil {
		t.Fatal(err)
	}
	targetBytes, err := normalizeOracleValue(17, []byte(`\x41`), oracleTargetValue)
	if err != nil {
		t.Fatal(err)
	}
	if postgresBytes != targetBytes {
		t.Fatalf("bytea normalization differs: Postgres=%s target=%s", postgresBytes, targetBytes)
	}

	largeA, err := normalizeOracleValue(3802, []byte(`{"n":9007199254740992}`), oraclePostgresText)
	if err != nil {
		t.Fatal(err)
	}
	largeB, err := normalizeOracleValue(3802, []byte(`{"n":9007199254740993}`), oraclePostgresText)
	if err != nil {
		t.Fatal(err)
	}
	if largeA == largeB {
		t.Fatal("distinct large JSON integers must not normalize equally")
	}
}

func TestCDCOracleRejectsDuplicateKeys(t *testing.T) {
	columns := []oracleColumn{{Name: "tenant_id", OID: 23}, {Name: "id", OID: 23}}
	values := []oracleValue{{Canonical: "1"}, {Canonical: "2"}}
	rows := make(oracleRows)
	if err := addOracleRow(rows, columns, []int{0, 1}, values, "target"); err != nil {
		t.Fatal(err)
	}
	if err := addOracleRow(rows, columns, []int{0, 1}, values, "target"); err == nil {
		t.Fatal("expected duplicate primary-key error")
	}
}

func TestCDCDataIntegrityOracle(t *testing.T) {
	skipIfNotAvailable(t)
	binary := buildCDCOracleBinary(t)
	for _, target := range []cdcOracleTarget{cdcOracleIceberg, cdcOracleDuckLake} {
		t.Run(string(target), func(t *testing.T) {
			runCDCOracleScenario(t, binary, target)
		})
	}
}

func runCDCOracleScenario(t *testing.T, binary string, target cdcOracleTarget) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	source, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect source Postgres: %v", err)
	}
	defer source.Close(context.Background())

	suffix := fmt.Sprintf("%s_%d", target, time.Now().UnixNano())
	slot := "cdc_oracle_" + suffix
	prefix := "cdc-oracle/" + suffix
	statePath := filepath.Join(t.TempDir(), "state.db")
	catalogPath := filepath.Join(t.TempDir(), "ducklake.sqlite")
	dataPath := fmt.Sprintf("s3://%s/%s/data/", s3Bucket, prefix)
	tables := []oracleTableRef{
		{Schema: "public", Table: "cdc_oracle_groups", KeyColumns: []string{"id"}},
		{Schema: "public", Table: "cdc_oracle_values", KeyColumns: []string{"tenant_id", "id"}},
	}

	cleanupCDCOracle(t, source, slot)
	createCDCOracleSchema(t, source)
	t.Cleanup(func() {
		cleanupConn, connectErr := pgx.Connect(context.Background(), pgConnStr())
		if connectErr != nil {
			t.Logf("CDC oracle cleanup connection: %v", connectErr)
			return
		}
		defer cleanupConn.Close(context.Background())
		cleanupCDCOracle(t, cleanupConn, slot)
	})

	args := cdcOracleSyncArgs(target, slot, prefix, statePath, catalogPath, dataPath)
	process := startCDCSyncProcess(t, binary, args)
	t.Cleanup(process.kill)
	waitForCDCSlot(t, ctx, source, slot, process)

	initialLSN := applyCDCOracleInitialData(t, source)
	waitForCDCFlush(t, ctx, source, slot, initialLSN, process)
	targetLSNs := assertCDCOracleCheckpoint(t, ctx, target, prefix, catalogPath, dataPath, source, tables, initialLSN, nil, "initial insert and nulls")

	mutationLSN := applyCDCOracleMutations(t, source)
	waitForCDCFlush(t, ctx, source, slot, mutationLSN, process)
	targetLSNs = assertCDCOracleCheckpoint(t, ctx, target, prefix, catalogPath, dataPath, source, tables, mutationLSN, targetLSNs, "mixed mutations")

	// Restart with the same state and target. Subsequent comparison validates
	// resume and post-restart CDC. Forced pre-ack replay remains covered by the
	// dedicated failpoint integration tests.
	process.stop(t)
	process = startCDCSyncProcess(t, binary, args)
	t.Cleanup(process.kill)
	waitForCDCSlot(t, ctx, source, slot, process)

	restartLSN := applyCDCOraclePostRestart(t, source)
	waitForCDCFlush(t, ctx, source, slot, restartLSN, process)
	assertCDCOracleCheckpoint(t, ctx, target, prefix, catalogPath, dataPath, source, tables, restartLSN, targetLSNs, "restart and resume")
	process.stop(t)
}

func cdcOracleSyncArgs(target cdcOracleTarget, slot, prefix, statePath, catalogPath, dataPath string) []string {
	args := []string{
		"sync",
		"--source-url=" + pgConnStr(),
		"--s3-bucket=" + s3Bucket,
		"--s3-prefix=" + prefix,
		"--s3-endpoint=" + minioEndpoint,
		"--s3-region=" + s3Region,
		"--target-format=" + string(target),
		"--slot-name=" + slot,
		"--state-path=" + statePath,
		"--flush-rows=1000",
		"--flush-interval=250ms",
		"--include-tables=public.cdc_oracle_groups,public.cdc_oracle_values",
		"--log-level=INFO",
	}
	if target == cdcOracleDuckLake {
		args = append(args, "--ducklake-catalog="+catalogPath, "--ducklake-data-path="+dataPath)
	}
	return args
}

func buildCDCOracleBinary(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve CDC oracle source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	binary := filepath.Join(t.TempDir(), "streambed")
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/streambed")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build Streambed: %v\n%s", err, output)
	}
	return binary
}

func cleanupCDCOracle(t *testing.T, conn *pgx.Conn, slot string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if slot != "" {
		_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{slot}.Sanitize())
		deadline := time.Now().Add(3 * time.Second)
		for {
			var active bool
			err := conn.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&active)
			if err != nil {
				break // no slot (or connection is already unavailable)
			}
			if !active {
				_, _ = conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot)
				break
			}
			if time.Now().After(deadline) {
				t.Logf("CDC oracle cleanup: replication slot %s remained active", slot)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS cdc_oracle_values")
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS cdc_oracle_groups")
}

func createCDCOracleSchema(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE cdc_oracle_groups (
			id INTEGER PRIMARY KEY,
			label TEXT,
			revision INTEGER NOT NULL
		)`,
		`CREATE TABLE cdc_oracle_values (
			tenant_id INTEGER NOT NULL,
			id INTEGER NOT NULL,
			group_id INTEGER,
			name TEXT,
			active BOOLEAN,
			amount NUMERIC(18,4),
			payload JSONB,
			created_at TIMESTAMP,
			observed_at TIMESTAMPTZ,
			token UUID,
			data BYTEA,
			PRIMARY KEY (tenant_id, id)
		)`,
	} {
		if _, err := conn.Exec(context.Background(), statement); err != nil {
			t.Fatalf("create CDC oracle schema: %v\n%s", err, statement)
		}
	}
}

func applyCDCOracleInitialData(t *testing.T, conn *pgx.Conn) pglogrepl.LSN {
	t.Helper()
	return applyCDCOracleTransaction(t, conn, []string{
		`INSERT INTO cdc_oracle_groups VALUES (1, 'alpha', 1), (2, 'beta', 1), (3, '<NULL>', 1)`,
		`INSERT INTO cdc_oracle_values VALUES
			(1, 1, 1, 'first', true, 12.5000, '{"a":1,"tags":["x","y"]}',
			 TIMESTAMP '2024-01-02 03:04:05.123456', TIMESTAMPTZ '2024-01-02 03:04:05.123456+00',
			 '550e8400-e29b-41d4-a716-446655440000', decode('00DEADFF', 'hex')),
			(1, 2, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2, 1, 2, 'delete-me', false, -0.0100, '{"large":9007199254740993}',
			 TIMESTAMP '1970-01-01 00:00:00', TIMESTAMPTZ '2024-01-01 23:00:00-05',
			 '00000000-0000-0000-0000-000000000000', decode('5c783431', 'hex'))`,
	})
}

func applyCDCOracleMutations(t *testing.T, conn *pgx.Conn) pglogrepl.LSN {
	t.Helper()
	return applyCDCOracleTransaction(t, conn, []string{
		`UPDATE cdc_oracle_groups SET label='alpha-updated', revision=2 WHERE id=1`,
		`UPDATE cdc_oracle_values SET tenant_id=3, id=10, name='key-moved' WHERE tenant_id=1 AND id=1`,
		`UPDATE cdc_oracle_values SET name='version-1' WHERE tenant_id=1 AND id=2`,
		`UPDATE cdc_oracle_values SET name='version-2', amount=2.0000 WHERE tenant_id=1 AND id=2`,
		`UPDATE cdc_oracle_values SET name='version-3', amount=3.0000 WHERE tenant_id=1 AND id=2`,
		`DELETE FROM cdc_oracle_values WHERE tenant_id=2 AND id=1`,
		`INSERT INTO cdc_oracle_values (tenant_id,id,group_id,name,active,amount,payload)
		 VALUES (2,1,2,'reborn',true,99.9900,'{"state":"new"}')`,
		`INSERT INTO cdc_oracle_groups VALUES (4, 'multi-table', 1)`,
		`INSERT INTO cdc_oracle_values (tenant_id,id,group_id,name,active,amount)
		 VALUES (4,1,4,'multi-table',true,4.0000)`,
	})
}

func applyCDCOraclePostRestart(t *testing.T, conn *pgx.Conn) pglogrepl.LSN {
	t.Helper()
	return applyCDCOracleTransaction(t, conn, []string{
		`UPDATE cdc_oracle_groups SET label='after-restart', revision=2 WHERE id=4`,
		`UPDATE cdc_oracle_values SET amount=44.4400, payload='{"after":"restart"}' WHERE tenant_id=4 AND id=1`,
		`DELETE FROM cdc_oracle_values WHERE tenant_id=1 AND id=2`,
		`INSERT INTO cdc_oracle_values (tenant_id,id,group_id,name,active,amount)
		 VALUES (3,1,3,'post-restart',false,0.0000)`,
	})
}

func applyCDCOracleTransaction(t *testing.T, conn *pgx.Conn, statements []string) pglogrepl.LSN {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin CDC oracle transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("apply CDC oracle mutation: %v\n%s", err, statement)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit CDC oracle transaction: %v", err)
	}
	var raw string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&raw); err != nil {
		t.Fatalf("read CDC oracle commit LSN: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		t.Fatalf("parse CDC oracle commit LSN %q: %v", raw, err)
	}
	return lsn
}

func waitForCDCSlot(t *testing.T, ctx context.Context, source *pgx.Conn, slot string, process *cdcSyncProcess) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("Streambed exited before replication setup: %v\n%s", process.waitErr, process.output.String())
		default:
		}
		var ready bool
		err := source.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)
			AND EXISTS (SELECT 1 FROM pg_publication WHERE pubname=$1)`, slot).Scan(&ready)
		if err == nil && ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for replication setup: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("replication slot/publication %s not ready\n%s", slot, process.output.String())
}

func waitForCDCFlush(t *testing.T, ctx context.Context, source *pgx.Conn, slot string, want pglogrepl.LSN, process *cdcSyncProcess) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("Streambed exited before durable LSN %s: %v\n%s", want, process.waitErr, process.output.String())
		default:
		}
		var reached bool
		err := source.QueryRow(ctx, `SELECT confirmed_flush_lsn::text, confirmed_flush_lsn >= $2::pg_lsn
			FROM pg_replication_slots WHERE slot_name=$1`, slot, want.String()).Scan(&last, &reached)
		if err == nil && reached {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for durable LSN %s: %v", want, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("Streambed did not acknowledge durable LSN %s within 30s (last=%q)\n%s", want, last, process.output.String())
}

func assertCDCOracleCheckpoint(t *testing.T, ctx context.Context, target cdcOracleTarget, prefix, catalogPath, dataPath string, source *pgx.Conn, tables []oracleTableRef, sourceLSN pglogrepl.LSN, previous map[string]pglogrepl.LSN, checkpoint string) map[string]pglogrepl.LSN {
	t.Helper()
	reader := openCDCTargetReader(t, target, prefix, catalogPath, dataPath)
	defer reader.Close()
	current := make(map[string]pglogrepl.LSN, len(tables))
	for _, table := range tables {
		key := table.Schema + "." + table.Table
		targetLSN, err := reader.FlushLSN(ctx, table)
		if err != nil {
			t.Fatalf("checkpoint %q: %v", checkpoint, err)
		}
		if targetLSN == 0 || targetLSN > sourceLSN {
			t.Fatalf("checkpoint %q: invalid %s flush LSN for %s: target=%s source barrier=%s", checkpoint, target, key, targetLSN, sourceLSN)
		}
		if prior := previous[key]; prior != 0 && targetLSN <= prior {
			t.Fatalf("checkpoint %q: %s flush LSN did not advance for %s: previous=%s current=%s", checkpoint, target, key, prior, targetLSN)
		}
		current[key] = targetLSN

		want := readPostgresOracleRows(t, ctx, source, table)
		got, err := reader.ReadRows(ctx, table, want.Columns)
		if err != nil {
			t.Fatalf("checkpoint %q: read %s target %s: %v", checkpoint, target, key, err)
		}
		assertOracleRowSetsMatch(t, checkpoint, target, table, want.Rows, got)
	}
	return current
}

func openCDCTargetReader(t *testing.T, target cdcOracleTarget, prefix, catalogPath, dataPath string) cdcTargetReader {
	t.Helper()
	switch target {
	case cdcOracleIceberg:
		db := newTestDuckDB(t)
		s3Client, err := storage.NewS3Client(context.Background(), s3Bucket, s3Region, minioEndpoint)
		if err != nil {
			t.Fatalf("create Iceberg oracle S3 client: %v", err)
		}
		catalog := iceberg.NewCatalog(s3Client, s3Bucket, prefix)
		return &directTargetReader{
			name: cdcOracleIceberg,
			db:   db,
			tableExpr: func(table oracleTableRef) string {
				return fmt.Sprintf("iceberg_scan('s3://%s/%s/%s/%s', allow_moved_paths=true)", s3Bucket, prefix, table.Schema, table.Table)
			},
			readLSN: func(ctx context.Context, table oracleTableRef) (string, bool, error) {
				return catalog.GetSnapshotFlushLSN(ctx, table.Schema, table.Table)
			},
		}
	case cdcOracleDuckLake:
		db, err := ducklake.Open(context.Background(), ducklake.Config{
			CatalogPath: catalogPath,
			DataPath:    dataPath,
			S3Endpoint:  minioEndpoint,
			S3Region:    s3Region,
			ReadOnly:    true,
		})
		if err != nil {
			t.Fatalf("open DuckLake oracle reader: %v", err)
		}
		return &directTargetReader{
			name: cdcOracleDuckLake,
			db:   db,
			tableExpr: func(table oracleTableRef) string {
				return fmt.Sprintf(`"streambed".%s.%s`, quoteOracleIdent(table.Schema), quoteOracleIdent(table.Table))
			},
			readLSN: func(ctx context.Context, table oracleTableRef) (string, bool, error) {
				return ducklake.GetTableFlushLSN(ctx, db, "streambed", table.Schema, table.Table)
			},
		}
	default:
		t.Fatalf("unknown CDC oracle target %q", target)
		return nil
	}
}

func readPostgresOracleRows(t *testing.T, ctx context.Context, conn *pgx.Conn, table oracleTableRef) oracleRowSet {
	t.Helper()
	query := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s",
		quoteOracleIdent(table.Schema), quoteOracleIdent(table.Table), quoteOracleColumnList(table.KeyColumns))
	rows, err := conn.Query(ctx, query, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatalf("read source oracle table %s.%s: %v", table.Schema, table.Table, err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	columns := make([]oracleColumn, len(fields))
	for i, field := range fields {
		columns[i] = oracleColumn{Name: field.Name, OID: field.DataTypeOID}
	}
	keyIndexes, err := oracleKeyIndexes(columns, table.KeyColumns)
	if err != nil {
		t.Fatal(err)
	}
	result := oracleRowSet{Columns: columns, Rows: make(oracleRows)}
	for rows.Next() {
		raw := rows.RawValues()
		values := make([]oracleValue, len(raw))
		for i := range raw {
			values[i], err = normalizeOracleValue(columns[i].OID, raw[i], oraclePostgresText)
			if err != nil {
				t.Fatalf("normalize source %s.%s column %s: %v", table.Schema, table.Table, columns[i].Name, err)
			}
		}
		if err := addOracleRow(result.Rows, columns, keyIndexes, values, "Postgres"); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate source oracle rows: %v", err)
	}
	return result
}

func queryDirectTargetRows(ctx context.Context, db *sql.DB, target cdcOracleTarget, tableExpr string, keyColumns []string, columns []oracleColumn) (oracleRows, error) {
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s", tableExpr, quoteOracleColumnList(keyColumns))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(names) != len(columns) {
		return nil, fmt.Errorf("column count mismatch: Postgres=%d %s=%d (%v)", len(columns), target, len(names), names)
	}
	for i := range names {
		if names[i] != columns[i].Name {
			return nil, fmt.Errorf("column %d name mismatch: Postgres=%q %s=%q", i, columns[i].Name, target, names[i])
		}
	}
	keyIndexes, err := oracleKeyIndexes(columns, keyColumns)
	if err != nil {
		return nil, err
	}
	result := make(oracleRows)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		normalized := make([]oracleValue, len(columns))
		for i := range values {
			normalized[i], err = normalizeOracleValue(columns[i].OID, values[i], oracleTargetValue)
			if err != nil {
				return nil, fmt.Errorf("normalize %s column %s: %w", target, columns[i].Name, err)
			}
		}
		if err := addOracleRow(result, columns, keyIndexes, normalized, string(target)); err != nil {
			return nil, err
		}
	}
	return result, rows.Err()
}

func oracleKeyIndexes(columns []oracleColumn, keys []string) ([]int, error) {
	indexes := make([]int, len(keys))
	for i, key := range keys {
		indexes[i] = -1
		for j, column := range columns {
			if column.Name == key {
				indexes[i] = j
				break
			}
		}
		if indexes[i] < 0 {
			return nil, fmt.Errorf("oracle key column %q not found in %v", key, columns)
		}
	}
	return indexes, nil
}

func addOracleRow(rows oracleRows, columns []oracleColumn, keyIndexes []int, values []oracleValue, source string) error {
	keyParts := make([]string, len(keyIndexes))
	for i, index := range keyIndexes {
		if values[index].Null {
			keyParts[i] = "N"
		} else {
			keyParts[i] = fmt.Sprintf("V%d:%s", len(values[index].Canonical), values[index].Canonical)
		}
	}
	key := strings.Join(keyParts, "|")
	row := make(map[string]oracleValue, len(columns))
	for i, column := range columns {
		row[column.Name] = values[i]
	}
	if previous, exists := rows[key]; exists {
		return fmt.Errorf("duplicate primary key %q in %s: first=%v duplicate=%v", key, source, previous, row)
	}
	rows[key] = row
	return nil
}

func assertOracleRowSetsMatch(t *testing.T, checkpoint string, target cdcOracleTarget, table oracleTableRef, source, destination oracleRows) {
	t.Helper()
	type difference struct{ kind, key, detail string }
	var differences []difference
	for key, expected := range source {
		actual, ok := destination[key]
		if !ok {
			differences = append(differences, difference{"missing", key, fmt.Sprintf("Postgres=%v", expected)})
			continue
		}
		columns := make([]string, 0, len(expected))
		for column := range expected {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			actualValue, exists := actual[column]
			if !exists {
				differences = append(differences, difference{"missing_column", key, column})
			} else if expected[column] != actualValue {
				differences = append(differences, difference{"value_mismatch", key,
					fmt.Sprintf("column=%s Postgres=%s %s=%s", column, expected[column], target, actualValue)})
			}
		}
	}
	for key, actual := range destination {
		if _, exists := source[key]; !exists {
			differences = append(differences, difference{"extra", key, fmt.Sprintf("%s=%v", target, actual)})
		}
	}
	if len(differences) == 0 {
		t.Logf("CDC oracle checkpoint %q: %s %s.%s matches Postgres (%d rows)", checkpoint, target, table.Schema, table.Table, len(source))
		return
	}
	sort.Slice(differences, func(i, j int) bool {
		if differences[i].kind == differences[j].kind {
			return differences[i].key < differences[j].key
		}
		return differences[i].kind < differences[j].kind
	})
	limit := len(differences)
	if limit > 10 {
		limit = 10
	}
	for _, difference := range differences[:limit] {
		t.Logf("[%s] key=%s %s", difference.kind, difference.key, difference.detail)
	}
	t.Fatalf("CDC oracle checkpoint %q failed for %s %s.%s: Postgres=%d rows target=%d rows differences=%d",
		checkpoint, target, table.Schema, table.Table, len(source), len(destination), len(differences))
}

func normalizeOracleValue(oid uint32, value any, source oracleValueSource) (oracleValue, error) {
	if value == nil {
		return oracleValue{Null: true}, nil
	}
	if raw, ok := value.([]byte); ok && raw == nil {
		return oracleValue{Null: true}, nil
	}
	result := func(value string) (oracleValue, error) {
		return oracleValue{Canonical: value}, nil
	}
	text := oracleValueText(value)
	switch oid {
	case 16:
		switch strings.ToLower(text) {
		case "t", "true":
			return result("true")
		case "f", "false":
			return result("false")
		default:
			return oracleValue{}, fmt.Errorf("invalid boolean %q", text)
		}
	case 20, 21, 23:
		n := new(big.Int)
		if _, ok := n.SetString(text, 10); !ok {
			return oracleValue{}, fmt.Errorf("invalid integer %q", text)
		}
		return result(n.String())
	case 700, 701:
		bits := 64
		if oid == 700 {
			bits = 32
		}
		f, err := strconv.ParseFloat(text, bits)
		if err != nil {
			return oracleValue{}, err
		}
		if math.IsNaN(f) {
			return result("NaN")
		}
		return result(strconv.FormatFloat(f, 'g', -1, bits))
	case 1700:
		if strings.EqualFold(text, "nan") || strings.EqualFold(text, "infinity") || strings.EqualFold(text, "-infinity") {
			return result(strings.ToUpper(text))
		}
		n := new(big.Rat)
		if _, ok := n.SetString(text); !ok {
			return oracleValue{}, fmt.Errorf("invalid numeric %q", text)
		}
		return result(n.RatString())
	case 1082:
		if timestamp, ok := value.(time.Time); ok {
			return result(timestamp.Format("2006-01-02"))
		}
		parsed, err := time.Parse("2006-01-02", text)
		if err != nil {
			return oracleValue{}, err
		}
		return result(parsed.Format("2006-01-02"))
	case 1114:
		parsed, err := oracleTimestamp(value, false)
		if err != nil {
			return oracleValue{}, err
		}
		return result(parsed.Format("2006-01-02T15:04:05.999999999"))
	case 1184:
		parsed, err := oracleTimestamp(value, true)
		if err != nil {
			return oracleValue{}, err
		}
		return result(parsed.UTC().Format(time.RFC3339Nano))
	case 2950:
		if raw, ok := value.([]byte); ok && source == oracleTargetValue && len(raw) == 16 {
			id, err := uuid.FromBytes(raw)
			if err != nil {
				return oracleValue{}, err
			}
			return result(id.String())
		}
		id, err := uuid.Parse(text)
		if err != nil {
			return oracleValue{}, err
		}
		return result(id.String())
	case 17:
		raw, ok := value.([]byte)
		if !ok {
			return oracleValue{}, fmt.Errorf("bytea value has type %T", value)
		}
		if source == oraclePostgresText {
			if !strings.HasPrefix(string(raw), `\x`) {
				return oracleValue{}, fmt.Errorf("unexpected Postgres bytea text %q", raw)
			}
			decoded, err := hex.DecodeString(string(raw[2:]))
			if err != nil {
				return oracleValue{}, err
			}
			raw = decoded
		}
		return result(hex.EncodeToString(raw))
	case 114, 3802:
		canonical, err := canonicalOracleJSON(text)
		if err != nil {
			return oracleValue{}, err
		}
		return result(canonical)
	default:
		return result(text)
	}
}

func canonicalOracleJSON(text string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	return appendCanonicalOracleJSON(nil, value)
}

func appendCanonicalOracleJSON(dst []byte, value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		dst = append(dst, 'n')
	case bool:
		if typed {
			dst = append(dst, "b1"...)
		} else {
			dst = append(dst, "b0"...)
		}
	case string:
		dst = append(dst, fmt.Sprintf("s%d:", len(typed))...)
		dst = append(dst, typed...)
	case json.Number:
		n := new(big.Rat)
		if _, ok := n.SetString(typed.String()); !ok {
			return "", fmt.Errorf("invalid JSON number %q", typed)
		}
		canonical := n.RatString()
		dst = append(dst, fmt.Sprintf("d%d:", len(canonical))...)
		dst = append(dst, canonical...)
	case []any:
		dst = append(dst, fmt.Sprintf("a%d:", len(typed))...)
		for _, item := range typed {
			canonical, err := appendCanonicalOracleJSON(nil, item)
			if err != nil {
				return "", err
			}
			dst = append(dst, fmt.Sprintf("%d:", len(canonical))...)
			dst = append(dst, canonical...)
		}
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dst = append(dst, fmt.Sprintf("o%d:", len(keys))...)
		for _, key := range keys {
			canonical, err := appendCanonicalOracleJSON(nil, typed[key])
			if err != nil {
				return "", err
			}
			dst = append(dst, fmt.Sprintf("%d:%s%d:", len(key), key, len(canonical))...)
			dst = append(dst, canonical...)
		}
	default:
		return "", fmt.Errorf("unsupported decoded JSON value %T", value)
	}
	return string(dst), nil
}

func oracleValueText(value any) string {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(value)
	}
}

func oracleTimestamp(value any, withTimeZone bool) (time.Time, error) {
	if timestamp, ok := value.(time.Time); ok {
		return timestamp, nil
	}
	text := oracleValueText(value)
	formats := []string{
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05-07",
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, format := range formats {
		if parsed, err := time.Parse(format, text); err == nil {
			if withTimeZone && parsed.Location() == time.UTC && !strings.ContainsAny(text, "+-Z") {
				return time.Time{}, fmt.Errorf("timestamptz %q has no timezone", text)
			}
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", text)
}

func quoteOracleIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteOracleColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteOracleIdent(column)
	}
	return strings.Join(quoted, ", ")
}
