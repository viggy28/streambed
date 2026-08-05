//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/viggy28/streambed/internal/iceberg"
)

// TestMutationModesDuckDBCorrectness verifies both mutation modes through
// DuckDB's exact iceberg_scan reader. The MOR case deliberately combines
// replacement data and equality deletes in the same snapshot and exercises
// repeated updates, a key change, a delete, resurrection, and composite keys.
func TestMutationModesDuckDBCorrectness(t *testing.T) {
	skipIfNotAvailable(t)

	for _, mode := range []iceberg.MutationMode{iceberg.MutationModeCOW, iceberg.MutationModeMOR} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cleanup(t)
			clearS3Prefix(t)
			execSQL(t, "DROP TABLE IF EXISTS mutation_mode_test")
			t.Cleanup(func() {
				execSQL(t, "DROP TABLE IF EXISTS mutation_mode_test")
				cleanup(t)
			})

			execSQL(t, `CREATE TABLE mutation_mode_test (
				tenant_id INT NOT NULL,
				item_id INT NOT NULL,
				name TEXT,
				PRIMARY KEY (tenant_id, item_id)
			)`)
			createSlotAndPublication(t)
			statePath := t.TempDir() + "/state.db"

			execSQL(t, `INSERT INTO mutation_mode_test VALUES
				(1, 1, 'one'), (1, 2, 'two'), (2, 1, 'delete-me'), (2, 2, 'old')`)
			runSyncWithMutationMode(t, ctx, 12*time.Second, mode, statePath)

			duckDB := newTestDuckDB(t)
			assertMatch := func() {
				assertPgIcebergMatch(t, duckDB, "public", "mutation_mode_test",
					[]string{"tenant_id", "item_id"}, []string{"name"})
			}

			// MOR is intentionally committed across separate pipeline runs. This
			// verifies accumulated delete manifests, startup reconciliation, exact
			// values, and uniqueness after every snapshot.
			execSQL(t, "UPDATE mutation_mode_test SET name = 'one-v2' WHERE tenant_id = 1 AND item_id = 1")
			execSQL(t, "UPDATE mutation_mode_test SET name = 'one-v3' WHERE tenant_id = 1 AND item_id = 1")
			runSyncWithMutationMode(t, ctx, 8*time.Second, mode, statePath)
			assertMatch()

			execSQL(t, "UPDATE mutation_mode_test SET item_id = 20, name = 'two-moved' WHERE tenant_id = 1 AND item_id = 2")
			runSyncWithMutationMode(t, ctx, 8*time.Second, mode, statePath)
			assertMatch()

			execSQL(t, "UPDATE mutation_mode_test SET item_id = 21, name = 'two-moved-again' WHERE tenant_id = 1 AND item_id = 20")
			runSyncWithMutationMode(t, ctx, 8*time.Second, mode, statePath)
			assertMatch()

			execSQL(t, "DELETE FROM mutation_mode_test WHERE tenant_id = 2 AND item_id = 1")
			execSQL(t, "DELETE FROM mutation_mode_test WHERE tenant_id = 2 AND item_id = 2")
			runSyncWithMutationMode(t, ctx, 8*time.Second, mode, statePath)
			assertMatch()

			execSQL(t, "INSERT INTO mutation_mode_test VALUES (2, 2, 'resurrected')")
			runSyncWithMutationMode(t, ctx, 8*time.Second, mode, statePath)
			assertMatch()

			if mode == iceberg.MutationModeMOR {
				keys := listS3Keys(t, s3Prefix+"/public/mutation_mode_test/data/")
				var deletes int
				for _, key := range keys {
					if strings.HasSuffix(key, "-delete.parquet") {
						deletes++
					}
				}
				if deletes == 0 {
					t.Fatal("MOR produced no equality-delete file")
				}
			}
		})
	}
}

func listS3Keys(t *testing.T, prefix string) []string {
	t.Helper()
	client := newTestS3Client(t)
	ctx := context.Background()
	var keys []string
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, s3ListInput(prefix, token))
		if err != nil {
			t.Fatalf("list S3 prefix %q: %v", prefix, err)
		}
		for _, obj := range out.Contents {
			keys = append(keys, objectKey(obj))
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return keys
}

// These small wrappers keep the test helper independent of AWS pointer boilerplate.
func s3ListInput(prefix string, token *string) *s3.ListObjectsV2Input {
	return &s3.ListObjectsV2Input{Bucket: aws.String(s3Bucket), Prefix: aws.String(prefix), ContinuationToken: token}
}

func objectKey(obj types.Object) string { return aws.ToString(obj.Key) }
