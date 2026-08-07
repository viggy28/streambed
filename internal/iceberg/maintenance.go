package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	ice "github.com/apache/iceberg-go"
)

type MaintenancePlan struct {
	Table            string
	ExpiredSnapshots int
	DeleteObjects    []string
}

// ExpireSnapshots keeps the current snapshot plus retainLast most recent older
// snapshots and deletes objects that are reachable only from expired snapshots.
// It is intentionally conservative: it never deletes objects reachable from any
// retained snapshot and does not attempt arbitrary orphan deletion.
func (c *Catalog) ExpireSnapshots(ctx context.Context, schema, table string, retainLast int, dryRun bool) (MaintenancePlan, error) {
	if retainLast < 0 {
		retainLast = 0
	}
	meta, version, metaKey, err := c.readCurrentMetadata(ctx, schema, table)
	if err != nil {
		return MaintenancePlan{}, err
	}
	plan := MaintenancePlan{Table: schema + "." + table}
	if len(meta.Snapshots) == 0 {
		return plan, nil
	}

	retainedIDs := map[int64]bool{}
	if meta.CurrentSnapshotID > 0 {
		retainedIDs[meta.CurrentSnapshotID] = true
	}
	ordered := append([]snapshot(nil), meta.Snapshots...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].TimestampMS > ordered[j].TimestampMS })
	for _, s := range ordered {
		if len(retainedIDs) >= retainLast+1 {
			break
		}
		retainedIDs[s.SnapshotID] = true
	}

	retainedReachable := map[string]bool{}
	expiredReachable := map[string]bool{}
	for _, s := range meta.Snapshots {
		target := expiredReachable
		if retainedIDs[s.SnapshotID] {
			target = retainedReachable
		}
		if err := c.collectSnapshotObjects(ctx, s, target); err != nil {
			return plan, err
		}
	}
	basePath := c.tablePath(schema, table)
	retainedReachable[path.Join(basePath, "metadata", "version-hint.text")] = true
	retainedReachable[metaKey] = true
	for _, ml := range meta.MetadataLog {
		retainedReachable[s3KeyFromURI(ml.MetadataFile)] = true
	}

	for key := range expiredReachable {
		if !retainedReachable[key] {
			plan.DeleteObjects = append(plan.DeleteObjects, key)
		}
	}
	sort.Strings(plan.DeleteObjects)

	var kept []snapshot
	for _, s := range meta.Snapshots {
		if retainedIDs[s.SnapshotID] {
			kept = append(kept, s)
		} else {
			plan.ExpiredSnapshots++
		}
	}
	if dryRun || plan.ExpiredSnapshots == 0 {
		return plan, nil
	}
	meta.Snapshots = kept
	var keptLog []snapshotLogEntry
	for _, e := range meta.SnapshotLog {
		if retainedIDs[e.SnapshotID] {
			keptLog = append(keptLog, e)
		}
	}
	meta.SnapshotLog = keptLog
	meta.LastUpdatedMS = time.Now().UnixMilli()
	newVersion := version + 1
	newMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", newVersion))
	meta.MetadataLog = append(meta.MetadataLog, metadataLogEntry{TimestampMS: meta.LastUpdatedMS, MetadataFile: fmt.Sprintf("s3://%s/%s", c.bucket, metaKey)})
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return plan, err
	}
	if err := c.storage.PutObject(ctx, newMetaKey, data, "application/json"); err != nil {
		return plan, err
	}
	if err := c.storage.PutObject(ctx, path.Join(basePath, "metadata", "version-hint.text"), []byte(strconv.Itoa(newVersion)), "text/plain"); err != nil {
		return plan, err
	}
	if len(plan.DeleteObjects) > 0 {
		if err := c.storage.DeleteObjects(ctx, plan.DeleteObjects); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

// DeleteOrphanFiles deletes objects under a table prefix that are not reachable
// from the current metadata file and current snapshot. It defaults to dry-run at
// the CLI layer; callers should avoid running it concurrently with sync.
func (c *Catalog) DeleteOrphanFiles(ctx context.Context, schema, table string, dryRun bool) (MaintenancePlan, error) {
	if !dryRun {
		return MaintenancePlan{}, fmt.Errorf("orphan deletion requires object age/locking support; run dry-run only")
	}
	meta, _, metaKey, err := c.readCurrentMetadata(ctx, schema, table)
	if err != nil {
		return MaintenancePlan{}, err
	}
	basePath := c.tablePath(schema, table)
	plan := MaintenancePlan{Table: schema + "." + table}
	reachable := map[string]bool{
		path.Join(basePath, "metadata", "version-hint.text"): true,
		metaKey: true,
	}
	for _, ml := range meta.MetadataLog {
		reachable[s3KeyFromURI(ml.MetadataFile)] = true
	}
	for _, s := range meta.Snapshots {
		if err := c.collectSnapshotObjects(ctx, s, reachable); err != nil {
			return plan, err
		}
	}
	keys, err := c.storage.ListPrefix(ctx, basePath)
	if err != nil {
		return plan, err
	}
	for _, key := range keys {
		clean := strings.TrimPrefix(path.Clean(key), "/")
		if !reachable[clean] {
			plan.DeleteObjects = append(plan.DeleteObjects, clean)
		}
	}
	sort.Strings(plan.DeleteObjects)
	return plan, nil
}

func (c *Catalog) readCurrentMetadata(ctx context.Context, schema, table string) (tableMetadata, int, string, error) {
	basePath := c.tablePath(schema, table)
	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return tableMetadata{}, 0, "", fmt.Errorf("read version-hint: %w", err)
	}
	version, err := strconv.Atoi(string(hintData))
	if err != nil {
		return tableMetadata{}, 0, "", fmt.Errorf("parse version hint: %w", err)
	}
	metaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", version))
	metaData, err := c.storage.GetObject(ctx, metaKey)
	if err != nil {
		return tableMetadata{}, 0, "", fmt.Errorf("read metadata: %w", err)
	}
	var meta tableMetadata
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return tableMetadata{}, 0, "", fmt.Errorf("parse metadata: %w", err)
	}
	return meta, version, metaKey, nil
}

func (c *Catalog) collectSnapshotObjects(ctx context.Context, s snapshot, out map[string]bool) error {
	if s.ManifestList == "" {
		return nil
	}
	manifestListKey := s3KeyFromURI(s.ManifestList)
	out[manifestListKey] = true
	manifestListData, err := c.storage.GetObject(ctx, manifestListKey)
	if err != nil {
		return err
	}
	manifests, err := readManifestListAvro(manifestListData)
	if err != nil {
		return err
	}
	for _, mf := range manifests {
		manifestKey := s3KeyFromURI(mf.FilePath())
		out[manifestKey] = true
		mfData, err := c.storage.GetObject(ctx, manifestKey)
		if err != nil {
			return err
		}
		entries, err := ice.ReadManifest(mf, bytes.NewReader(mfData), true)
		if err != nil {
			return err
		}
		for _, e := range entries {
			out[s3KeyFromURI(e.DataFile().FilePath())] = true
		}
	}
	return nil
}
