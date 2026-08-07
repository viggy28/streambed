package iceberg

import (
	"bytes"
	"encoding/binary"
	"strconv"

	pqbuilder "github.com/viggy28/streambed/internal/parquet"
)

func selectCOWFiles(files []DataFile, buf *tableBuffer, cols []pqbuilder.ColumnDef) (candidates, carried []DataFile, pruned bool) {
	if len(files) == 0 || len(buf.Deletes) == 0 || len(buf.KeyColumns) == 0 {
		return files, nil, false
	}
	fieldIDs := make([]int, len(buf.KeyColumns))
	keyOIDs := make([]uint32, len(buf.KeyColumns))
	for i, idx := range buf.KeyColumns {
		if idx < 0 || idx >= len(buf.Columns) {
			return files, nil, false
		}
		col := buf.Columns[idx]
		id := buf.fieldIDs[col.Name]
		if id <= 0 {
			return files, nil, false
		}
		fieldIDs[i] = id
		keyOIDs[i] = col.OID
	}
	encodedDeletes := make([][][]byte, 0, len(buf.Deletes))
	for _, del := range dedupValueRows(buf.Deletes) {
		if len(del) != len(fieldIDs) {
			return files, nil, false
		}
		enc := make([][]byte, len(del))
		for i, v := range del {
			b, ok := encodeBoundValue(keyOIDs[i], v)
			if !ok {
				return files, nil, false
			}
			enc[i] = b
		}
		encodedDeletes = append(encodedDeletes, enc)
	}
	for _, f := range files {
		if !hasRequiredBounds(f, fieldIDs) {
			return files, nil, false
		}
		if fileMayContainAnyKey(f, fieldIDs, keyOIDs, encodedDeletes) {
			candidates = append(candidates, f)
		} else {
			carried = append(carried, f)
		}
	}
	return candidates, carried, len(carried) > 0
}

func hasRequiredBounds(f DataFile, fieldIDs []int) bool {
	for _, id := range fieldIDs {
		if _, ok := f.LowerBounds[id]; !ok {
			return false
		}
		if _, ok := f.UpperBounds[id]; !ok {
			return false
		}
	}
	return true
}

func fileMayContainAnyKey(f DataFile, fieldIDs []int, oids []uint32, encodedKeys [][][]byte) bool {
	for _, key := range encodedKeys {
		mayContain := true
		for i, id := range fieldIDs {
			lo, hi := f.LowerBounds[id], f.UpperBounds[id]
			if compareEncodedBound(oids[i], key[i], lo) < 0 || compareEncodedBound(oids[i], key[i], hi) > 0 {
				mayContain = false
				break
			}
		}
		if mayContain {
			return true
		}
	}
	return false
}

func compareEncodedBound(oid uint32, a, b []byte) int {
	switch oid {
	case 21, 23:
		if len(a) >= 4 && len(b) >= 4 {
			av := int32(binary.LittleEndian.Uint32(a))
			bv := int32(binary.LittleEndian.Uint32(b))
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			default:
				return 0
			}
		}
	case 20:
		if len(a) >= 8 && len(b) >= 8 {
			av := int64(binary.LittleEndian.Uint64(a))
			bv := int64(binary.LittleEndian.Uint64(b))
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			default:
				return 0
			}
		}
	}
	return bytes.Compare(a, b)
}

func computeKeyBounds(buf *tableBuffer, rows [][]pqbuilder.Value) (map[int][]byte, map[int][]byte) {
	if len(buf.KeyColumns) == 0 || len(rows) == 0 {
		return nil, nil
	}
	lower := make(map[int][]byte)
	upper := make(map[int][]byte)
	for _, row := range rows {
		for _, idx := range buf.KeyColumns {
			if idx < 0 || idx >= len(buf.Columns) || idx >= len(row) {
				return nil, nil
			}
			col := buf.Columns[idx]
			id := buf.fieldIDs[col.Name]
			if id <= 0 {
				return nil, nil
			}
			b, ok := encodeBoundValue(col.OID, row[idx])
			if !ok {
				return nil, nil
			}
			if cur, ok := lower[id]; !ok || compareEncodedBound(col.OID, b, cur) < 0 {
				lower[id] = append([]byte(nil), b...)
			}
			if cur, ok := upper[id]; !ok || compareEncodedBound(col.OID, b, cur) > 0 {
				upper[id] = append([]byte(nil), b...)
			}
		}
	}
	return lower, upper
}

func encodeBoundValue(oid uint32, v pqbuilder.Value) ([]byte, bool) {
	if v.IsNull {
		return nil, false
	}
	s := string(v.Data)
	switch oid {
	case 21, 23:
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, false
		}
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(int32(n)))
		return b[:], true
	case 20:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, false
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(n))
		return b[:], true
	case 16, 17, 700, 701, 1082, 1114, 1184, 2950:
		// These Iceberg types need type-specific binary bound encodings. Until
		// those are implemented, omit bounds so COW pruning falls back safely.
		return nil, false
	default:
		// PgOIDToIcebergType maps all other OIDs to Iceberg string today.
		return []byte(s), true
	}
}
