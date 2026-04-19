package oracle

import (
	"strings"
	"testing"
)

func TestDiffRowSets_Identical(t *testing.T) {
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "value": "100"},
		"2": {"id": "2", "name": "bob", "value": "200"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "value": "100"},
		"2": {"id": "2", "name": "bob", "value": "200"},
	}
	discs := DiffRowSets(pg, ice, nil)
	if len(discs) != 0 {
		t.Fatalf("expected 0 discrepancies, got %d: %+v", len(discs), discs)
	}
}

func TestDiffRowSets_MissingInIceberg(t *testing.T) {
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice"},
		"2": {"id": "2", "name": "bob"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "alice"},
	}
	discs := DiffRowSets(pg, ice, nil)
	if len(discs) != 1 {
		t.Fatalf("expected 1 discrepancy, got %d", len(discs))
	}
	if discs[0].Kind != "missing_in_iceberg" || discs[0].Key != "2" {
		t.Fatalf("unexpected discrepancy: %+v", discs[0])
	}
}

func TestDiffRowSets_ExtraInIceberg(t *testing.T) {
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "alice"},
		"2": {"id": "2", "name": "bob"},
	}
	discs := DiffRowSets(pg, ice, nil)
	if len(discs) != 1 {
		t.Fatalf("expected 1 discrepancy, got %d", len(discs))
	}
	if discs[0].Kind != "extra_in_iceberg" || discs[0].Key != "2" {
		t.Fatalf("unexpected discrepancy: %+v", discs[0])
	}
}

func TestDiffRowSets_ValueMismatch(t *testing.T) {
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "value": "100"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "value": "999"},
	}
	discs := DiffRowSets(pg, ice, nil)
	if len(discs) != 1 {
		t.Fatalf("expected 1 discrepancy, got %d: %+v", len(discs), discs)
	}
	if discs[0].Kind != "value_mismatch" || discs[0].Key != "1" {
		t.Fatalf("unexpected discrepancy: %+v", discs[0])
	}
	if !strings.Contains(discs[0].Details, "value") {
		t.Errorf("details should mention column name: %q", discs[0].Details)
	}
}

func TestDiffRowSets_CompareColumnsFilter(t *testing.T) {
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "updated_at": "t1"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "updated_at": "t2"},
	}
	// When compareColumns explicitly excludes updated_at, mismatch should not surface.
	discs := DiffRowSets(pg, ice, []string{"id", "name"})
	if len(discs) != 0 {
		t.Fatalf("expected 0 discrepancies when updated_at not compared, got %+v", discs)
	}
}

func TestDiffRowSets_ColumnMissingFromOneSide(t *testing.T) {
	// Schema evolution scenario: iceberg has a column PG doesn't yet expose
	// (or vice versa). DiffRowSets should silently skip columns missing from
	// either side rather than flagging every row.
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "alice", "new_col": "x"},
	}
	discs := DiffRowSets(pg, ice, nil)
	if len(discs) != 0 {
		t.Fatalf("expected 0 discrepancies for column on one side, got %+v", discs)
	}
}

func TestDiffRowSets_MultipleDiscrepancies(t *testing.T) {
	pg := map[string]map[string]string{
		"1": {"id": "1", "name": "alice"},
		"2": {"id": "2", "name": "bob"},
		"3": {"id": "3", "name": "carol"},
	}
	ice := map[string]map[string]string{
		"1": {"id": "1", "name": "ALICE"}, // value_mismatch
		// "2" is missing_in_iceberg
		"3": {"id": "3", "name": "carol"},
		"4": {"id": "4", "name": "dan"}, // extra_in_iceberg
	}
	discs := DiffRowSets(pg, ice, nil)
	if len(discs) != 3 {
		t.Fatalf("expected 3 discrepancies, got %d: %+v", len(discs), discs)
	}
	kinds := map[string]int{}
	for _, d := range discs {
		kinds[d.Kind]++
	}
	if kinds["missing_in_iceberg"] != 1 || kinds["extra_in_iceberg"] != 1 || kinds["value_mismatch"] != 1 {
		t.Fatalf("unexpected kind counts: %v", kinds)
	}
}

func TestDiffReport_Counts(t *testing.T) {
	r := DiffReport{
		Discrepancies: []Discrepancy{
			{Kind: "missing_in_iceberg"},
			{Kind: "missing_in_iceberg"},
			{Kind: "extra_in_iceberg"},
			{Kind: "value_mismatch"},
			{Kind: "value_mismatch"},
			{Kind: "value_mismatch"},
		},
	}
	m, e, mm := r.Counts()
	if m != 2 || e != 1 || mm != 3 {
		t.Errorf("Counts() = (%d,%d,%d), want (2,1,3)", m, e, mm)
	}
}

func TestSqlQuote(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"hello", "'hello'"},
		{"", "''"},
		{"O'Brien", "'O''Brien'"},
		{"it's 'quoted'", "'it''s ''quoted'''"},
		{"no-quotes-here", "'no-quotes-here'"},
	}
	for _, tc := range cases {
		got := sqlQuote(tc.in)
		if got != tc.want {
			t.Errorf("sqlQuote(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
