package wal

import "testing"

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"my_table", `"my_table"`},
		{"public", `"public"`},
		// embedded double quotes must be doubled
		{`my"table`, `"my""table"`},
		{`a""b`, `"a""""b"`},
		// empty string
		{"", `""`},
		// SQL keywords
		{"select", `"select"`},
		// mixed case preserved
		{"MyTable", `"MyTable"`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := quoteIdent(tt.input)
			if got != tt.want {
				t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestEscapeLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		// single quotes must be doubled
		{"it's", "it''s"},
		{"O'Brien", "O''Brien"},
		{`no'quotes'here`, `no''quotes''here`},
		// no quotes
		{"simple", "simple"},
		// empty string
		{"", ""},
		// multiple adjacent quotes
		{"''", "''''"},
		// SQL injection attempt
		{"'; DROP TABLE users; --", "''; DROP TABLE users; --"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := escapeLiteral(tt.input)
			if got != tt.want {
				t.Errorf("escapeLiteral(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
