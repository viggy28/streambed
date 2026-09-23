package server

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
)

type sqlTokenKind int

const (
	tokenWord sqlTokenKind = iota
	tokenQuotedIdent
	tokenString
	tokenSymbol
)

type sqlToken struct {
	kind        sqlTokenKind
	text        string
	startOffset int // Inclusive byte offset in the original query.
	endOffset   int // Exclusive byte offset in the original query.
}

type timeTravelRef struct {
	tableName           string
	timestamp           time.Time
	tableStartOffset    int
	timeTravelEndOffset int
	alias               string
}

type icebergTableReplacement struct {
	queryStartOffset int // Inclusive byte offset of the table reference.
	queryEndOffset   int // Exclusive byte offset after the AT clause.
	replacementSQL   string
}

// prepareTimeTravelQuery validates Streambed's V1 time-travel contract and,
// for path-based Iceberg tables, rewrites DuckLake's native table syntax to an
// iceberg_scan call. DuckLake queries are returned unchanged.
func prepareTimeTravelQuery(ctx context.Context, query, targetFormat string, catalog *TableCatalog) (string, error) {
	refs, err := findTimeTravelRefs(query)
	if err != nil {
		return "", err
	}
	if len(refs) == 0 {
		return query, nil
	}

	requestedTime := refs[0].timestamp
	for _, ref := range refs[1:] {
		if !ref.timestamp.Equal(requestedTime) {
			return "", fmt.Errorf("all historical tables in a query must use the same timestamp in V1")
		}
	}

	switch targetFormat {
	case "ducklake":
		return query, nil
	case "iceberg":
		// Iceberg does not support DuckLake's native AT syntax, so rewrite it below.
	default:
		return "", fmt.Errorf("unsupported target format %q", targetFormat)
	}
	if catalog == nil {
		return "", fmt.Errorf("iceberg table catalog is not initialized")
	}

	replacements := make([]icebergTableReplacement, 0, len(refs))
	for _, ref := range refs {
		tableName := ref.tableName
		parts := strings.Split(tableName, ".")
		if len(parts) == 3 {
			if parts[0] != "streambed" {
				return "", fmt.Errorf("Iceberg time travel does not support catalog %q", parts[0])
			}
			tableName = strings.Join(parts[1:], ".")
		}
		scan, err := catalog.ResolveAtTimestamp(ctx, tableName, ref.timestamp)
		if err != nil {
			return "", err
		}
		replacements = append(replacements, icebergTableReplacement{
			queryStartOffset: ref.tableStartOffset,
			queryEndOffset:   ref.timeTravelEndOffset,
			replacementSQL:   scan + ref.alias,
		})
	}

	// Replace right-to-left so byte offsets from the original query stay valid.
	for i := len(replacements) - 1; i >= 0; i-- {
		r := replacements[i]
		query = query[:r.queryStartOffset] + r.replacementSQL + query[r.queryEndOffset:]
	}
	return query, nil
}

func findTimeTravelRefs(query string) ([]timeTravelRef, error) {
	tokens, err := tokenizeSQL(query)
	if err != nil {
		return nil, err
	}
	var refs []timeTravelRef
	for i := 0; i < len(tokens); i++ {
		if !tokenIs(tokens[i], "AT") || i+1 >= len(tokens) || tokens[i+1].text != "(" {
			continue
		}
		// V1 intentionally accepts only DuckLake's timestamp form. Snapshot IDs
		// and arbitrary timestamp expressions remain out of scope.
		if i+6 >= len(tokens) ||
			!tokenIs(tokens[i+2], "TIMESTAMP") || tokens[i+3].text != "=>" ||
			!tokenIs(tokens[i+4], "TIMESTAMPTZ") || tokens[i+5].kind != tokenString ||
			tokens[i+6].text != ")" {
			return nil, fmt.Errorf("unsupported time travel clause; V1 requires AT (TIMESTAMP => TIMESTAMPTZ 'YYYY-MM-DD HH:MM:SS')")
		}

		timestampText, err := unquoteSQLString(tokens[i+5].text)
		if err != nil {
			return nil, fmt.Errorf("invalid time travel timestamp: %w", err)
		}
		at, err := parseUTCTimestamp(timestampText)
		if err != nil {
			return nil, err
		}

		tableStartToken, tableName, alias, err := tableBeforeAT(query, tokens, i)
		if err != nil {
			return nil, err
		}
		refs = append(refs, timeTravelRef{
			tableName:           tableName,
			timestamp:           at,
			tableStartOffset:    tokens[tableStartToken].startOffset,
			timeTravelEndOffset: tokens[i+6].endOffset,
			alias:               alias,
		})
		i += 6
	}
	return refs, nil
}

func tableBeforeAT(query string, tokens []sqlToken, atIndex int) (int, string, string, error) {
	previous := atIndex - 1
	if previous < 0 || !isIdentifier(tokens[previous]) {
		return 0, "", "", fmt.Errorf("AT time travel clause must follow a table name or alias")
	}

	// Without an alias, AT immediately follows the table name. DuckLake puts
	// aliases before AT ("table AS t AT (...)"), so if this first attempt does
	// not reach FROM/JOIN/comma, try the two supported alias forms.
	if start, name, ok, err := qualifiedTableEndingAt(tokens, previous); err != nil {
		return 0, "", "", err
	} else if ok {
		return start, name, "", nil
	}

	tableEnd := previous - 1 // bare alias: table t AT (...)
	if previous >= 2 && tokenIs(tokens[previous-1], "AS") {
		tableEnd = previous - 2 // explicit alias: table AS t AT (...)
	}
	if tableEnd < 0 {
		return 0, "", "", fmt.Errorf("AT time travel clause must follow a table in FROM or JOIN")
	}
	start, name, ok, err := qualifiedTableEndingAt(tokens, tableEnd)
	if err != nil {
		return 0, "", "", err
	}
	if !ok {
		return 0, "", "", fmt.Errorf("AT time travel clause must follow a table in FROM or JOIN")
	}
	alias := " " + strings.TrimSpace(query[tokens[tableEnd].endOffset:tokens[atIndex].startOffset])
	return start, name, alias, nil
}

func qualifiedTableEndingAt(tokens []sqlToken, end int) (int, string, bool, error) {
	if end < 0 || !isIdentifier(tokens[end]) {
		return 0, "", false, nil
	}
	start := end
	for start >= 2 && tokens[start-1].text == "." && isIdentifier(tokens[start-2]) {
		start -= 2
	}
	if start == 0 || !(tokenIs(tokens[start-1], "FROM") || tokenIs(tokens[start-1], "JOIN") || tokens[start-1].text == ",") {
		return 0, "", false, nil
	}

	var parts []string
	for i := start; i <= end; i += 2 {
		part, err := normalizeIdentifier(tokens[i])
		if err != nil {
			return 0, "", false, err
		}
		parts = append(parts, part)
	}
	if len(parts) > 3 {
		return 0, "", false, fmt.Errorf("time travel expects table, schema.table, or catalog.schema.table, got %q", strings.Join(parts, "."))
	}
	return start, strings.Join(parts, "."), true, nil
}

func parseUTCTimestamp(value string) (time.Time, error) {
	offsetLayouts := []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05.999999999Z07:00",
	}
	for _, layout := range offsetLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	naiveLayouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02",
	}
	for _, layout := range naiveLayouts {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time travel timestamp %q", value)
}

func tokenizeSQL(query string) ([]sqlToken, error) {
	var tokens []sqlToken
	for i := 0; i < len(query); {
		if unicode.IsSpace(rune(query[i])) {
			i++
			continue
		}
		if i+1 < len(query) && query[i:i+2] == "--" {
			i += 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(query) && query[i:i+2] == "/*" {
			start := i
			i += 2
			depth := 1
			for i < len(query) && depth > 0 {
				if i+1 < len(query) && query[i:i+2] == "/*" {
					depth++
					i += 2
				} else if i+1 < len(query) && query[i:i+2] == "*/" {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return nil, fmt.Errorf("unterminated SQL comment at byte %d", start)
			}
			continue
		}

		start := i
		if query[i] == '$' {
			if delimiter, ok := dollarQuoteDelimiter(query[i:]); ok {
				closing := strings.Index(query[i+len(delimiter):], delimiter)
				if closing < 0 {
					return nil, fmt.Errorf("unterminated dollar-quoted SQL string at byte %d", start)
				}
				i += len(delimiter) + closing + len(delimiter)
				tokens = append(tokens, sqlToken{kind: tokenString, text: query[start:i], startOffset: start, endOffset: i})
				continue
			}
		}
		switch query[i] {
		case '\'':
			i++
			for i < len(query) {
				if query[i] == '\'' {
					if i+1 < len(query) && query[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			if i > len(query) || query[i-1] != '\'' {
				return nil, fmt.Errorf("unterminated SQL string at byte %d", start)
			}
			tokens = append(tokens, sqlToken{kind: tokenString, text: query[start:i], startOffset: start, endOffset: i})
		case '"':
			i++
			for i < len(query) {
				if query[i] == '"' {
					if i+1 < len(query) && query[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			if i > len(query) || query[i-1] != '"' {
				return nil, fmt.Errorf("unterminated quoted identifier at byte %d", start)
			}
			tokens = append(tokens, sqlToken{kind: tokenQuotedIdent, text: query[start:i], startOffset: start, endOffset: i})
		default:
			if isWordByte(query[i]) {
				i++
				for i < len(query) && isWordByte(query[i]) {
					i++
				}
				tokens = append(tokens, sqlToken{kind: tokenWord, text: query[start:i], startOffset: start, endOffset: i})
				continue
			}
			if i+1 < len(query) && query[i:i+2] == "=>" {
				i += 2
			} else {
				i++
			}
			tokens = append(tokens, sqlToken{kind: tokenSymbol, text: query[start:i], startOffset: start, endOffset: i})
		}
	}
	return tokens, nil
}

func dollarQuoteDelimiter(value string) (string, bool) {
	if len(value) < 2 || value[0] != '$' {
		return "", false
	}
	for i := 1; i < len(value); i++ {
		if value[i] == '$' {
			return value[:i+1], true
		}
		if !(value[i] == '_' || value[i] >= 'a' && value[i] <= 'z' || value[i] >= 'A' && value[i] <= 'Z' || value[i] >= '0' && value[i] <= '9') {
			return "", false
		}
	}
	return "", false
}

func isWordByte(b byte) bool {
	return b == '_' || b == '$' || b >= 0x80 || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

func isIdentifier(token sqlToken) bool {
	return token.kind == tokenWord || token.kind == tokenQuotedIdent
}

func tokenIs(token sqlToken, value string) bool {
	return token.kind == tokenWord && strings.EqualFold(token.text, value)
}

func normalizeIdentifier(token sqlToken) (string, error) {
	if token.kind == tokenWord {
		return strings.ToLower(token.text), nil
	}
	if token.kind != tokenQuotedIdent || len(token.text) < 2 {
		return "", fmt.Errorf("invalid SQL identifier %q", token.text)
	}
	return strings.ReplaceAll(token.text[1:len(token.text)-1], `""`, `"`), nil
}

func unquoteSQLString(value string) (string, error) {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return "", fmt.Errorf("expected a SQL string literal")
	}
	return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), nil
}
