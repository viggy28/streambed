//go:build integration

package querycompat

import (
	"fmt"
	"regexp"
)

type expectation string

const (
	supported       expectation = "supported"
	unsupported     expectation = "unsupported"
	knownDifference expectation = "known_difference"
)

type expectedBehavior struct {
	Expectation expectation
	ErrorMatch  string
	IgnoreOIDs  []int
	Reason      string
}

type queryCase struct {
	Name         string
	Category     string
	Query        string
	Ordered      bool
	FloatEpsilon float64
	Default      expectedBehavior
	ByTarget     map[targetFormat]expectedBehavior
}

func (tc queryCase) behavior(target targetFormat) expectedBehavior {
	if behavior, ok := tc.ByTarget[target]; ok {
		return behavior
	}
	return tc.Default
}

func supportedCase(name, category, query string) queryCase {
	return queryCase{
		Name:     name,
		Category: category,
		Query:    query,
		Ordered:  true,
		Default:  expectedBehavior{Expectation: supported},
	}
}

var queryCases = []queryCase{
	supportedCase("integer projection", "selection", `SELECT id, int4_value FROM oracle_values ORDER BY id`),
	supportedCase("boolean filter", "selection", `SELECT id FROM oracle_values WHERE bool_value IS TRUE ORDER BY id`),
	supportedCase("limit and offset", "selection", `SELECT id FROM oracle_values ORDER BY id LIMIT 2 OFFSET 1`),
	supportedCase("distinct with null", "selection", `SELECT DISTINCT group_id FROM oracle_values ORDER BY group_id NULLS LAST`),
	supportedCase("coalesce", "nulls", `SELECT id, coalesce(text_value, 'missing') AS value FROM oracle_values ORDER BY id`),
	supportedCase("nullif", "nulls", `SELECT id, nullif(int4_value, 0) AS value FROM oracle_values ORDER BY id`),
	supportedCase("case expression", "expressions", `SELECT id, CASE WHEN bool_value THEN 'yes' WHEN bool_value IS FALSE THEN 'no' ELSE 'unknown' END AS value FROM oracle_values ORDER BY id`),
	supportedCase("integer arithmetic", "expressions", `SELECT id, int4_value + 1 AS value FROM oracle_values WHERE int4_value IS NOT NULL ORDER BY id`),
	supportedCase("integer comparison", "expressions", `SELECT id, int8_value > 0 AS positive FROM oracle_values WHERE int8_value IS NOT NULL ORDER BY id`),
	supportedCase("lower", "strings", `SELECT id, lower(text_value) AS value FROM oracle_values ORDER BY id`),
	supportedCase("concatenation", "strings", `SELECT id, text_value || ':' || id::text AS value FROM oracle_values ORDER BY id`),
	supportedCase("substring", "strings", `SELECT id, substring(text_value FROM 1 FOR 3) AS value FROM oracle_values ORDER BY id`),
	supportedCase("ilike", "strings", `SELECT id FROM oracle_values WHERE text_value ILIKE '%HELLO%' ORDER BY id`),
	supportedCase("date projection", "datetime", `SELECT id, date_value FROM oracle_values ORDER BY id`),
	supportedCase("date arithmetic", "datetime", `SELECT id, date_value + 1 AS value FROM oracle_values ORDER BY id`),
	supportedCase("timestamp projection", "datetime", `SELECT id, timestamp_value FROM oracle_values ORDER BY id`),
	supportedCase("timestamptz projection", "datetime", `SELECT id, timestamptz_value FROM oracle_values ORDER BY id`),
	supportedCase("count", "aggregates", `SELECT count(*) AS value FROM oracle_values`),
	supportedCase("minimum integer", "aggregates", `SELECT min(int4_value) AS value FROM oracle_values`),
	supportedCase("maximum bigint", "aggregates", `SELECT max(int8_value) AS value FROM oracle_values`),
	supportedCase("grouped count", "aggregates", `SELECT group_id, count(*) AS value FROM oracle_values GROUP BY group_id ORDER BY group_id NULLS LAST`),
	supportedCase("inner join", "joins", `SELECT v.id, g.name FROM oracle_values v JOIN oracle_groups g ON g.id = v.group_id ORDER BY v.id`),
	supportedCase("left join", "joins", `SELECT g.id, count(v.id) AS value FROM oracle_groups g LEFT JOIN oracle_values v ON v.group_id = g.id GROUP BY g.id ORDER BY g.id`),
	supportedCase("common table expression", "advanced", `WITH selected AS (SELECT id, group_id FROM oracle_values WHERE id <= 3) SELECT id, group_id FROM selected ORDER BY id`),
	supportedCase("exists subquery", "advanced", `SELECT g.id, EXISTS (SELECT 1 FROM oracle_values v WHERE v.group_id = g.id) AS has_values FROM oracle_groups g ORDER BY g.id`),
	supportedCase("row number window", "advanced", `SELECT id, row_number() OVER (PARTITION BY group_id ORDER BY id) AS value FROM oracle_values ORDER BY id`),
	supportedCase("union all", "advanced", `SELECT id FROM oracle_groups UNION ALL SELECT id FROM oracle_values WHERE id > 3 ORDER BY id`),
	supportedCase("boolean projection", "types", `SELECT id, bool_value FROM oracle_values ORDER BY id`),
	supportedCase("uuid projection", "types", `SELECT id, uuid_value FROM oracle_values ORDER BY id`),
	supportedCase("bytea projection", "types", `SELECT id, bytea_value FROM oracle_values ORDER BY id`),
	supportedCase("real projection", "types", `SELECT id, real_value FROM oracle_values ORDER BY id`),
	supportedCase("double projection", "types", `SELECT id, double_value FROM oracle_values ORDER BY id`),
	{
		Name: "smallint storage mapping", Category: "types",
		Query: `SELECT id, int2_value FROM oracle_values ORDER BY id`, Ordered: true,
		Default: expectedBehavior{
			Expectation: knownDifference,
			IgnoreOIDs:  []int{1},
			Reason:      "Iceberg maps Postgres SMALLINT to Iceberg int, which DuckDB exposes as INTEGER",
		},
		ByTarget: map[targetFormat]expectedBehavior{
			targetDuckLake: {Expectation: supported},
		},
	},
	{
		Name: "numeric storage mapping", Category: "types",
		Query: `SELECT id, numeric_value FROM oracle_values ORDER BY id`, Ordered: true,
		Default: expectedBehavior{
			Expectation: knownDifference,
			IgnoreOIDs:  []int{1},
			Reason:      "numeric values are currently stored and exposed as strings",
		},
	},
	{
		Name: "unconstrained numeric storage mapping", Category: "types",
		Query: `SELECT id, unconstrained_value FROM oracle_values ORDER BY id`, Ordered: true,
		Default: expectedBehavior{
			Expectation: knownDifference,
			IgnoreOIDs:  []int{1},
			Reason:      "unconstrained numeric values are currently stored and exposed as strings",
		},
	},
	{
		Name: "varchar storage mapping", Category: "types",
		Query: `SELECT id, varchar_value FROM oracle_values ORDER BY id`, Ordered: true,
		Default: expectedBehavior{
			Expectation: knownDifference,
			IgnoreOIDs:  []int{1},
			Reason:      "DuckDB VARCHAR is exposed as Postgres TEXT rather than VARCHAR",
		},
	},
	{
		Name: "jsonb storage mapping", Category: "types",
		Query: `SELECT id, jsonb_value FROM oracle_values ORDER BY id`, Ordered: true,
		Default: expectedBehavior{
			Expectation: knownDifference,
			IgnoreOIDs:  []int{1},
			Reason:      "JSONB is currently stored and exposed as text",
		},
	},
	{
		Name: "postgres session function", Category: "unsupported",
		Query: `SELECT pg_backend_pid() AS value`, Ordered: true,
		Default: expectedBehavior{
			Expectation: unsupported,
			ErrorMatch:  `(?i)(pg_backend_pid|does not exist|not found)`,
			Reason:      "DuckDB does not implement Postgres backend session functions",
		},
	},
}

func validateCases() error {
	seen := make(map[string]struct{}, len(queryCases))
	for _, tc := range queryCases {
		if tc.Name == "" || tc.Category == "" || tc.Query == "" {
			return fmt.Errorf("query case has empty name, category, or query: %+v", tc)
		}
		if _, exists := seen[tc.Name]; exists {
			return fmt.Errorf("duplicate query case name %q", tc.Name)
		}
		seen[tc.Name] = struct{}{}
		for _, target := range []targetFormat{targetIceberg, targetDuckLake} {
			behavior := tc.behavior(target)
			switch behavior.Expectation {
			case supported:
				if behavior.ErrorMatch != "" || len(behavior.IgnoreOIDs) > 0 {
					return fmt.Errorf("supported case %q has error/OID exceptions for %s", tc.Name, target)
				}
			case unsupported:
				if behavior.ErrorMatch == "" || behavior.Reason == "" {
					return fmt.Errorf("unsupported case %q needs error match and reason for %s", tc.Name, target)
				}
				if _, err := regexp.Compile(behavior.ErrorMatch); err != nil {
					return fmt.Errorf("unsupported case %q has invalid error regex: %w", tc.Name, err)
				}
			case knownDifference:
				if behavior.Reason == "" || len(behavior.IgnoreOIDs) == 0 {
					return fmt.Errorf("known difference %q needs reason and explicit OID exceptions for %s", tc.Name, target)
				}
			default:
				return fmt.Errorf("case %q has invalid expectation %q for %s", tc.Name, behavior.Expectation, target)
			}
		}
	}
	return nil
}
