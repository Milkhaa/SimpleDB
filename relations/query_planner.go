package relations

import "fmt"

// query_planner.go holds WHERE validation and index-matching logic for SELECT (and UPDATE/DELETE).
// It decides which index (or primary key) to use for a given set of WHERE equality predicates.

// validateWhereKeys ensures keys have no duplicate or unknown columns. Does not require full PK or index.
func validateWhereKeys(schema *Schema, keys []sqlNamedCell) error {
	seen := make(map[string]bool)
	for _, k := range keys {
		if columnIndex(schema, k.Column) < 0 {
			return fmt.Errorf("unknown column %q", k.Column)
		}
		if seen[k.Column] {
			return fmt.Errorf("duplicate column %q in WHERE", k.Column)
		}
		seen[k.Column] = true
	}
	return nil
}

// validateWhereIsFullPKey ensures keys specify exactly the primary key columns (no more, no less).
func validateWhereIsFullPKey(schema *Schema, keys []sqlNamedCell) error {
	if err := validateWhereKeys(schema, keys); err != nil {
		return err
	}
	pkey := schema.PKey()
	if len(keys) != len(pkey) {
		return fmt.Errorf("WHERE must specify all %d primary key column(s), got %d", len(pkey), len(keys))
	}
	keySet := whereColumnSet(keys)
	for _, pkeyIdx := range pkey {
		name := schema.Cols[pkeyIdx].Name
		if !keySet[name] {
			return fmt.Errorf("WHERE must include primary key column %q", name)
		}
	}
	return nil
}

// indexColumnNames returns the column names for the given index (indices into schema.Cols).
func indexColumnNames(schema *Schema, indexID int) []string {
	if indexID < 0 || indexID >= len(schema.Indices) {
		return nil
	}
	names := make([]string, 0, len(schema.Indices[indexID]))
	for _, colIdx := range schema.Indices[indexID] {
		if colIdx >= 0 && colIdx < len(schema.Cols) {
			names = append(names, schema.Cols[colIdx].Name)
		}
	}
	return names
}

// whereColumnSet returns the set of column names in keys (for set comparison).
func whereColumnSet(keys []sqlNamedCell) map[string]bool {
	m := make(map[string]bool)
	for _, k := range keys {
		m[k.Column] = true
	}
	return m
}

// matchIndexForWhere returns which lookup to use for a SELECT with the given WHERE keys.
//
// Allowed SELECT WHERE clauses: equality only (col = val [and col2 = val2 ...]). The set of
// column names in WHERE must match exactly one of:
//   - The full primary key → use primary-key lookup (returns indexID 0, at most one row).
//   - The full column list of one secondary index → use that index (returns indexID 1.., zero or more rows).
//
// Choice order: we first check if the WHERE columns are exactly the primary key; if so, return 0.
// Otherwise we look for a secondary index (Indices[1], Indices[2], ...) whose columns exactly
// match the WHERE set (same count, every index column present in WHERE). The first such index
// is used. If no match, we return an error (e.g. partial key or unknown column set).
//
// Examples (table t with primary key (a), index (c), index (b, c)):
//
//	where a = 'x'           → primary key (indexID 0)
//	where c = '45'          → index (c) (indexID 1)
//	where b = 'y' and c = '45' → index (b, c) (indexID 2)
//	where c = '45' and b = 'y' → same as above (column set matches; order in WHERE does not matter)
//	where b = 'y'           → error (no index on (b) only)
func matchIndexForWhere(schema *Schema, keys []sqlNamedCell) (indexID int, err error) {
	if err := validateWhereKeys(schema, keys); err != nil {
		return 0, err
	}
	keySet := whereColumnSet(keys)
	pkey := schema.PKey()
	if len(keys) == len(pkey) {
		all := true
		for _, idx := range pkey {
			if !keySet[schema.Cols[idx].Name] {
				all = false
				break
			}
		}
		if all {
			return 0, nil
		}
	}
	for id := 1; id < len(schema.Indices); id++ {
		idxNames := indexColumnNames(schema, id)
		if len(keys) != len(idxNames) {
			continue
		}
		match := true
		for _, n := range idxNames {
			if !keySet[n] {
				match = false
				break
			}
		}
		if match {
			return id, nil
		}
	}
	return 0, fmt.Errorf("WHERE must specify either the full primary key or a full index (e.g. index (c) for where c = val)")
}
