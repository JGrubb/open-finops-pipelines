# DuckDB Loader Algorithm Specification

## Overview
This document describes the complete algorithm for loading AWS Cost and Usage Report (CUR) data from staged CSV files into DuckDB with robust schema evolution and column name normalization.

## Core Problem
AWS CUR data has several challenges:
1. **CSV headers use format**: `category/ColumnName` (e.g., `identity/LineItemId`, `resourceTags/user:Environment`)
2. **Manifest columns use format**: `{"category": "identity", "name": "LineItemId", "type": "String"}`
3. **Schema evolution**: New columns appear in different months/customers
4. **Reserved words**: Column names like `group`, `order` conflict with SQL keywords
5. **Duplicate normalized names**: Different original names normalize to same result
6. **Resource tags**: Always should be VARCHAR regardless of declared type

## Column Name Normalization Algorithm

### Step 1: Base Normalization
For any column name (input: string) → normalized string:

1. **Convert camelCase to snake_case**: `LineItemId` → `line_item_id`
   - Use regex: `([a-z0-9])([A-Z])` → `\1_\2`
2. **Convert to lowercase**: `INVOICE_ID` → `invoice_id`
3. **Replace non-alphanumeric with underscores**: `user:Environment` → `user_environment`
   - Use regex: `[^a-z0-9]` → `_`
4. **Collapse consecutive underscores**: `user__env` → `user_env`
   - Use regex: `_+` → `_`
5. **Strip leading/trailing underscores**: `_column_` → `column`

### Step 2: Handle Edge Cases
1. **Empty names**: `""` → `"unknown_column"`
2. **Starts with digit**: `2factor` → `col_2factor`
3. **SQL reserved words**: Apply suffix `_col`
   - Reserved words list: `group`, `order`, `select`, `from`, `where`, `join`, etc.

### Step 3: Handle Duplicates (Critical)
When processing multiple columns that normalize to the same name:
1. **First occurrence**: Use normalized name as-is
2. **Second occurrence**: Append `_1` suffix
3. **Third occurrence**: Append `_2` suffix
4. **Continue incrementing**: `_3`, `_4`, etc.

**Example**:
- `resourceTags/user:Environment` → `resource_tags_user_environment`
- `resourceTags/user:environment` → `resource_tags_user_environment_1`

## Schema Evolution Strategy

### Unified Schema Creation
1. **Collect all columns** from all manifests (discovered + staged states)
2. **For each manifest column**:
   - Original name = `f"{category}/{name}"` (e.g., `"identity/LineItemId"`)
   - Apply normalization algorithm with duplicate handling
   - Map AWS type to DuckDB type (see type mapping below)
   - Force resourceTags category to VARCHAR regardless of declared type

### Type Mapping (AWS CUR → DuckDB)
```
"String" → "VARCHAR"
"OptionalString" → "VARCHAR"
"BigDecimal" → "DECIMAL(18,2)"
"OptionalBigDecimal" → "DECIMAL(18,2)"
"DateTime" → "TIMESTAMP"
"Interval" → "VARCHAR"
default → "VARCHAR"
```

### Special Rule: ResourceTags Override
If `category == "resourceTags"`, always use `"VARCHAR"` regardless of declared type.

## Table Schema Management Algorithm

### Initial Table Creation
1. **Check if table exists** using `information_schema.columns`
2. **If table doesn't exist**:
   - Generate unified schema from ALL known manifests
   - Create `CREATE TABLE` statement with all normalized column names
   - Execute table creation

### Schema Evolution (Per Manifest)
1. **Get existing table columns** from `information_schema.columns`
2. **Process current manifest columns** with normalization + duplicate handling
3. **Identify new columns** not in existing table schema
4. **For each new column**:
   - Generate `ALTER TABLE {table} ADD COLUMN {normalized_name} {type};`
   - Execute ALTER statement

## CSV Loading Algorithm

### Column Mapping Creation
For each CSV file being loaded:

1. **Read CSV header row** (handle gzip compression)
2. **Create column mapping** with duplicate handling:
   ```python
   normalized_columns = []
   seen_normalized_names = {}

   for original_col in header:
       base_normalized = normalize_column_name(original_col)

       if base_normalized in seen_normalized_names:
           seen_normalized_names[base_normalized] += 1
           final_name = f"{base_normalized}_{seen_normalized_names[base_normalized]}"
       else:
           seen_normalized_names[base_normalized] = 0
           final_name = base_normalized

       normalized_columns.append(final_name)
   ```

### Data Type Specification for read_csv
For each column in CSV header:

1. **Find matching manifest column definition**:
   - Look for manifest entry where `f"{category}/{name}"` == CSV header
2. **Determine DuckDB type**:
   - If category == "resourceTags": use "VARCHAR"
   - Else: map AWS type using type mapping table
   - Fallback: "VARCHAR"
3. **Build column spec**: `"'{normalized_name}': '{duckdb_type}'"`

### DuckDB Insertion
Use `INSERT INTO ... SELECT FROM read_csv()` pattern:

```sql
INSERT INTO {table_name} ({comma_separated_normalized_columns})
SELECT * FROM read_csv(
    '{csv_path}',
    columns = {{'{col1}': '{type1}', '{col2}': '{type2}', ...}},
    header = true,
    delim = ',',
    compression = '{'gzip' if gz_file else 'none'}'
)
```

## State Management

### Pipeline States
```
discovered → staging → loading → loaded
     ↓          ↓         ↓
   failed    failed    failed
```

### State Transitions
1. **Before loading manifest**: Update state to "loading"
2. **After successful load**: Update state to "loaded"
3. **On any error**: Update state to "failed" with error message

## Processing Order
1. **Sort manifests** by `billing_period DESC` (newest first for consistency)
2. **Process each manifest atomically** (all files succeed or all fail)
3. **For each CSV file in manifest**:
   - Ensure table schema is current
   - Load CSV with proper column mapping
   - Track row counts

## Error Handling
1. **Missing CSV files**: Warn but continue with other files
2. **Schema errors**: Fail manifest and update state to "failed"
3. **Data loading errors**: Fail manifest and update state to "failed"
4. **Always preserve error messages** in state database

## Critical Implementation Details

### Duplicate Handling Consistency
The same duplicate handling logic MUST be used in:
- Unified schema creation (`get_unified_schema`)
- Column mapping creation (`create_column_mapping`)
- New column detection (`get_new_columns`)
- CSV column processing (`load_csv_file`)

### CSV Header Format
AWS CUR CSV headers are in format: `category/ColumnName`
- Example: `identity/LineItemId`, `bill/PayerAccountId`, `resourceTags/user:Environment`

### Manifest Column Format
AWS CUR manifest columns are objects:
```json
{"category": "identity", "name": "LineItemId", "type": "String"}
```

### Reserved Words List
Minimum required reserved words to handle:
`group`, `order`, `select`, `from`, `where`, `join`, `inner`, `outer`, `left`, `right`, `on`, `as`, `and`, `or`, `not`, `in`, `exists`, `between`, `like`, `is`, `null`, `true`, `false`, `case`, `when`, `then`, `else`, `end`, `union`, `intersect`, `except`, `all`, `distinct`, `limit`, `offset`, `having`, `by`, `asc`, `desc`, `create`, `table`, `insert`, `update`, `delete`, `alter`, `drop`, `user`, `role`

## Success Criteria
- Load 1M+ rows successfully
- Handle 250+ columns with proper types
- Support schema evolution across billing periods
- Handle duplicate column names consistently
- Preserve data types from manifest specifications
- Provide accurate row count reporting