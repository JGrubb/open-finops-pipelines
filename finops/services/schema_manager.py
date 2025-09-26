import json
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass

from finops.services.state_db import StateDB


@dataclass
class ColumnDefinition:
    """Represents a normalized column definition for DuckDB."""
    original_name: str
    normalized_name: str
    category: str
    aws_type: str
    duckdb_type: str


class SchemaManager:
    """Manages schema evolution and column mapping for DuckDB loading."""

    # AWS CUR type to DuckDB type mapping
    TYPE_MAPPING = {
        "String": "VARCHAR",
        "OptionalString": "VARCHAR",
        "BigDecimal": "DECIMAL(18,2)",
        "OptionalBigDecimal": "DECIMAL(18,2)",
        "DateTime": "TIMESTAMP",
        "Interval": "VARCHAR",  # Time intervals stored as strings
    }

    def __init__(self, state_db: StateDB):
        self.state_db = state_db

    def normalize_column_name(self, column_name: str) -> str:
        """
        Normalize column names by:
        1. Converting to lowercase
        2. Replacing non-alphanumeric characters with underscores
        3. Removing consecutive underscores
        4. Removing leading/trailing underscores
        5. Adding prefix if it starts with a number
        6. Adding suffix if it's a reserved word

        Examples:
        - 'aws:autoscaling:groupName' -> 'aws_autoscaling_group_name'
        - 'user:kubernetes.io/created-for/pv/name' -> 'user_kubernetes_io_created_for_pv_name'
        - 'LineItemId' -> 'line_item_id'
        - 'group' -> 'group_col' (reserved word)
        """
        # Convert camelCase to snake_case first
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', column_name)

        # Convert to lowercase
        name = name.lower()

        # Replace non-alphanumeric characters with underscores
        name = re.sub(r'[^a-z0-9]', '_', name)

        # Remove consecutive underscores
        name = re.sub(r'_+', '_', name)

        # Remove leading/trailing underscores
        name = name.strip('_')

        # Handle edge cases
        if not name:
            name = "unknown_column"

        # If starts with number, add prefix
        if name[0].isdigit():
            name = f"col_{name}"

        # If it's a reserved word, add suffix
        if self.is_reserved_word(name):
            name = f"{name}_col"

        return name

    def is_reserved_word(self, word: str) -> bool:
        """Check if a word is a SQL reserved word."""
        # Common SQL reserved words
        reserved_words = {
            'group', 'order', 'select', 'from', 'where', 'join', 'inner', 'outer',
            'left', 'right', 'on', 'as', 'and', 'or', 'not', 'in', 'exists',
            'between', 'like', 'is', 'null', 'true', 'false', 'case', 'when',
            'then', 'else', 'end', 'union', 'intersect', 'except', 'all',
            'distinct', 'limit', 'offset', 'having', 'by', 'asc', 'desc',
            'create', 'table', 'insert', 'update', 'delete', 'alter', 'drop',
            'index', 'view', 'database', 'schema', 'column', 'primary', 'key',
            'foreign', 'references', 'constraint', 'unique', 'check', 'default',
            'grant', 'revoke', 'user', 'role', 'commit', 'rollback', 'begin',
            'transaction', 'start', 'end'
        }
        return word.lower() in reserved_words

    def _parse_column(self, col: Dict) -> Tuple[str, str, str, str]:
        """Parse a column dict into its components."""
        category = col['category']
        name = col['name']
        original_name = f"{category}/{name}"
        aws_type = col['type']
        return category, name, original_name, aws_type

    def _get_duckdb_type(self, category: str, aws_type: str) -> str:
        """Get DuckDB type for a column, handling special cases."""
        if category == 'resourceTags':
            return "VARCHAR"
        return self.TYPE_MAPPING.get(aws_type, "VARCHAR")

    def get_duckdb_type(self, category: str, aws_type: str) -> str:
        """Public method to get DuckDB type for a column."""
        return self._get_duckdb_type(category, aws_type)

    def _resolve_duplicate_name(self, base_name: str, seen_names: Dict[str, int]) -> str:
        """Resolve duplicate normalized names by adding suffix."""
        if base_name in seen_names:
            seen_names[base_name] += 1
            return f"{base_name}_{seen_names[base_name]}"
        else:
            seen_names[base_name] = 0
            return base_name

    def _process_manifest_columns(self, manifest_columns: List[Dict],
                                 existing_columns: Optional[Set[str]] = None) -> List[ColumnDefinition]:
        """Core method to process manifest columns into ColumnDefinitions."""
        columns = []
        seen_normalized_names = {}

        for col in manifest_columns:
            category, _, original_name, aws_type = self._parse_column(col)
            base_normalized_name = self.normalize_column_name(original_name)
            normalized_name = self._resolve_duplicate_name(base_normalized_name, seen_normalized_names)

            # Filter by existing columns if specified
            if existing_columns is not None and normalized_name in existing_columns:
                continue

            duckdb_type = self._get_duckdb_type(category, aws_type)

            columns.append(ColumnDefinition(
                original_name=original_name,
                normalized_name=normalized_name,
                category=category,
                aws_type=aws_type,
                duckdb_type=duckdb_type
            ))

        return columns

    def get_unified_schema(self, billing_periods: Optional[List[str]] = None) -> List[ColumnDefinition]:
        """
        Build unified schema from all manifests or specific billing periods.
        Returns list of normalized column definitions.
        """
        if billing_periods:
            manifests = []
            for period in billing_periods:
                period_manifests = self.state_db.get_manifests_by_billing_period(period)
                manifests.extend(period_manifests)
        else:
            # Get all discovered and staged manifests
            discovered = self.state_db.get_manifests_by_state("discovered")
            staged = self.state_db.get_manifests_by_state("staged")
            manifests = discovered + staged

        # Collect all columns across all manifests, handling duplicates
        all_columns: Dict[str, ColumnDefinition] = {}
        seen_normalized_names = {}

        for manifest in manifests:
            columns_json = manifest['columns_schema']
            columns = json.loads(columns_json) if isinstance(columns_json, str) else columns_json

            for col in columns:
                # The actual CSV column name is category/name (e.g., "identity/LineItemId")
                category = col['category']
                name = col['name']
                original_name = f"{category}/{name}"
                base_normalized_name = self.normalize_column_name(original_name)
                aws_type = col['type']

                # Handle duplicate normalized names by adding suffix
                if base_normalized_name in seen_normalized_names:
                    seen_normalized_names[base_normalized_name] += 1
                    normalized_name = f"{base_normalized_name}_{seen_normalized_names[base_normalized_name]}"
                else:
                    seen_normalized_names[base_normalized_name] = 0
                    normalized_name = base_normalized_name

                # Force resourceTags to VARCHAR regardless of original type
                if category == 'resourceTags':
                    duckdb_type = "VARCHAR"
                else:
                    duckdb_type = self.TYPE_MAPPING.get(aws_type, "VARCHAR")

                # Use original_name as key since normalized names might have duplicates
                if original_name not in all_columns:
                    all_columns[original_name] = ColumnDefinition(
                        original_name=original_name,
                        normalized_name=normalized_name,
                        category=category,
                        aws_type=aws_type,
                        duckdb_type=duckdb_type
                    )

        # Sort columns for consistent ordering
        return sorted(all_columns.values(), key=lambda x: (x.category, x.normalized_name))

    def generate_create_table_sql(self, table_name: str, schema: List[ColumnDefinition]) -> str:
        """Generate CREATE TABLE SQL statement for DuckDB."""
        column_definitions = []

        for col in schema:
            # Column names are already normalized and safe, no need to quote
            column_definitions.append(f"    {col.normalized_name} {col.duckdb_type}")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        sql += ",\n".join(column_definitions)
        sql += "\n);"

        return sql

    def get_new_columns(self, _: str, existing_columns: Set[str],
                       manifest_columns: List[Dict]) -> List[ColumnDefinition]:
        """
        Identify new columns from a manifest that don't exist in the current table schema.
        Returns list of new column definitions.
        """
        return self._process_manifest_columns(manifest_columns, existing_columns)

    def generate_alter_table_sql(self, table_name: str, new_columns: List[ColumnDefinition]) -> List[str]:
        """Generate ALTER TABLE statements to add new columns."""
        statements = []

        for col in new_columns:
            # Column names are already normalized and safe
            sql = f"ALTER TABLE {table_name} ADD COLUMN {col.normalized_name} {col.duckdb_type};"
            statements.append(sql)

        return statements

    def create_column_mapping(self, manifest_columns: List[Dict]) -> Dict[str, str]:
        """
        Create mapping from original column names to normalized column names.
        Used for CSV column header transformation during loading.
        """
        columns = self._process_manifest_columns(manifest_columns)
        return {col.original_name: col.normalized_name for col in columns}

    def get_schema_summary(self, schema: List[ColumnDefinition]) -> Dict:
        """Get summary statistics about a schema."""
        categories = {}
        types = {}

        for col in schema:
            categories[col.category] = categories.get(col.category, 0) + 1
            types[col.duckdb_type] = types.get(col.duckdb_type, 0) + 1

        return {
            "total_columns": len(schema),
            "categories": dict(sorted(categories.items())),
            "types": dict(sorted(types.items()))
        }