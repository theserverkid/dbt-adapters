"""Polars + PyIceberg connection wrapper for dbt-iceberg.

Executes SQL models using Polars SQLContext and manages Iceberg tables
via PyIceberg — no JVM, no Spark, no Thrift server required.

Inspired by the django-iceberg backend (polars_iceberg.backend.iceberg_manager).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import polars as pl
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.iceberg.connections import IcebergConnectionWrapper
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("Iceberg")

# Iceberg type name → Spark/dbt type string mapping
_ICEBERG_TYPE_MAP: Dict[str, str] = {
    "boolean": "boolean",
    "int": "int",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "date": "date",
    "time": "time",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "string": "string",
    "uuid": "string",
    "binary": "binary",
    "fixed": "binary",
}


def _iceberg_type_to_spark_str(iceberg_type: Any) -> str:
    """Convert a PyIceberg type object to a Spark-compatible type string."""
    type_str = str(iceberg_type).lower()
    # Handle decimal(p, s)
    if type_str.startswith("decimal"):
        return type_str
    # Handle list, map, struct as string representations
    if type_str.startswith(("list", "map", "struct")):
        return type_str
    return _ICEBERG_TYPE_MAP.get(type_str, "string")


class PolarsConnectionWrapper(IcebergConnectionWrapper):
    """Connection wrapper using Polars SQLContext + PyIceberg for SQL execution.

    All SQL models are executed in-process:
    - Source/ref tables are loaded from Iceberg via PyIceberg → Arrow → Polars
    - SQL is executed via pl.SQLContext
    - Results are written back to Iceberg via PyIceberg

    Catalog introspection (SHOW TABLES, DESCRIBE, etc.) calls PyIceberg directly.
    """

    def __init__(self, credentials: Any) -> None:
        self._credentials = credentials
        self._catalog = self._create_catalog()
        self._namespace = credentials.schema
        self._table_cache: Dict[str, pl.DataFrame] = {}
        self._view_registry: Dict[str, pl.DataFrame] = {}
        self._last_result: Optional[List[tuple]] = None
        self._last_description: Sequence[
            Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
        ] = []

    def _create_catalog(self) -> RestCatalog:
        """Create a PyIceberg RestCatalog from credentials."""
        creds = self._credentials
        props: Dict[str, str] = {
            "uri": creds.iceberg_rest_uri,
            "warehouse": creds.iceberg_warehouse,
        }
        if creds.iceberg_token:
            props["token"] = creds.iceberg_token

        # S3/MinIO credentials from environment or kubernetes_env_secret
        import os

        s3_endpoint = os.environ.get("S3_ENDPOINT")
        s3_key = os.environ.get("S3_ACCESS_KEY")
        s3_secret = os.environ.get("S3_SECRET_KEY")
        if s3_endpoint:
            props["s3.endpoint"] = s3_endpoint
            props["s3.path-style-access"] = "true"
        if s3_key:
            props["s3.access-key-id"] = s3_key
        if s3_secret:
            props["s3.secret-access-key"] = s3_secret

        return RestCatalog("dbt_catalog", **props)

    # --- IcebergConnectionWrapper interface ---

    def cursor(self) -> "PolarsConnectionWrapper":
        return self

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        self._table_cache.clear()
        self._view_registry.clear()

    def rollback(self) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Optional[List[tuple]]:
        return self._last_result

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        return self._last_description

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        """Route SQL to the appropriate handler."""
        sql = sql.strip()
        if sql.endswith(";"):
            sql = sql[:-1].strip()

        if not sql:
            self._last_result = []
            self._last_description = []
            return

        sql_upper = sql.upper()

        try:
            if sql_upper.startswith("SHOW DATABASES") or sql_upper.startswith("SHOW SCHEMAS"):
                self._handle_show_schemas()
            elif sql_upper.startswith("SHOW TABLES") or sql_upper.startswith("SHOW TABLE"):
                self._handle_show_tables(sql)
            elif sql_upper.startswith("DESCRIBE") or sql_upper.startswith("DESC "):
                self._handle_describe(sql)
            elif sql_upper.startswith("CREATE SCHEMA") or sql_upper.startswith(
                "CREATE DATABASE"
            ):
                self._handle_create_schema(sql)
            elif sql_upper.startswith("DROP TABLE") or sql_upper.startswith("DROP VIEW"):
                self._handle_drop(sql)
            elif "CREATE OR REPLACE TEMPORARY VIEW" in sql_upper or "CREATE TEMPORARY VIEW" in sql_upper:
                self._handle_create_temp_view(sql)
            elif sql_upper.startswith("CREATE") and "AS" in sql_upper:
                self._handle_ctas(sql)
            elif sql_upper.startswith("INSERT OVERWRITE"):
                self._handle_insert_overwrite(sql)
            elif sql_upper.startswith("INSERT INTO"):
                self._handle_insert_into(sql)
            elif sql_upper.startswith("MERGE INTO"):
                self._handle_merge(sql)
            elif sql_upper.startswith("ALTER TABLE"):
                self._handle_alter_table(sql)
            elif sql_upper.startswith("SELECT") or sql_upper.startswith("WITH"):
                self._handle_select(sql)
            else:
                # Unsupported SQL — fail fast with clear error
                raise DbtRuntimeError(
                    f"Polars engine does not support this SQL statement. "
                    f"Consider rewriting as a Python model.\n"
                    f"SQL: {sql[:200]}..."
                )
        except DbtRuntimeError:
            raise
        except Exception as e:
            raise DbtRuntimeError(
                f"Error executing SQL with Polars engine: {type(e).__name__}: {e}\n"
                f"SQL: {sql[:200]}"
            ) from e

    # --- SQL Handlers ---

    def _handle_show_schemas(self) -> None:
        namespaces = self._catalog.list_namespaces()
        self._last_result = [(ns[0],) for ns in namespaces]
        self._last_description = [("namespace", "string", None, None, None, None, True)]

    def _handle_show_tables(self, sql: str) -> None:
        # Extract schema from "SHOW TABLES IN schema" or "SHOW TABLE EXTENDED IN schema"
        match = re.search(r"(?:IN|FROM)\s+[`']?(\w+)[`']?", sql, re.IGNORECASE)
        namespace = match.group(1) if match else self._namespace

        try:
            tables = self._catalog.list_tables(namespace)
        except Exception:
            tables = []

        # dbt expects: (schema, tableName, isTemporary)
        self._last_result = [(namespace, tbl[1], False) for tbl in tables]
        self._last_description = [
            ("namespace", "string", None, None, None, None, True),
            ("tableName", "string", None, None, None, None, True),
            ("isTemporary", "boolean", None, None, None, None, True),
        ]

    def _handle_describe(self, sql: str) -> None:
        # Extract table name from DESCRIBE [EXTENDED] [FORMATTED] schema.table or table
        cleaned = re.sub(r"(?:EXTENDED|FORMATTED)", "", sql, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"^(?:DESCRIBE|DESC)\s+", "", cleaned, flags=re.IGNORECASE).strip()
        table_ref = cleaned.strip("`").strip("'").strip('"')

        namespace, table_name = self._parse_table_ref(table_ref)

        try:
            table = self._catalog.load_table((namespace, table_name))
        except NoSuchTableError:
            self._last_result = []
            self._last_description = []
            return

        schema = table.schema()
        rows = []
        for field in schema.fields:
            rows.append((field.name, _iceberg_type_to_spark_str(field.field_type), ""))

        # Add separator and metadata (dbt's parse_describe_extended expects this)
        rows.append(("", "", ""))
        rows.append(("# Detailed Table Information", "", ""))
        rows.append(("Provider", "iceberg", ""))
        rows.append(("Type", "TABLE", ""))
        rows.append(("Owner", "", ""))

        self._last_result = rows
        self._last_description = [
            ("col_name", "string", None, None, None, None, True),
            ("data_type", "string", None, None, None, None, True),
            ("comment", "string", None, None, None, None, True),
        ]

    def _handle_create_schema(self, sql: str) -> None:
        match = re.search(r"(?:SCHEMA|DATABASE)\s+(?:IF\s+NOT\s+EXISTS\s+)?[`']?(\w+)[`']?", sql, re.IGNORECASE)
        if match:
            namespace = match.group(1)
            try:
                self._catalog.create_namespace(namespace)
            except Exception:
                pass  # Already exists
        self._last_result = []
        self._last_description = []

    def _handle_drop(self, sql: str) -> None:
        match = re.search(r"(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?([`'\w.]+)", sql, re.IGNORECASE)
        if match:
            table_ref = match.group(1).strip("`").strip("'")
            namespace, table_name = self._parse_table_ref(table_ref)
            try:
                self._catalog.drop_table((namespace, table_name))
            except NoSuchTableError:
                pass
            self._table_cache.pop(f"{namespace}.{table_name}", None)
        self._last_result = []
        self._last_description = []

    def _handle_create_temp_view(self, sql: str) -> None:
        # CREATE [OR REPLACE] TEMPORARY VIEW name AS SELECT ...
        match = re.search(
            r"TEMPORARY\s+VIEW\s+([`'\w.]+)\s+AS\s+(.*)", sql, re.IGNORECASE | re.DOTALL
        )
        if not match:
            raise DbtRuntimeError(f"Could not parse CREATE TEMPORARY VIEW: {sql[:200]}")

        view_name = match.group(1).strip("`").strip("'")
        select_sql = match.group(2).strip()

        ctx = self._build_sql_context(select_sql)
        result = ctx.execute(select_sql).collect()
        self._view_registry[view_name] = result
        self._last_result = []
        self._last_description = []

    def _handle_ctas(self, sql: str) -> None:
        # CREATE [OR REPLACE] TABLE [schema.]name [USING iceberg] [PARTITIONED BY ...] AS SELECT ...
        match = re.search(
            r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+)([`'\w.]+)(.+?)\s+AS\s+(.*)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise DbtRuntimeError(f"Could not parse CREATE TABLE AS SELECT: {sql[:200]}")

        table_ref = match.group(1).strip("`").strip("'")
        select_sql = match.group(3).strip()
        namespace, table_name = self._parse_table_ref(table_ref)

        ctx = self._build_sql_context(select_sql)
        result_df = ctx.execute(select_sql).collect()
        arrow_table = result_df.to_arrow()

        self._write_to_iceberg(namespace, table_name, arrow_table, overwrite=True)

        self._last_result = []
        self._last_description = []

    def _handle_insert_into(self, sql: str) -> None:
        # INSERT INTO [TABLE] name SELECT ...
        match = re.search(
            r"INSERT\s+INTO\s+(?:TABLE\s+)?([`'\w.]+)\s+(.*)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise DbtRuntimeError(f"Could not parse INSERT INTO: {sql[:200]}")

        table_ref = match.group(1).strip("`").strip("'")
        select_sql = match.group(2).strip()
        namespace, table_name = self._parse_table_ref(table_ref)

        ctx = self._build_sql_context(select_sql)
        result_df = ctx.execute(select_sql).collect()
        arrow_table = result_df.to_arrow()

        # Append to existing table
        full_id = (namespace, table_name)
        try:
            iceberg_table = self._catalog.load_table(full_id)
            aligned = self._align_to_schema(arrow_table, iceberg_table.schema())
            iceberg_table.append(aligned)
        except NoSuchTableError:
            self._catalog.create_table(full_id, schema=arrow_table.schema)
            iceberg_table = self._catalog.load_table(full_id)
            iceberg_table.overwrite(arrow_table)

        self._table_cache.pop(f"{namespace}.{table_name}", None)
        self._last_result = []
        self._last_description = []

    def _handle_insert_overwrite(self, sql: str) -> None:
        # INSERT OVERWRITE [TABLE] name SELECT ...
        match = re.search(
            r"INSERT\s+OVERWRITE\s+(?:TABLE\s+)?([`'\w.]+)\s+(.*)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise DbtRuntimeError(f"Could not parse INSERT OVERWRITE: {sql[:200]}")

        table_ref = match.group(1).strip("`").strip("'")
        select_sql = match.group(2).strip()
        namespace, table_name = self._parse_table_ref(table_ref)

        ctx = self._build_sql_context(select_sql)
        result_df = ctx.execute(select_sql).collect()
        arrow_table = result_df.to_arrow()

        self._write_to_iceberg(namespace, table_name, arrow_table, overwrite=True)

        self._last_result = []
        self._last_description = []

    def _handle_merge(self, sql: str) -> None:
        """Implement MERGE INTO as Polars DataFrame operations.

        MERGE INTO target USING source ON (condition)
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        match = re.search(
            r"MERGE\s+INTO\s+([`'\w.]+)\s+(?:AS\s+\w+\s+)?USING\s+([`'\w.]+)\s+(?:AS\s+\w+\s+)?ON\s+\((.+?)\)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise DbtRuntimeError(
                f"Could not parse MERGE INTO statement. "
                f"Consider rewriting as a Python model.\n"
                f"SQL: {sql[:200]}"
            )

        target_ref = match.group(1).strip("`").strip("'")
        source_ref = match.group(2).strip("`").strip("'")
        on_condition = match.group(3).strip()

        target_ns, target_name = self._parse_table_ref(target_ref)
        source_ns, source_name = self._parse_table_ref(source_ref)

        # Load target from Iceberg
        target_df = self._load_table_as_polars(target_ns, target_name)

        # Load source (could be a temp view or Iceberg table)
        source_key = f"{source_ns}.{source_name}" if source_ns else source_name
        if source_name in self._view_registry:
            source_df = self._view_registry[source_name]
        elif source_key in self._view_registry:
            source_df = self._view_registry[source_key]
        else:
            source_df = self._load_table_as_polars(source_ns, source_name)

        # Extract join keys from ON condition (e.g., "target.id = source.id AND target.k = source.k")
        join_keys = self._parse_join_keys(on_condition)

        if not join_keys:
            raise DbtRuntimeError(
                f"Could not extract join keys from MERGE ON condition: {on_condition}. "
                f"Consider rewriting as a Python model."
            )

        # MERGE logic: keep unmatched target rows + all source rows
        unmatched_target = target_df.join(source_df, on=join_keys, how="anti")
        result_df = pl.concat([unmatched_target, source_df], how="diagonal_relaxed")

        arrow_table = result_df.to_arrow()
        self._write_to_iceberg(target_ns, target_name, arrow_table, overwrite=True)

        self._last_result = []
        self._last_description = []

    def _handle_alter_table(self, sql: str) -> None:
        # ALTER TABLE name ADD COLUMNS (col1 type1, col2 type2)
        match = re.search(
            r"ALTER\s+TABLE\s+([`'\w.]+)\s+ADD\s+COLUMNS?\s*\((.+)\)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if match:
            table_ref = match.group(1).strip("`").strip("'")
            columns_str = match.group(2).strip()
            namespace, table_name = self._parse_table_ref(table_ref)

            table = self._catalog.load_table((namespace, table_name))
            with table.update_schema() as update:
                for col_def in columns_str.split(","):
                    parts = col_def.strip().split()
                    if len(parts) >= 2:
                        col_name = parts[0].strip("`")
                        col_type = " ".join(parts[1:])
                        update.add_column(col_name, col_type)

            self._table_cache.pop(f"{namespace}.{table_name}", None)

        self._last_result = []
        self._last_description = []

    def _handle_select(self, sql: str) -> None:
        ctx = self._build_sql_context(sql)
        result_df = ctx.execute(sql).collect()

        # Convert to list of tuples
        self._last_result = [tuple(row) for row in result_df.iter_rows()]
        self._last_description = [
            (name, str(dtype), None, None, None, None, True)
            for name, dtype in zip(result_df.columns, result_df.dtypes)
        ]

    # --- Helpers ---

    def _parse_table_ref(self, ref: str) -> Tuple[str, str]:
        """Parse a table reference like 'schema.table' or 'table' into (namespace, table_name)."""
        ref = ref.strip("`").strip("'").strip('"')
        parts = ref.split(".")
        if len(parts) >= 2:
            return parts[-2], parts[-1]
        return self._namespace, parts[0]

    def _load_table_as_polars(self, namespace: str, table_name: str) -> pl.DataFrame:
        """Load an Iceberg table as a Polars DataFrame, with caching."""
        cache_key = f"{namespace}.{table_name}"
        if cache_key not in self._table_cache:
            try:
                table = self._catalog.load_table((namespace, table_name))
                arrow = table.scan().to_arrow()
                self._table_cache[cache_key] = pl.from_arrow(arrow)
            except NoSuchTableError:
                self._table_cache[cache_key] = pl.DataFrame()
        return self._table_cache[cache_key]

    def _build_sql_context(self, sql: str) -> pl.SQLContext:
        """Build a Polars SQLContext with all referenced tables registered."""
        ctx = pl.SQLContext()

        # Register any temp views
        for name, df in self._view_registry.items():
            ctx.register(name, df)

        # Find table references in the SQL and load them from Iceberg
        table_refs = self._extract_table_refs(sql)
        for ref in table_refs:
            namespace, table_name = self._parse_table_ref(ref)
            key = f"{namespace}.{table_name}"
            # Don't re-register if already registered as a view
            if ref not in self._view_registry and key not in self._view_registry:
                df = self._load_table_as_polars(namespace, table_name)
                # Register with both qualified and unqualified names
                ctx.register(key, df)
                ctx.register(table_name, df)

        return ctx

    def _extract_table_refs(self, sql: str) -> List[str]:
        """Extract table references from SQL.

        Looks for patterns like:
        - FROM schema.table
        - JOIN schema.table
        - FROM table
        - JOIN table
        """
        # Match FROM/JOIN followed by a table reference (not a subquery)
        pattern = r"(?:FROM|JOIN)\s+([`']?[\w]+(?:\.[\w]+)?[`']?)"
        matches = re.findall(pattern, sql, re.IGNORECASE)

        refs = []
        sql_keywords = {
            "select", "from", "where", "join", "on", "and", "or", "not",
            "in", "as", "left", "right", "inner", "outer", "cross", "full",
            "group", "order", "by", "having", "limit", "offset", "union",
            "except", "intersect", "with", "case", "when", "then", "else",
            "end", "null", "true", "false", "between", "like", "is", "exists",
            "values", "set", "into", "table", "view", "using", "lateral",
        }
        for m in matches:
            cleaned = m.strip("`").strip("'")
            if cleaned.lower() not in sql_keywords:
                refs.append(cleaned)
        return refs

    def _write_to_iceberg(
        self, namespace: str, table_name: str, arrow_table: pa.Table, overwrite: bool = True
    ) -> None:
        """Write an Arrow table to Iceberg (create or overwrite)."""
        full_id = (namespace, table_name)
        try:
            iceberg_table = self._catalog.load_table(full_id)
        except NoSuchTableError:
            iceberg_table = self._catalog.create_table(full_id, schema=arrow_table.schema)

        if overwrite:
            try:
                iceberg_table.overwrite(arrow_table)
            except Exception:
                # Schema mismatch — drop and recreate
                self._catalog.drop_table(full_id)
                iceberg_table = self._catalog.create_table(full_id, schema=arrow_table.schema)
                iceberg_table.overwrite(arrow_table)
        else:
            aligned = self._align_to_schema(arrow_table, iceberg_table.schema())
            iceberg_table.append(aligned)

        # Clear cache
        self._table_cache.pop(f"{namespace}.{table_name}", None)

    @staticmethod
    def _align_to_schema(arrow_table: pa.Table, iceberg_schema: Any) -> pa.Table:
        """Align an Arrow table to match an Iceberg schema (from django-iceberg pattern)."""
        target_schema = iceberg_schema.as_arrow()
        new_columns = []
        for target_field in target_schema:
            col_name = target_field.name
            if col_name in arrow_table.column_names:
                col = arrow_table.column(col_name)
                if col.type != target_field.type:
                    try:
                        col = col.cast(target_field.type)
                    except Exception:
                        pass
                new_columns.append(col)
            else:
                new_columns.append(pa.nulls(len(arrow_table), type=target_field.type))

        return pa.table(
            {field.name: col for field, col in zip(target_schema, new_columns)},
            schema=target_schema,
        )

    @staticmethod
    def _parse_join_keys(on_condition: str) -> List[str]:
        """Extract join key column names from a MERGE ON condition.

        Parses patterns like:
        - "target.id = source.id" → ["id"]
        - "target.id = source.id AND target.name = source.name" → ["id", "name"]
        """
        keys = []
        # Split on AND
        parts = re.split(r"\bAND\b", on_condition, flags=re.IGNORECASE)
        for part in parts:
            # Match "alias.col = alias.col" pattern
            match = re.search(r"[\w.]*\.(\w+)\s*=\s*[\w.]*\.(\w+)", part.strip())
            if match:
                left_col = match.group(1)
                right_col = match.group(2)
                # Use the column name (should be the same on both sides)
                keys.append(left_col if left_col == right_col else left_col)
        return keys
