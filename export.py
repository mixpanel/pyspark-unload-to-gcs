import argparse
from datetime import datetime

from pyspark.sql import SparkSession, functions as F


def collect_metadata(
    spark: SparkSession, catalog: str, schema: str, table: str
) -> dict:
    """
    Collect metadata needed for validation.

    Returns a dict with:
    - row_count: COUNT(*) for validation

    Note: Timestamps for CDC sync are now computed in build_cdc_query()
    """
    metadata: dict = {}

    # Row count for validation
    try:
        result = spark.sql(f"SELECT count(*) FROM {catalog}.{schema}.{table}").first()
        if result and result[0] is not None:
            metadata["row_count"] = int(result[0])
    except Exception as e:
        metadata["row_count_error"] = str(e)

    return metadata


def validate_row_count(metadata: dict, limit: int) -> None:
    """
    Validate row count against limit. Raises exception if exceeded.

    Args:
        metadata: metadata dict from collect_metadata()
        limit: row count limit (0 = no limit)

    Raises:
        Exception: if row count exceeds limit or is unavailable
    """
    if limit <= 0:
        return  # No validation needed

    row_count = metadata.get("row_count")
    if row_count is None:
        error = metadata.get("row_count_error", "Unknown error")
        raise Exception(f"Row count unavailable: {error}")

    if row_count > limit:
        raise Exception(f"Row count {row_count} exceeds limit {limit}")


def check_procedure_exists(
    spark: SparkSession, catalog: str, schema: str, procedure_name: str
) -> bool:
    """
    Check if a stored procedure exists in the specified catalog.schema.

    Args:
        spark: SparkSession
        catalog: Databricks catalog name
        schema: Databricks schema name
        procedure_name: Name of the procedure to check (e.g., 'get_mytable_cdc')

    Returns:
        bool: True if procedure exists, False otherwise
    """
    try:
        # DESCRIBE PROCEDURE returns rows if the procedure exists
        result = spark.sql(
            f"DESCRIBE PROCEDURE {catalog}.{schema}.{procedure_name}"
        ).collect()
        return len(result) > 0
    except Exception:
        # If DESCRIBE fails, the procedure doesn't exist
        return False


def build_cdc_query(
    spark: SparkSession, catalog: str, schema: str, table: str, time_cutoff_ms: int
) -> str:
    """
    Build CDC query for Change Data Capture sync.

    If a stored procedure named get_{table}_cdc exists, it will be used instead
    of the default table_changes query. This allows customers to apply custom
    transformations on top of CDC data.

    Procedure contract:
        CREATE PROCEDURE {catalog}.{schema}.get_{table}_cdc(
            start_timestamp STRING,
            end_timestamp STRING
        )
        RETURNS TABLE
        -- Must include _mp_change_type column with values 'INSERT' or 'DELETE'

    Args:
        spark: SparkSession
        catalog: Databricks catalog name
        schema: Databricks schema name
        table: Databricks table name
        time_cutoff_ms: Timestamp in milliseconds from last sync (0 for first sync)

    Returns:
        query_string: SQL query to execute
    """
    # First sync - snapshot at latest commit (procedure not used for initial sync)
    # Use DESCRIBE HISTORY to get the exact timestamp of the latest commit,
    # avoiding race conditions where current_timestamp() > latest commit time
    if time_cutoff_ms == 0:
        history_result = spark.sql(
            f"SELECT timestamp FROM (DESCRIBE HISTORY {catalog}.{schema}.{table} LIMIT 1)"
        ).first()
        ts = history_result[0]
        query = f"SELECT 'INSERT' as _mp_change_type, * FROM {catalog}.{schema}.{table} TIMESTAMP AS OF '{ts.isoformat()}'"
        return query

    # Subsequent sync - check for stored procedure first
    # Add 1ms to exclude entries that were already processed (Databricks timestamps are at millisecond precision)
    cutoff_dt = datetime.fromtimestamp((time_cutoff_ms + 1) / 1000.0)
    now_result = spark.sql("SELECT current_timestamp()").first()
    now = now_result[0]

    # Check if a CDC procedure exists for this table
    procedure_name = f"get_{table}_cdc"
    if check_procedure_exists(spark, catalog, schema, procedure_name):
        # Use stored procedure - it handles transformations and returns _mp_change_type
        query = f"CALL {catalog}.{schema}.{procedure_name}('{cutoff_dt.isoformat()}', '{now.isoformat()}')"
        return query

    # Fall back to default table_changes query
    query = f"""
    SELECT CASE
        WHEN _change_type = 'update_postimage' THEN 'INSERT'
        WHEN _change_type = 'update_preimage' THEN 'DELETE'
        WHEN _change_type = 'insert' THEN 'INSERT'
        ELSE 'DELETE'
    END as _mp_change_type, *
    FROM table_changes('{catalog}.{schema}.{table}', '{cutoff_dt.isoformat()}', '{now.isoformat()}')
    """
    return query


def build_query(spark: SparkSession, args: argparse.Namespace) -> str:
    """
    Build export query based on sync type.

    Args:
        spark: SparkSession
        args: Parsed command-line arguments

    Returns:
        query_string: SQL query to execute
    """
    table_ref = f"{args.catalog}.{args.schema_name}.{args.table}"

    if args.sync_type == "cdc":
        return build_cdc_query(
            spark, args.catalog, args.schema_name, args.table, args.time_cutoff_ms
        )
    elif args.sync_type == "time-based":
        # Lower bound: events after the cutoff time
        query = f"SELECT * FROM {table_ref} WHERE unix_timestamp({args.updated_time_column})*1000 >= {args.time_cutoff_ms}"
        # Upper bound: if delay is set, only include events before (now - delay)
        if args.delay_ms > 0 and args.now_ms > 0:
            upper_bound_ms = args.now_ms - args.delay_ms
            query += f" AND unix_timestamp({args.updated_time_column})*1000 <= {upper_bound_ms}"
        return query
    else:  # full or scd-latest
        query = f"SELECT * FROM {table_ref}"
        return query


def export_to_gcs_with_query(
    spark: SparkSession, query: str, args: argparse.Namespace
) -> None:
    """
    Export query results to GCS using Spark.

    Args:
        spark: SparkSession
        query: SQL query string to execute
        args: Parsed command-line arguments
    """
    spark.conf.set(
        "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", "true"
    )
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("fs.gs.project.id", args.gcp_project)
    spark.conf.set("fs.gs.auth.service.account.email", args.service_account_email)
    spark.conf.set("fs.gs.auth.service.account.private.key", args.service_account_key)
    spark.conf.set(
        "fs.gs.auth.service.account.private.key.id", args.service_account_key_id
    )

    df = spark.sql(query)
    # Split the computed_hash_ignore_columns string into a list of column names
    ignore_columns = args.computed_hash_ignore_columns.split()
    if len(ignore_columns) > 0:
        filtered_cols = [c for c in df.columns if c not in ignore_columns]
        filtered_cols = [
            c for c in df.columns if c not in args.computed_hash_ignore_columns
        ]
        filtered_cols.sort()
        # Create a struct containing all filtered columns
        struct_col = F.struct(*[F.col(c) for c in filtered_cols])
        # Convert the struct to a JSON string and compute hash
        if len(args.computed_hash_column) != 0:
            df = df.withColumn(args.computed_hash_column, F.md5(F.to_json(struct_col)))

    # Build write options with optional maxRecordsPerFile
    write_options = {"compression": "gzip"}
    if args.max_records_per_file is not None:
        write_options["maxRecordsPerFile"] = args.max_records_per_file

    if args.export_format == "csv":
        writer = df.coalesce(1).write.format(args.export_format)
        for key, value in write_options.items():
            writer = writer.option(key, value)
        writer.option("header", "true").mode("overwrite").save(
            f"gs://{args.bucket}//{args.prefix}/"
        )
    else:
        writer = df.write.format(args.export_format)
        for key, value in write_options.items():
            writer = writer.option(key, value)
        writer.mode("overwrite").save(f"gs://{args.bucket}//{args.prefix}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark to GCS unload using SparkPython"
    )
    # Will remove these arguments soon
    parser.add_argument("sql", help="results of sql query to unload", nargs="?")
    parser.add_argument(
        "export_format",
        help="format to export the data. Supports json and csv",
        default="json",
        nargs="?",
        choices=["json", "csv"],
    )
    parser.add_argument(
        "gcp_project", help="project in which gcs resources are", nargs="?"
    )
    parser.add_argument("bucket", help="gcs bucket to unload", nargs="?")
    parser.add_argument("prefix", help="gcs path to unload", nargs="?")
    parser.add_argument(
        "service_account_email",
        help="service account which has access to the gcs bucket and path",
        nargs="?",
    )
    parser.add_argument(
        "service_account_key_id", help="key with which to authorize the gcs", nargs="?"
    )
    parser.add_argument(
        "service_account_key", help="key with which to authorize the gcs", nargs="?"
    )
    parser.add_argument(
        "computed_hash_column",
        help="column to emit for computed hash",
        default="",
        nargs="?",
    )
    parser.add_argument(
        "computed_hash_ignore_columns",
        help="ignore the passed columns from hash computation",
        default="",
        nargs="?",
    )
    parser.add_argument(
        "max_records_per_file",
        type=int,
        nargs="?",
        default=None,
        help="maximum number of records per output file to limit compressed .gz file size (optional)",
    )
    # Positional arguments translated to named arguments
    parser.add_argument(
        "--export_format",
        help="format to export the data. Supports json and csv",
        default="json",
        choices=["json", "csv"],
    )
    parser.add_argument("--gcp_project", help="project in which gcs resources are")
    parser.add_argument("--bucket", help="gcs bucket to unload")
    parser.add_argument("--prefix", help="gcs path to unload")
    parser.add_argument(
        "--service_account_email",
        help="service account which has access to the gcs bucket and path",
    )
    parser.add_argument(
        "--service_account_key_id", help="key with which to authorize the gcs"
    )
    parser.add_argument(
        "--service_account_key", help="key with which to authorize the gcs"
    )
    parser.add_argument(
        "--computed_hash_column", help="column to emit for computed hash", default=""
    )
    parser.add_argument(
        "--computed_hash_ignore_columns",
        help="ignore the passed columns from hash computation",
        default="",
    )
    parser.add_argument(
        "--max_records_per_file",
        type=int,
        nargs="?",
        default=None,
        help="maximum number of records per output file to limit compressed .gz file size",
    )
    # New arguments for v2 export
    parser.add_argument(
        "--sync_type",
        type=str,
        choices=["time-based", "full", "scd-latest", "cdc"],
        help="Type of sync (time-based, full, scd-latest, cdc)",
    )
    parser.add_argument("--catalog", type=str, help="Databricks catalog name")
    parser.add_argument("--schema_name", type=str, help="Databricks schema name")
    parser.add_argument("--table", type=str, help="Databricks table name")
    parser.add_argument(
        "--collect_metadata",
        type=str,
        help="Collect metadata (row count) before export ('true' or 'false')",
    )
    parser.add_argument(
        "--validate_row_count",
        type=int,
        help="Fail if row count exceeds this limit (0=no limit)",
    )
    parser.add_argument(
        "--time_cutoff_ms",
        type=int,
        help="Time cutoff in milliseconds for incremental sync (0 for first sync)",
    )
    parser.add_argument(
        "--updated_time_column",
        type=str,
        help="Column name for time-based filtering (empty string if not applicable)",
    )
    parser.add_argument(
        "--delay_ms",
        type=int,
        help="Delay in milliseconds for adspend (0 if no delay)",
    )
    parser.add_argument(
        "--now_ms",
        type=int,
        help="Current time in milliseconds from the Go server (for consistent time filtering for adspend)",
    )

    args = parser.parse_args()

    # V1 path (use SQL passed in as argument)
    if args.sql:
        export_to_gcs_with_query(spark, args.sql, args)
    else:
        # V2 path (construct SQL query in Python code)
        if args.collect_metadata.lower() == "true":
            metadata = collect_metadata(
                spark, args.catalog, args.schema_name, args.table
            )
            validate_row_count(metadata, args.validate_row_count)
        query = build_query(spark, args)
        export_to_gcs_with_query(spark, query, args)
