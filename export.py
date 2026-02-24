import argparse
import json
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def ms_to_datetime(ms: int) -> datetime:
    """Convert milliseconds since epoch to a timezone-aware datetime object."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def datetime_to_ms(dt: datetime) -> int:
    """Convert a datetime object to milliseconds since epoch."""
    return int(dt.timestamp() * 1000)


def generate_filter(non_nullable_columns: str | None) -> str:
    """
    Generate a SQL condition to filter out rows where required columns are NULL or empty.
    """
    if not non_nullable_columns:
        return ""

    columns = non_nullable_columns.split(",")
    conditions = [f"{field} IS NOT NULL AND {field} != ''" for field in columns]
    return " AND ".join(conditions)


def validate_row_count(
    spark: SparkSession, catalog: str, schema: str, table: str, limit: int
) -> None:
    if limit <= 0:
        return

    try:
        result = spark.sql(f"SELECT count(*) FROM {catalog}.{schema}.{table}").first()
        if result is None or result[0] is None:
            raise Exception("Row count unavailable: query returned no result")
        row_count = int(result[0])
    except Exception as e:
        raise Exception(f"Row count unavailable: {e}")

    if row_count > limit:
        raise Exception(f"Row count {row_count} exceeds limit {limit}")


def _get_latest_commit_timestamp(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> datetime:
    history_result = spark.sql(
        f"SELECT timestamp FROM (DESCRIBE HISTORY {catalog}.{schema}.{table} LIMIT 1)"
    ).first()
    last_commit_dt = history_result[0]
    return last_commit_dt


def _get_latest_timestamp(spark: SparkSession) -> datetime:
    now_result = spark.sql("SELECT current_timestamp()").first()
    now_dt = now_result[0]
    return now_dt


def _extract_project_id(bucket: str) -> str | None:
    """
    TODO: pass in the actual project id
    Extract project ID from bucket name (e.g., 'mixpanel-3855718-d06b8cbdffe4ef68' -> '3855718').
    """
    match = re.search(r"mixpanel-(\d+)-", bucket)
    return match.group(1) if match else None


def __check_custom_sql_exists(catalog: str, schema: str, table: str, mixpanel_project: str) -> bool:
    path = f"dbfs:/external/mixpanel/{mixpanel_project}/queries/{catalog}__{schema}__{table}/recurring_query.sql"
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def _get_custom_sql_query(
    catalog: str,
    schema: str,
    table: str,
    mixpanel_project: str,
    time_cutoff_ms: int,
    end_dt: datetime,
) -> tuple[str, dict]:
    base_path = f"/external/mixpanel/{mixpanel_project}/queries/{catalog}__{schema}__{table}"

    if time_cutoff_ms == 0:
        query_path = f"{base_path}/initial_query.sql"
    else:
        query_path = f"{base_path}/recurring_query.sql"

    query = dbutils.fs.head(f"dbfs:{query_path}")

    # Add 1ms to exclude entries already processed (Databricks timestamps are at millisecond precision)
    cutoff_dt = ms_to_datetime(time_cutoff_ms + 1) if time_cutoff_ms > 0 else None

    query_params = {
        "end_timestamp": end_dt.isoformat(),
    }
    if cutoff_dt:
        query_params["start_timestamp"] = cutoff_dt.isoformat()

    return query, query_params


def _get_cdc_table_query(
    catalog: str,
    schema: str,
    table: str,
    time_cutoff_ms: int,
    end_dt: datetime,
) -> str:
    table_ref = f"{catalog}.{schema}.{table}"

    if time_cutoff_ms == 0:
        # First sync - table_changes has 30-day retention, so query the table directly
        return f"SELECT 'INSERT' as _mp_change_type, * FROM {table_ref} TIMESTAMP AS OF '{end_dt.isoformat()}'"
    # Add 1ms to exclude entries already processed (Databricks timestamps are at millisecond precision)
    cutoff_dt = ms_to_datetime(time_cutoff_ms + 1)
    return f"""
    SELECT CASE
        WHEN _change_type = 'update_postimage' THEN 'INSERT'
        WHEN _change_type = 'update_preimage' THEN 'DELETE'
        WHEN _change_type = 'insert' THEN 'INSERT'
        ELSE 'DELETE'
    END as _mp_change_type, *
    FROM table_changes('{table_ref}', '{cutoff_dt.isoformat()}', '{end_dt.isoformat()}')
    """


def build_query(spark: SparkSession, args: argparse.Namespace) -> tuple[str, dict, int]:
    """
    Build the SQL query for the given sync type.
    Returns a tuple of (query, change_capture_sync_last_commit_ms).
    change_capture_sync_last_commit_ms is truthy for CDC sync types.
    """
    table_ref = f"{args.catalog}.{args.schema_name}.{args.table}"

    if args.sync_type == "cdc":
        query_params = {}
        if args.time_cutoff_ms == 0:
            end_dt = _get_latest_commit_timestamp(spark, args.catalog, args.schema_name, args.table)
        else:
            end_dt = _get_latest_timestamp(spark)
        if __check_custom_sql_exists(
            args.catalog, args.schema_name, args.table, args.mixpanel_project
        ):
            query, query_params = _get_custom_sql_query(
                args.catalog,
                args.schema_name,
                args.table,
                args.mixpanel_project,
                args.time_cutoff_ms,
                end_dt,
            )
        else:
            query = _get_cdc_table_query(
                args.catalog, args.schema_name, args.table, args.time_cutoff_ms, end_dt
            )
        return query, query_params, datetime_to_ms(end_dt)
    if args.sync_type == "time-based":
        filter_condition = generate_filter(args.non_nullable_columns)
        query = f"SELECT * FROM {table_ref} WHERE unix_timestamp({args.updated_time_column})*1000 >= {args.time_cutoff_ms}"
        if filter_condition:
            query += f" AND {filter_condition}"
        if args.delay_ms > 0 and args.now_ms > 0:
            upper_bound_ms = args.now_ms - args.delay_ms
            query += f" AND unix_timestamp({args.updated_time_column})*1000 <= {upper_bound_ms}"
        return query, {}, 0
    elif args.sync_type == "full":
        filter_condition = generate_filter(args.non_nullable_columns)
        query = f"SELECT * FROM {table_ref}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        return query, {}, 0
    elif args.sync_type == "scd-latest":
        if not args.group_id_column or not args.scd_time_column:
            raise ValueError("scd-latest sync requires --group_id_column and --scd_time_column")
        filter_condition = generate_filter(args.non_nullable_columns)
        where_clause = f" WHERE {filter_condition}" if filter_condition else ""
        return (
            f"""SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY {args.group_id_column} ORDER BY {args.scd_time_column} DESC) AS row_num
    FROM {table_ref}{where_clause}
) RankedRows
WHERE row_num = 1""",
            {},
            0,
        )
    else:
        raise ValueError(f"Unknown sync_type: {args.sync_type}")


def export_to_gcs_with_query(
    spark: SparkSession, query: str, query_params: dict, args: argparse.Namespace
) -> None:
    spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", "true")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("fs.gs.project.id", args.gcp_project)
    spark.conf.set("fs.gs.auth.service.account.email", args.service_account_email)
    spark.conf.set("fs.gs.auth.service.account.private.key", args.service_account_key)
    spark.conf.set("fs.gs.auth.service.account.private.key.id", args.service_account_key_id)

    df = spark.sql(query, args=query_params)
    # Split the computed_hash_ignore_columns string into a list of column names
    ignore_columns = args.computed_hash_ignore_columns.split()
    if len(ignore_columns) > 0:
        filtered_cols = [c for c in df.columns if c not in ignore_columns]
        filtered_cols = [c for c in df.columns if c not in args.computed_hash_ignore_columns]
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
    parser = argparse.ArgumentParser(description="Spark to GCS unload using SparkPython")
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
    parser.add_argument("--service_account_key_id", help="key with which to authorize the gcs")
    parser.add_argument("--service_account_key", help="key with which to authorize the gcs")
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
    parser.add_argument(
        "--non_nullable_columns",
        type=str,
        help="Comma-separated list of columns that must not be NULL or empty (for full, scd-latest, time-based sync)",
    )
    parser.add_argument(
        "--group_id_column",
        type=str,
        help="Column to partition by for SCD latest sync (required for scd-latest)",
    )
    parser.add_argument(
        "--scd_time_column",
        type=str,
        help="Column to order by (descending) for SCD latest sync (required for scd-latest)",
    )

    args = parser.parse_args()
    # TODO: make more resilient
    args.mixpanel_project = _extract_project_id(args.bucket)

    validate_row_count(spark, args.catalog, args.schema_name, args.table, args.validate_row_count)
    query, query_params, change_capture_sync_last_commit_ms = build_query(spark, args)
    export_to_gcs_with_query(spark, query, query_params, args)
    resolved_query = query
    for key, value in query_params.items():
        resolved_query = resolved_query.replace(f":{key}", f"'{value}'")
    result = {
        "query": resolved_query,
        "change_capture_sync_last_commit_ms": change_capture_sync_last_commit_ms,
    }
    dbutils.notebook.exit(json.dumps(result))
