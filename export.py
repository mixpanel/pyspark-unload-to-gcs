import argparse
from pyspark.sql import functions as F

def export_to_gcs(args):
    """
    This function takes in GCS credentials and unloads the results of a
    Query to GCS using spark.
    """
    spark.conf.set("spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled", "true")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("fs.gs.project.id", args.gcp_project)
    spark.conf.set("fs.gs.auth.service.account.email", args.service_account_email)
    spark.conf.set("fs.gs.auth.service.account.private.key", args.service_account_key)
    spark.conf.set("fs.gs.auth.service.account.private.key.id", args.service_account_key_id)

    df = spark.sql(args.sql)
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
        writer.option("header", "true").mode("overwrite").save(f"gs://{args.bucket}//{args.prefix}/")
    else:
        writer = df.write.format(args.export_format)
        for key, value in write_options.items():
            writer = writer.option(key, value)
        writer.mode("overwrite").save(f"gs://{args.bucket}//{args.prefix}/")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Spark to GCS unload using SparkPython')
    parser.add_argument("sql", help="results of sql query to unload")
    parser.add_argument('export_format', help='format to export the data. Supports json and csv', default='json', choices=['json', 'csv'])
    parser.add_argument('gcp_project', help='project in which gcs resources are')
    parser.add_argument("bucket", help="gcs bucket to unload")
    parser.add_argument("prefix", help="gcs path to unload")
    parser.add_argument('service_account_email', help='service account which has access to the gcs bucket and path')
    parser.add_argument('service_account_key_id', help='key with which to authorize the gcs')
    parser.add_argument('service_account_key', help='key with which to authorize the gcs')
    parser.add_argument('computed_hash_column', help="column to emit for computed hash", default='')
    parser.add_argument('computed_hash_ignore_columns', help="ignore the passed columns from hash computation", default='')
    parser.add_argument('max_records_per_file', type=int, nargs='?', default=None, help="maximum number of records per output file to limit compressed .gz file size (optional)")

    args = parser.parse_args()
    export_to_gcs(args)
