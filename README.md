# Unload to GCS using Pyspark

Pyspark script to unload from spark to GCS. 

Use this script as a job in databricks with the task type SparkPython.
Pass a SQL query to unload, gcs paths, and credentials in as arguments.

# Development

When creating the PR, push your code to a branch. Then, run `databricks_test.go` integration tests on your devbox against your new branch by changing the branch name manually. Include the test results in your PR. 


# Saved SQL exports

Pass `--sync_type full --query 'SELECT id AS lookup_id, name AS label FROM catalog.schema.source'`
with the usual GCS arguments to export a SQL result. The query overrides the configured
catalog/schema/table and generated filters. Put any required filters in the SQL itself.
Use fully qualified table references; result column names and aliases are preserved.
`--query` cannot be combined with incremental sync modes or `--use_custom_sql`.

With `--query`, `--validate_row_count 5000000` counts the query result before exporting,
including allowing zero rows and exactly five million rows. Without `--query`, existing
table-based exports and row-count validation are unchanged. Counting and exporting are
separate SQL executions, so the source should remain stable between them.

Local tests (`uv run pytest`) mock Spark and GCS. Before enabling the flag in DWE, validate
this script on Databricks and make it available to the configured export job. The package
change alone does not enable saved SQL in the DWE caller.
