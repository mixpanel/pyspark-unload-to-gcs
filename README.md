# Unload to GCS using Pyspark

Pyspark script to unload from spark to GCS. 

Use this script as a job in databricks with the task type SparkPython.
Pass a SQL query to unload, gcs paths, and credentials in as arguments.

# Development

When creating the PR, push your code to a branch. Then, run `databricks_test.go` integration tests on your devbox against your new branch by changing the branch name manually. Include the test results in your PR. 

