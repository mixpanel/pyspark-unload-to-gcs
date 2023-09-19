import argparse
import pyspark
import pyspark.sql.functions as f


def export_to_gcs(args):
    """
    This function takes in GCS credentials and unloads the results of a 
    Query to GCS using spark. 
    """
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("fs.gs.project.id", args.gcp_project)
    spark.conf.set("fs.gs.auth.service.account.email", args.service_account_email)
    spark.conf.set("fs.gs.auth.service.account.private.key", args.service_account_key)
    spark.conf.set("fs.gs.auth.service.account.private.key.id", args.service_account_key_id)

    df = spark.sql(args.sql)
    df.write.format(args.export_format).option("compression", "gzip").option("header", "true").mode("overwrite").save(f"gs://{args.bucket}//{args.prefix}/")
    

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
    
    args = parser.parse_args()
    export_to_gcs(args)
