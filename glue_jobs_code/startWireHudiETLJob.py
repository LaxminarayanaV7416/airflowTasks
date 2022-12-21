import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'start_date'])

spark = (SparkSession 
    .builder 
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
    .config("spark.sql.hive.convertMetastoreParquet", "false") 
    .config("spark.sql.parquet.writeLegacyFormat", "true") 
    .config("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

start_date = args['start_date']

PATH = "s3://startwire-jobs-data/start_wire_active_jobs_daily/"
PATH = f"{PATH}{start_date}/"

print(PATH)

df = spark.read.json(PATH)

df = df.select([F.col("_source.company").alias("company"),
            F.col("_source.cpa").alias("cpa"),
            F.col("_source.ctr_rate").alias("ctr_rate"),
            F.col("_source.description").alias("description"),
            F.col("_source.express_apply").alias("express_apply"),
            F.col("_source.feed_id").alias("feed_id"),
            F.col("_source.id").alias("id"),
            F.col("_source.lat_lon").alias("lat_lon"),
            F.col("_source.original_job_reference").alias("original_job_reference"),
            F.col("_source.position").alias("position"),
            F.col("_source.posted_at").alias("posted_at"),
            F.col("_source.price").alias("price"),
            F.col("_source.priority").alias("priority"),
            F.col("_source.properties").alias("properties"),
            F.col("_source.slug").alias("slug"),
            F.col("_source.slug_idx").alias("slug_idx"),
            F.col("_source.statistic").alias("statistic"),
            F.col("_source.updated_at").alias("updated_at"),
            F.col("_source.url").alias("url")])


tableName = "active_jobs_daily_upserts"

hudi_config = {
    "hoodie.table.name": tableName,
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "id",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.datasource.meta.sync.enable" : "true",
    "hoodie.datasource.hive_sync.assume_date_partitioning": "false",
    "hoodie.datasource.hive_sync.database" : "startwire_active_jobs",
    "hoodie.datasource.hive_sync.table": "startwire_active_jobs",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.bulkinsert.shuffle.parallelism": 4,
    "hoodie.upsert.shuffle.parallelism": 4,
    "hoodie.insert.shuffle.parallelism": 4,
    "hoodie.datasource.write.row.writer.enable": "true",
    "hoodie.metadata.index.column.stats.enable": "true",
    "index_type":"BLOOM",
    "hoodie.bloom_index_use_metadata":"true",
    "hoodie.bloom.index.prune.by.ranges":"true",
    "hoodie.bloom.index.use.caching":"true",
    "hoodie.bloom.index.filter.type":"DYNAMIC_V0"
}

hudi_config["path"] = "s3://startwire-jobs-data/start_wire_active_jobs_daily_hudi/"

df.write.format("org.apache.hudi").options(
    **hudi_config).mode("append").save()
    