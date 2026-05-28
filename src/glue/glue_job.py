# Iceberg with --datalake-formats iceberg (native Glue 4.0 support)
# CSV → Transform → Iceberg table
import sys
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, year, month, dayofmonth, hour, to_timestamp
from datetime import datetime, timedelta, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _ensure_glue_iceberg_catalog(spark_session, warehouse: str, account: str, region: str) -> None:
    """Register glue_catalog if missing (normally set at JVM via job --conf; fallback when script updated before apply)."""
    if spark_session.conf.get("spark.sql.catalog.glue_catalog"):
        return
    spark_session.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark_session.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark_session.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark_session.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse)
    spark_session.conf.set("spark.sql.catalog.glue_catalog.glue.id", account)
    spark_session.conf.set("spark.sql.catalog.glue_catalog.glue.region", region)


def _set_current_catalog_glue(spark_session):
    """Use glue_catalog for the session."""
    setter = getattr(spark_session.catalog, "setCurrentCatalog", None)
    if callable(setter):
        setter("glue_catalog")
    else:
        spark_session._jsparkSession.sessionState().catalogManager().setCurrentCatalog("glue_catalog")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path",
        "TempDir",
        "iceberg_warehouse",
        "glue_database",
        "iceberg_table_name",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

input_path = args["input_path"]
_iceberg_warehouse = args["iceberg_warehouse"].rstrip("/")
glue_database = args["glue_database"]
iceberg_table_name = args["iceberg_table_name"]

# Get AWS account and region
_boto_session = boto3.session.Session()
_glue_region = _boto_session.region_name or os.environ.get("AWS_REGION") or "ap-northeast-1"
_glue_account = boto3.client("sts", region_name=_glue_region).get_caller_identity()["Account"]

_glue_api = boto3.client("glue", region_name=_glue_region)
try:
    _glue_api.get_database(Name=glue_database)
except ClientError as exc:
    code = exc.response.get("Error", {}).get("Code", "")
    if code == "EntityNotFoundException":
        raise RuntimeError(
            f"Glue database {glue_database!r} does not exist in account {_glue_account} region {_glue_region}. "
            "Apply Terraform (aws_glue_catalog_database) or fix --glue_database."
        ) from exc
    raise

target_date = None
if "--target_date" in sys.argv:
    target_date = getResolvedOptions(sys.argv, ["target_date"]).get("target_date")


def resolve_input_prefix(base_path: str, date_value: Optional[str]) -> str:
    """Resolve input path with date or auto-detection."""
    if not date_value or date_value.upper() == "AUTO":
        jst = timezone(timedelta(hours=9))
        date_value = datetime.now(jst).strftime("%Y/%m/%d")
    else:
        # Handle wildcard patterns
        if "*" in base_path:
            return base_path
        date_value = date_value.strip().replace("-", "/")

    return f"{base_path.rstrip('/')}/{date_value}/"


# For development: if input_path ends with .csv, use it directly (wildcard pattern)
if input_path.endswith(".csv") or "*.csv" in input_path:
    input_prefix = input_path
else:
    input_prefix = resolve_input_prefix(input_path, target_date)

logger.info(f"Reading from: {input_prefix}")

# Read CSV with schema inference
try:
    df = spark.read.csv(
        input_prefix,
        header=True,
        inferSchema=True,
        mode="PERMISSIVE"
    )
except Exception as e:
    logger.warning(f"CSV read failed for {input_prefix}: {e}")
    logger.info("Attempting to create empty dataframe...")
    df = spark.createDataFrame([], "")

if df.count() == 0:
    logger.warning(f"No data found in {input_prefix}")
    # Create sample schema for Iceberg table
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("video_title", StringType(), True),
        StructField("views", StringType(), True),
        StructField("channel_name", StringType(), True),
    ])
    df = spark.createDataFrame([], schema)

logger.info(f"Input schema:\n{df.printSchema()}")

# Add metadata columns
df_with_metadata = (
    df.withColumn("processed_at", lit(datetime.now().isoformat()))
    .withColumn("glue_job_run_id", lit(args["JOB_NAME"]))
)

# Add partitioning columns (for Iceberg partitioning)
current_time = to_timestamp(lit(datetime.now().isoformat()))
df_partitioned = (
    df_with_metadata
    .withColumn("_event_year", year(current_time))
    .withColumn("_event_month", month(current_time))
    .withColumn("_event_day", dayofmonth(current_time))
    .withColumn("_event_hour", hour(current_time))
)

logger.info(f"Final schema:\n{df_partitioned.printSchema()}")

# Write to Iceberg
_ensure_glue_iceberg_catalog(spark, _iceberg_warehouse, _glue_account, _glue_region)
_set_current_catalog_glue(spark)

full_table = f"{glue_database}.{iceberg_table_name}"
logger.info(f"Writing to Iceberg table: {full_table}")

try:
    (
        df_partitioned.writeTo(full_table)
        .using("iceberg")
        .partitionedBy("_event_year", "_event_month", "_event_day", "_event_hour")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    logger.info(f"Successfully wrote {df.count()} records to Iceberg table {full_table}")
except Py4JJavaError as exc:
    parts = [str(exc)]
    j = exc.java_exception
    depth = 0
    while j is not None and depth < 8:
        msg = j.getMessage()
        if msg:
            parts.append(msg)
        j = j.getCause()
        depth += 1
    logger.error("Iceberg write failed: " + " | ".join(parts))
    raise RuntimeError("Iceberg write failed: " + " | ".join(parts)) from exc

job.commit()
