# Iceberg: rely on Glue job --datalake-formats iceberg (do not spark.conf.set spark.sql.extensions after Spark starts).
# If this script is old in S3, run: terraform apply in env/dev/aws (uploads this file to the glue script bucket).
import base64
import os
import gzip
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import (
    col,
    dayofmonth,
    explode,
    from_unixtime,
    get_json_object,
    hour,
    length,
    month,
    regexp_extract,
    to_timestamp,
    when,
    year,
)


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
    """Use glue_catalog for the session so writeTo('db.table') hits Glue Iceberg, not spark_catalog."""
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
# Same path as job --conf spark.sql.catalog.glue_catalog.warehouse (kept for Glue getResolvedOptions / schedulers).
_iceberg_warehouse = args["iceberg_warehouse"].rstrip("/")
glue_database = args["glue_database"]
iceberg_table_name = args["iceberg_table_name"]

# glue_catalog plugin class + impl must be set (job --conf from Terraform). STS only for Glue API preflight.
_boto_session = boto3.session.Session()
_glue_region = _boto_session.region_name or os.environ.get("AWS_REGION")
if not _glue_region:
    raise RuntimeError("AWS region not set (expected Glue AWS_REGION or boto3 default region)")
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

# Iceberg: --datalake-formats iceberg + --conf (glue_catalog SparkCatalog + GlueCatalog; see locals.tf) at JVM startup.
# Do not spark.conf.set spark.sql.extensions here — it is static after Spark starts (AnalysisException).

raw_schema = StructType(
    [
        StructField("messageType", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("logGroup", StringType(), True),
        StructField("logStream", StringType(), True),
        StructField("subscriptionFilters", ArrayType(StringType()), True),
        StructField(
            "logEvents",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("timestamp", LongType(), True),
                        StructField("message", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


def resolve_input_prefix(base_path: str, date_value: Optional[str]) -> str:
    if not date_value or date_value.upper() == "AUTO":
        jst = timezone(timedelta(hours=9))
        date_value = datetime.now(jst).strftime("%Y/%m/%d")
    else:
        date_value = date_value.strip().replace("-", "/")

    return f"{base_path.rstrip('/')}/{date_value}/"


input_prefix = resolve_input_prefix(input_path, target_date)

binary_df = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(input_prefix)
)


def _decompress_gzip_layers(data: bytes) -> bytes:
    while data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def decode_payload(payload: bytes) -> Optional[str]:
    data = _decompress_gzip_layers(payload)

    if data[:2] == b"H4" or data.startswith(b"ey"):
        try:
            decoded = base64.b64decode(data, validate=True)
            data = _decompress_gzip_layers(decoded)
        except Exception:
            pass

    data = _decompress_gzip_layers(data)

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


json_rdd = binary_df.select("content").rdd.map(lambda row: decode_payload(row.content)).filter(
    lambda line: line
)

raw_df = spark.read.schema(raw_schema).json(json_rdd)

exploded = raw_df.withColumn("log_event", explode(col("logEvents")))

base_df = (
    exploded.select(
        col("messageType").alias("message_type"),
        col("owner").alias("owner"),
        col("logGroup").alias("log_group"),
        col("logStream").alias("log_stream"),
        col("subscriptionFilters").alias("subscription_filters"),
        col("log_event.id").alias("event_id"),
        col("log_event.timestamp").alias("event_timestamp"),
        col("log_event.message").alias("event_message"),
    )
    .withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_timestamp") / 1000)),
    )
)

with_request_id = base_df.withColumn(
    "request_id",
    when(
        length(regexp_extract(col("event_message"), r"RequestId: ([a-f0-9\-]+)", 1))
        > 0,
        regexp_extract(col("event_message"), r"RequestId: ([a-f0-9\-]+)", 1),
    ),
)

with_report_metrics = (
    with_request_id.withColumn(
        "duration_ms",
        when(
            length(regexp_extract(col("event_message"), r"Duration: ([0-9.]+) ms", 1))
            > 0,
            regexp_extract(col("event_message"), r"Duration: ([0-9.]+) ms", 1).cast("double"),
        ),
    )
    .withColumn(
        "billed_duration_ms",
        when(
            length(regexp_extract(col("event_message"), r"Billed Duration: ([0-9.]+) ms", 1))
            > 0,
            regexp_extract(col("event_message"), r"Billed Duration: ([0-9.]+) ms", 1).cast("double"),
        ),
    )
    .withColumn(
        "memory_size_mb",
        when(
            length(regexp_extract(col("event_message"), r"Memory Size: ([0-9]+) MB", 1))
            > 0,
            regexp_extract(col("event_message"), r"Memory Size: ([0-9]+) MB", 1).cast("int"),
        ),
    )
    .withColumn(
        "max_memory_used_mb",
        when(
            length(regexp_extract(col("event_message"), r"Max Memory Used: ([0-9]+) MB", 1))
            > 0,
            regexp_extract(col("event_message"), r"Max Memory Used: ([0-9]+) MB", 1).cast("int"),
        ),
    )
    .withColumn(
        "init_duration_ms",
        when(
            length(regexp_extract(col("event_message"), r"Init Duration: ([0-9.]+) ms", 1))
            > 0,
            regexp_extract(col("event_message"), r"Init Duration: ([0-9.]+) ms", 1).cast("double"),
        ),
    )
)

with_app_json = (
    with_report_metrics.withColumn("app_ts", get_json_object(col("event_message"), "$.ts"))
    .withColumn("source", get_json_object(col("event_message"), "$.source"))
    .withColumn("path", get_json_object(col("event_message"), "$.path"))
    .withColumn("method", get_json_object(col("event_message"), "$.method"))
    .withColumn("body", get_json_object(col("event_message"), "$.body"))
    .withColumn(
        "status",
        get_json_object(col("event_message"), "$.status").cast("int"),
    )
)

final_df = (
    with_app_json.withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
    .withColumn("hour", hour(col("event_time")))
)

# Iceberg table lives in catalog "glue_catalog" (--datalake-formats iceberg registers it).
# writeTo("glue_catalog.db.tbl") is NOT parsed as catalog+db+table under default spark_catalog.
# glue_catalog must be registered (spark.sql.catalog.glue_catalog=...) then setCurrentCatalog + two-part db.table.
_ensure_glue_iceberg_catalog(spark, _iceberg_warehouse, _glue_account, _glue_region)
_set_current_catalog_glue(spark)
full_table = f"{glue_database}.{iceberg_table_name}"
try:
    (
        final_df.writeTo(full_table)
        .using("iceberg")
        .partitionedBy("year", "month", "day", "hour")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
except Py4JJavaError as exc:
    # Driver message is often generic ("Writing job aborted"); executor logs show S3 AccessDenied, Glue, etc.
    parts = [str(exc)]
    j = exc.java_exception
    depth = 0
    while j is not None and depth < 8:
        msg = j.getMessage()
        if msg:
            parts.append(msg)
        j = j.getCause()
        depth += 1
    raise RuntimeError("Iceberg write failed: " + " | ".join(parts)) from exc

job.commit()
