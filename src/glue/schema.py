from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Single source of truth for source columns.
# All files (glue_job.py, tests) import from here.
# /update-schema modifies only this file when adding or removing columns.
SOURCE_SCHEMA = StructType([
    StructField("video_title", StringType(), True),
    StructField("views", LongType(), True),
    StructField("channel_name", StringType(), True),
    StructField("channel_subscribers", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("video_duration_minutes", DoubleType(), True),
])

SOURCE_COLUMNS = [f.name for f in SOURCE_SCHEMA.fields]
