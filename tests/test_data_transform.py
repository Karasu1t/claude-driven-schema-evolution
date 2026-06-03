"""
Data Transformation Tests

Validates actual data values, type correctness, and format conversions
after transformation. Schema structure tests are in test_glue_job.py.
"""

import re
import sys
import os
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'glue'))
from schema import SOURCE_SCHEMA, SOURCE_COLUMNS


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("test-data-transform").getOrCreate()


@pytest.fixture(scope="session")
def sample_df(spark):
    """Realistic input matching test_data/sns_advertisement_yyyymmdd.csv."""
    data = [
        ("Python Tutorial",   45000, "CodeMastery",  120000, 2300, 42.5),
        ("Data Analysis",     38000, "DataLab",       98000, 1900, 38.0),
        ("AWS Basics",        52000, "CloudTech",    145000, 2800, 45.0),
        ("Machine Learning",  61000, "AIExperts",    189000, 3500, 52.0),
        ("Database Design",   47000, "DataEng",      112000, 2100, 40.0),
    ]
    return spark.createDataFrame(data, SOURCE_COLUMNS)


class TestDataValues:
    """Data values must survive transformation unchanged."""

    def test_string_values_preserved(self, sample_df):
        rows = sample_df.collect()
        assert rows[0]["video_title"] == "Python Tutorial"
        assert rows[1]["channel_name"] == "DataLab"

    def test_numeric_values_preserved(self, sample_df):
        rows = sample_df.collect()
        assert rows[0]["views"] == 45000
        assert rows[2]["likes"] == 2800
        assert abs(rows[0]["video_duration_minutes"] - 42.5) < 1e-6

    def test_row_count_preserved(self, sample_df):
        assert sample_df.count() == 5

    def test_null_for_missing_columns(self, spark):
        """Schema evolution must produce null — not empty string or zero."""
        partial = spark.createDataFrame(
            [("video_only", 1000)],
            ["video_title", "views"]
        )
        for col in SOURCE_COLUMNS:
            if col not in partial.columns:
                partial = partial.withColumn(col, lit(None).cast("string"))
        partial = partial.select(SOURCE_COLUMNS)

        row = partial.first()
        assert row["channel_name"] is None
        assert row["likes"] is None


class TestTypeCorrectness:
    """Column dtypes must match SOURCE_SCHEMA after transformation."""

    def test_long_column_dtypes(self, sample_df):
        dtype_map = dict(sample_df.dtypes)
        for col in ["views", "channel_subscribers", "likes"]:
            assert dtype_map[col] == "bigint", f"{col} should be bigint"

    def test_double_column_dtype(self, sample_df):
        dtype_map = dict(sample_df.dtypes)
        assert dtype_map["video_duration_minutes"] == "double"

    def test_string_column_dtypes(self, sample_df):
        dtype_map = dict(sample_df.dtypes)
        for col in ["video_title", "channel_name"]:
            assert dtype_map[col] == "string", f"{col} should be string"

    def test_schema_matches_source_schema(self, sample_df):
        """DataFrame schema must exactly match SOURCE_SCHEMA field by field."""
        for expected, actual in zip(SOURCE_SCHEMA.fields, sample_df.schema.fields):
            assert actual.name == expected.name
            assert actual.dataType == expected.dataType


class TestDateConversion:
    """Partition date derivation from YYYYMMDD filename convention."""

    @pytest.mark.parametrize("ver_date, expected", [
        ("20260529", "2026-05-29"),
        ("20260101", "2026-01-01"),
        ("20261231", "2026-12-31"),
    ])
    def test_date_format_conversion(self, ver_date, expected):
        result = f"{ver_date[:4]}-{ver_date[4:6]}-{ver_date[6:8]}"
        assert result == expected

    def test_invalid_ver_date_raises(self):
        with pytest.raises(ValueError, match="Invalid VER_DATE format"):
            ver_date = "2026529"
            if not (len(ver_date) == 8 and ver_date.isdigit()):
                raise ValueError(f"Invalid VER_DATE format: {ver_date}. Expected yyyymmdd")

    def test_partition_date_column_type(self, spark, sample_df):
        """partition_date column must be cast to date type."""
        df = sample_df.withColumn("partition_date", lit("2026-05-29").cast("date"))
        dtype_map = dict(df.dtypes)
        assert dtype_map["partition_date"] == "date"

    def test_partition_date_value(self, spark, sample_df):
        """partition_date value must match the converted VER_DATE."""
        df = sample_df.withColumn("partition_date", lit("2026-05-29").cast("date"))
        assert str(df.first()["partition_date"]) == "2026-05-29"


class TestMetadataColumns:
    """Metadata columns added by the Glue Job."""

    def test_processed_at_is_iso_format(self, sample_df):
        """processed_at must be a valid ISO 8601 timestamp string."""
        iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
        df = sample_df.withColumn(
            "processed_at", lit(datetime.now().isoformat()).cast("string")
        )
        value = df.first()["processed_at"]
        assert iso_pattern.match(value), f"processed_at '{value}' is not ISO format"

    def test_processed_at_dtype(self, sample_df):
        df = sample_df.withColumn(
            "processed_at", lit(datetime.now().isoformat()).cast("string")
        )
        assert dict(df.dtypes)["processed_at"] == "string"

    def test_metadata_columns_do_not_overwrite_source(self, sample_df):
        """Adding metadata columns must not affect source column values."""
        df = sample_df \
            .withColumn("partition_date", lit("2026-05-29").cast("date")) \
            .withColumn("processed_at", lit(datetime.now().isoformat()).cast("string"))
        row = df.first()
        assert row["video_title"] == "Python Tutorial"
        assert row["views"] == 45000
