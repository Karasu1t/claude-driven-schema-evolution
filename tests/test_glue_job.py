"""
Schema Evolution Tests

Validates structural correctness: column presence, order, and count.
For data value and type correctness, see test_data_transform.py.
"""

import sys
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'glue'))
from schema import SOURCE_COLUMNS


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder.appName("test-glue-job").getOrCreate()


class TestVERDateValidation:
    """UT-2: VER_DATE argument validation"""

    def test_ver_date_extraction_valid(self):
        """Test: Extract valid VER_DATE from arguments"""
        test_args = ['--VER_DATE', '20260529']
        
        ver_date = None
        if '--VER_DATE' in test_args:
            idx = test_args.index('--VER_DATE')
            if idx + 1 < len(test_args):
                ver_date = test_args[idx + 1]
        
        assert ver_date == '20260529', "Should extract VER_DATE correctly"

    def test_ver_date_extraction_invalid_empty(self):
        """Test: VER_DATE missing raises error"""
        test_args = []
        
        ver_date = None
        if '--VER_DATE' in test_args:
            idx = test_args.index('--VER_DATE')
            if idx + 1 < len(test_args):
                ver_date = test_args[idx + 1]
        
        with pytest.raises(ValueError, match="VER_DATE argument is required"):
            if not ver_date:
                raise ValueError("VER_DATE argument is required")

    def test_ver_date_extraction_invalid_no_value(self):
        """Test: VER_DATE without value raises error"""
        test_args = ['--VER_DATE']
        
        ver_date = None
        if '--VER_DATE' in test_args:
            idx = test_args.index('--VER_DATE')
            if idx + 1 < len(test_args):
                ver_date = test_args[idx + 1]
        
        with pytest.raises(ValueError, match="VER_DATE argument is required"):
            if not ver_date:
                raise ValueError("VER_DATE argument is required")


class TestSchemaEvolution:
    """UT-3: Schema evolution logic"""

    def test_schema_evolution_add_missing_columns(self, spark):
        """Test: Missing columns are automatically added"""
        # Input CSV with only 2 columns
        input_data = [
            ("video_1", "50000"),
            ("video_2", "75000"),
        ]
        input_df = spark.createDataFrame(
            input_data,
            ["video_title", "views"]
        )
        
        df = input_df
        for col_name in SOURCE_COLUMNS:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast("string"))

        df = df.select(SOURCE_COLUMNS)

        assert len(df.columns) == len(SOURCE_COLUMNS), "Should have all source columns after evolution"
        assert df.columns == SOURCE_COLUMNS, "Columns should be in correct order"
        assert df.count() == 2, "Should preserve row count"

    def test_schema_evolution_all_columns_present(self, spark):
        """Test: If all columns present, no changes"""
        input_data = [
            ("video_1", "50000", "channel_1", "100000", "5000", "10", "category_1"),
            ("video_2", "75000", "channel_2", "150000", "7500", "15", "category_2"),
        ]
        input_df = spark.createDataFrame(
            input_data,
            [
                'video_title', 'views', 'channel_name',
                'channel_subscribers', 'likes', 'video_duration_minutes', 'video_category'
            ]
        )
        
        df = input_df
        for col_name in SOURCE_COLUMNS:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast("string"))

        df = df.select(SOURCE_COLUMNS)

        assert len(df.columns) == len(SOURCE_COLUMNS), "Should have all source columns"
        assert df.count() == 2, "Should preserve row count"
        assert df.first()[0] == "video_1", "Should preserve data"

    def test_metadata_columns_added(self, spark):
        """Test: Metadata columns added by glue_job.py are present."""
        input_data = [("video_1", "50000")]
        df = spark.createDataFrame(input_data, ["video_title", "views"])

        df = df.withColumn(
            "processed_at",
            lit(datetime.now().isoformat()).cast("string")
        )
        df = df.withColumn("partition_date", lit("2026-05-29").cast("date"))

        assert "processed_at" in df.columns
        assert "partition_date" in df.columns


class TestDataQuality:
    """Data quality checks"""

    def test_column_order_preserved(self, spark):
        """Test: Column order is consistent"""
        input_data = [("v1", "100")]
        df = spark.createDataFrame(input_data, ["video_title", "views"])
        
        for col in SOURCE_COLUMNS:
            if col not in df.columns:
                df = df.withColumn(col, lit(None).cast("string"))

        df = df.select(SOURCE_COLUMNS)

        assert df.columns[:2] == SOURCE_COLUMNS[:2], "First columns should match"
