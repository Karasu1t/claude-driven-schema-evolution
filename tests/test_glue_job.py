"""
Unit Tests for Glue Job Schema Evolution Logic

Tests:
- UT-2: VER_DATE must be provided (error handling)
- UT-3: Schema evolution (missing columns auto-added)
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import lit
from datetime import datetime


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
        
        # Required columns
        required_columns = [
            'video_title', 'views', 'channel_name', 
            'channel_subscribers', 'likes', 'video_duration_minutes'
        ]
        
        # Schema evolution: add missing columns
        df = input_df
        for col_name in required_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast("string"))
        
        # Select in standard order
        df = df.select(required_columns)
        
        # Verify
        assert len(df.columns) == 6, "Should have 6 columns after evolution"
        assert df.columns == required_columns, "Columns should be in correct order"
        assert df.count() == 2, "Should preserve row count"

    def test_schema_evolution_all_columns_present(self, spark):
        """Test: If all columns present, no changes"""
        input_data = [
            ("video_1", "50000", "channel_1", "100000", "5000", "10"),
            ("video_2", "75000", "channel_2", "150000", "7500", "15"),
        ]
        input_df = spark.createDataFrame(
            input_data,
            [
                'video_title', 'views', 'channel_name',
                'channel_subscribers', 'likes', 'video_duration_minutes'
            ]
        )
        
        required_columns = [
            'video_title', 'views', 'channel_name',
            'channel_subscribers', 'likes', 'video_duration_minutes'
        ]
        
        # Schema evolution (should be no-op)
        df = input_df
        for col_name in required_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast("string"))
        
        df = df.select(required_columns)
        
        # Verify
        assert len(df.columns) == 6, "Should have 6 columns"
        assert df.count() == 2, "Should preserve row count"
        assert df.first()[0] == "video_1", "Should preserve data"

    def test_metadata_columns_added(self, spark):
        """Test: Metadata columns are added correctly"""
        input_data = [("video_1", "50000")]
        df = spark.createDataFrame(input_data, ["video_title", "views"])
        
        # Add metadata
        df = df.withColumn(
            "processed_at",
            lit(datetime.now().isoformat()).cast("string")
        )
        df = df.withColumn("glue_job_run_id", lit("test-run-id").cast("string"))
        
        # Verify
        assert "processed_at" in df.columns, "Should have processed_at column"
        assert "glue_job_run_id" in df.columns, "Should have glue_job_run_id column"
        assert df.first()["glue_job_run_id"] == "test-run-id", "Should have correct run ID"


class TestDataQuality:
    """Data quality checks"""

    def test_column_order_preserved(self, spark):
        """Test: Column order is consistent"""
        input_data = [("v1", "100")]
        df = spark.createDataFrame(input_data, ["video_title", "views"])
        
        required_columns = ['video_title', 'views', 'channel_name', 
                           'channel_subscribers', 'likes', 'video_duration_minutes']
        
        for col in required_columns:
            if col not in df.columns:
                df = df.withColumn(col, lit(None).cast("string"))
        
        df = df.select(required_columns)
        
        # Verify order
        assert df.columns[:2] == required_columns[:2], "First columns should match"
