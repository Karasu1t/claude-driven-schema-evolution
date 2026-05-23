import pytest
import polars as pl
import sqlite3
from pathlib import Path


class TestCSVLoading:
    """Test CSV reading with Polars"""
    
    def test_csv_load_old_schema(self):
        """Load CSV with old schema"""
        df = pl.read_csv('specs/sample_columns.csv')
        assert len(df) == 3
        assert list(df.columns) == ['id', 'name', 'email', 'created_at', 'status']
    
    def test_csv_has_required_columns(self):
        """Verify required columns exist in CSV"""
        df = pl.read_csv('specs/sample_columns.csv')
        required = ['id', 'name', 'email', 'created_at', 'status']
        for col in required:
            assert col in df.columns, f"Missing column: {col}"
    
    def test_csv_data_types(self):
        """Verify CSV data types"""
        df = pl.read_csv('specs/sample_columns.csv')
        # Polars automatically infers types
        assert df['id'].dtype == pl.Int64  # Polars infers numeric types
        assert df['name'].dtype == pl.String
        assert df['email'].dtype == pl.String


class TestSchemaEvolution:
    """Test schema evolution logic"""
    
    def test_add_missing_column(self):
        """Add missing column to DataFrame"""
        df = pl.read_csv('specs/sample_columns.csv')
        assert 'verified_at' not in df.columns
        
        # Add new column
        df = df.with_columns(pl.lit(None).cast(pl.String).alias('verified_at'))
        assert 'verified_at' in df.columns
        assert df['verified_at'].null_count() == len(df)
    
    def test_column_order_preserved(self):
        """Ensure column order matches new schema"""
        df = pl.read_csv('specs/sample_columns.csv')
        new_columns = ['id', 'name', 'email', 'created_at', 'status', 'verified_at']
        
        for col in new_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        df = df.select(new_columns)
        assert list(df.columns) == new_columns
    
    def test_data_not_lost_during_evolution(self):
        """Verify data is preserved during schema evolution"""
        df_old = pl.read_csv('specs/sample_columns.csv')
        original_rows = len(df_old)
        
        # Add new column
        df_new = df_old.with_columns(pl.lit(None).alias('verified_at'))
        
        # Row count should remain same
        assert len(df_new) == original_rows
        # Original data should be intact
        assert df_new['name'].to_list() == df_old['name'].to_list()


class TestParquetOutput:
    """Test Parquet file generation"""
    
    def test_write_parquet(self, tmp_path):
        """Write CSV to Parquet"""
        df = pl.read_csv('specs/sample_columns.csv')
        
        output_file = tmp_path / 'test.parquet'
        df.write_parquet(str(output_file))
        
        assert output_file.exists()
        assert output_file.stat().st_size > 0
    
    def test_read_parquet_back(self, tmp_path):
        """Write and read Parquet file"""
        df_original = pl.read_csv('specs/sample_columns.csv')
        
        output_file = tmp_path / 'test.parquet'
        df_original.write_parquet(str(output_file))
        
        df_read = pl.read_parquet(str(output_file))
        
        # Verify data integrity
        assert len(df_read) == len(df_original)
        assert list(df_read.columns) == list(df_original.columns)


class TestSQLiteMetadata:
    """Test SQLite metadata storage"""
    
    def test_create_schema_table(self):
        """Create schemas table in SQLite"""
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schemas (
                name TEXT PRIMARY KEY,
                columns TEXT,
                row_count INTEGER
            )
        ''')
        
        cursor.execute(
            "INSERT INTO schemas VALUES (?, ?, ?)",
            ('test', 'id,name', 5)
        )
        
        cursor.execute("SELECT * FROM schemas WHERE name='test'")
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == 'test'
        assert result[1] == 'id,name'
        assert result[2] == 5
        conn.close()
    
    def test_replace_schema_metadata(self):
        """Replace schema metadata with new version"""
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE schemas (
                name TEXT PRIMARY KEY,
                columns TEXT,
                row_count INTEGER
            )
        ''')
        
        # Insert version 1
        cursor.execute(
            "INSERT OR REPLACE INTO schemas VALUES (?, ?, ?)",
            ('sample', 'id,name', 5)
        )
        conn.commit()
        
        # Insert version 2 (should replace)
        cursor.execute(
            "INSERT OR REPLACE INTO schemas VALUES (?, ?, ?)",
            ('sample', 'id,name,email', 7)
        )
        conn.commit()
        
        cursor.execute("SELECT * FROM schemas WHERE name='sample'")
        result = cursor.fetchone()
        
        assert result[1] == 'id,name,email'
        assert result[2] == 7
        conn.close()


class TestFullPipeline:
    """Integration tests for the full pipeline"""
    
    def test_csv_to_parquet_pipeline(self, tmp_path):
        """Test complete CSV → Parquet pipeline"""
        # Read CSV
        df = pl.read_csv('specs/sample_columns.csv')
        
        # Add new column
        df = df.with_columns(pl.lit(None).alias('verified_at'))
        df = df.select(['id', 'name', 'email', 'created_at', 'status', 'verified_at'])
        
        # Write Parquet
        output_file = tmp_path / 'output.parquet'
        df.write_parquet(str(output_file))
        
        # Read back and verify
        df_verify = pl.read_parquet(str(output_file))
        assert len(df_verify) == len(df)
        assert list(df_verify.columns) == ['id', 'name', 'email', 'created_at', 'status', 'verified_at']
