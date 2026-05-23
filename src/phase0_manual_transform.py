import polars as pl
import sqlite3
import os
from pathlib import Path


def main():
    """Phase 0: Manual ETL transformation
    
    Workflow:
    1. Load CSV with old schema
    2. Validate and apply new schema
    3. Transform data
    4. Write to Parquet
    5. Save metadata to SQLite
    """
    
    # 作業ディレクトリを確認・修正
    script_dir = Path(__file__).parent.parent  # src/phase0_manual_transform.py から ../../ へ
    os.chdir(script_dir)
    print(f"📁 Working directory: {os.getcwd()}")
    
    # 1. CSV 読み込み（古いスキーマ）
    csv_path = Path('specs/sample_columns.csv')
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    df = pl.read_csv(str(csv_path))
    print(f"✅ Old schema loaded: {df.columns}")
    print(f"   Rows: {len(df)}")
    print(f"\n{df}")
    
    # 2. Schema validation - add missing columns from new schema
    new_columns = ['id', 'name', 'email', 'created_at', 'status', 'verified_at']
    for col in new_columns:
        if col not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(pl.String).alias(col)
            )
    
    # Ensure column order matches new schema
    df = df.select(new_columns)
    print(f"\n✅ New schema applied: {df.columns}")
    print(f"   Rows: {len(df)}\n")
    
    # 3. Data validation
    assert len(df) > 0, "DataFrame is empty"
    assert 'id' in df.columns, "Missing 'id' column"
    assert 'verified_at' in df.columns, "Missing 'verified_at' column"
    print("✅ Schema validation passed")
    
    # 4. Parquet output
    output_dir = Path('data')
    output_dir.mkdir(exist_ok=True, parents=True)
    
    parquet_path = output_dir / 'sample.parquet'
    df.write_parquet(str(parquet_path))
    print(f"✅ Parquet written: {parquet_path}")
    
    # 5. Schema metadata to SQLite
    sqlite_path = output_dir / 'schema_metadata.sqlite'
    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schemas (
            name TEXT PRIMARY KEY,
            columns TEXT,
            row_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    columns_str = ','.join(df.columns)
    cursor.execute(
        "INSERT OR REPLACE INTO schemas (name, columns, row_count) VALUES (?, ?, ?)",
        ('sample', columns_str, len(df))
    )
    conn.commit()
    cursor.execute("SELECT * FROM schemas WHERE name='sample'")
    row = cursor.fetchone()
    print(f"✅ Schema metadata saved:")
    print(f"   Name: {row[0]}, Columns: {row[1]}, Rows: {row[2]}")
    conn.close()
    
    print("\n✅ Phase 0 transformation completed successfully!")


if __name__ == '__main__':
    main()
