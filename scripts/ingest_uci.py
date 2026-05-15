"""
ingest_uci.py — Bronze layer ingestion for UCI Pima Indians Diabetes dataset.

Reads the raw CSV, applies minimal preprocessing to correctly represent missing
values (UCI encodes missing as 0 for clinical columns), generates a synthetic
patient_id, and writes to the Bronze Parquet landing zone.

Only missing-value encoding correction is applied at Bronze — no renaming,
no unit conversion, no schema changes.
"""

import sys
import os

# Ensure project root is on the path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from scripts.utils import load_config, get_logger, log_run

logger = get_logger("ingest_uci")


def main():
    config = load_config()

    try:
        # Step 1: Read the CSV
        logger.info("Reading UCI Pima Indians Diabetes CSV...")
        df = pd.read_csv(config["paths"]["raw_uci"], na_values=["?"])
        logger.info(f"  Raw CSV: {len(df)} rows, {len(df.columns)} columns")

        # Step 2: Replace impossible zeros with NaN for specified columns
        zero_cols = config["uci_impossible_zero_columns"]
        for col in zero_cols:
            if col in df.columns:
                count_zeros = (df[col] == 0).sum()
                df[col] = df[col].replace(0, pd.NA)
                logger.info(f"  Replaced {count_zeros} zeros with NaN in '{col}'")
            else:
                logger.warning(f"  Column '{col}' not found in UCI CSV — skipping")

        # Step 3: Generate patient_id
        df["patient_id"] = "uci_" + pd.Series(range(len(df))).astype(str).str.zfill(6)

        # Step 4: Add ingestion timestamp
        df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

        # Step 5: Compute null rate
        null_rate_pct = df.isnull().mean().mean() * 100

        # Step 6: Write to Bronze Parquet
        bronze_path = config["paths"]["bronze_uci"]
        os.makedirs(bronze_path, exist_ok=True)
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=bronze_path,
            existing_data_behavior="overwrite_or_ignore"
        )
        logger.info(f"Bronze UCI Parquet written to: {bronze_path}")

        # Step 7: Log run
        log_run(
            log_path=config["paths"]["ingestion_log"],
            source="uci",
            rows_ingested=len(df),
            null_rate_pct=null_rate_pct,
            status="SUCCESS"
        )

        # Step 8: Print summary
        print("\n" + "=" * 60)
        print("UCI Pima Indians Diabetes Bronze Ingestion — Summary")
        print("=" * 60)
        print(f"Total rows: {len(df)}")
        print(f"Overall null rate: {null_rate_pct:.2f}%")
        print(f"Columns: {list(df.columns)}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"UCI ingestion FAILED: {e}")
        log_run(
            log_path=config["paths"]["ingestion_log"],
            source="uci",
            rows_ingested=0,
            null_rate_pct=0.0,
            status="FAILED"
        )
        raise


if __name__ == "__main__":
    main()
