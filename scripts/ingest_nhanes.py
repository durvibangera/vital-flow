"""
ingest_nhanes.py — Bronze layer ingestion for CDC NHANES 2015-16 (cycle I) data.

Reads seven .XPT (SAS Transport) files from the 2015-16 NHANES survey cycle,
merges them sequentially on SEQN (the shared respondent sequence number),
and writes the combined raw record to the Bronze Parquet landing zone.

Source files (all cycle I suffix):
  DEMO_I.XPT   — Demographics: SEQN, age (RIDAGEYR), sex (RIAGENDR)
  BIOPRO_I.XPT — Biochemistry panel: fasting glucose (LBXSGL)
  DIQ_I.XPT    — Diabetes questionnaire: diagnosis label (DIQ010)
  GHB_I.XPT    — Glycohemoglobin: HbA1c (LBXGH)
  BPX_I.XPT    — Blood pressure exam: systolic BP (BPXSY1)
  BMX_I.XPT    — Body measures: BMI (BMXBMI)
  INS_I.XPT    — Insulin: LBXIN

No column renaming, unit conversion, or null-filling is performed at this stage.
All data is preserved exactly as received from the source.
"""

import sys
import os

# Ensure project root is on the path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pyreadstat
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from scripts.utils import load_config, get_logger, log_run

logger = get_logger("ingest_nhanes")


def safe_select(df: pd.DataFrame, columns: list, source_name: str) -> pd.DataFrame:
    """
    Select columns from a DataFrame. If a column is missing, log a warning
    and fill with NaN rather than raising an error.
    """
    present = [c for c in columns if c in df.columns]
    missing = [c for c in columns if c not in df.columns]
    for col in missing:
        logger.warning(f"Column '{col}' not found in {source_name} — filling with NaN")
    result = df[present].copy()
    for col in missing:
        result[col] = pd.NA
    return result


def main():
    config = load_config()

    try:
        # Step 1: Load all 7 XPT files
        logger.info("Loading NHANES 2015-16 (cycle I) XPT files...")

        demo_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_demo"])
        biopro_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_biopro"])
        diq_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_diq"])
        ghb_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_ghb"])
        bpx_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_bpx"])
        bmx_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_bmx"])
        ins_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_ins"])

        logger.info(f"  DEMO_I:   {len(demo_df)} rows, {len(demo_df.columns)} columns")
        logger.info(f"  BIOPRO_I: {len(biopro_df)} rows, {len(biopro_df.columns)} columns")
        logger.info(f"  DIQ_I:    {len(diq_df)} rows, {len(diq_df.columns)} columns")
        logger.info(f"  GHB_I:    {len(ghb_df)} rows, {len(ghb_df.columns)} columns")
        logger.info(f"  BPX_I:    {len(bpx_df)} rows, {len(bpx_df.columns)} columns")
        logger.info(f"  BMX_I:    {len(bmx_df)} rows, {len(bmx_df.columns)} columns")
        logger.info(f"  INS_I:    {len(ins_df)} rows, {len(ins_df.columns)} columns")

        # Step 2: Keep only needed columns from each file
        demo_df = safe_select(demo_df, ["SEQN", "RIDAGEYR", "RIAGENDR"], "DEMO_I")
        biopro_df = safe_select(biopro_df, ["SEQN", "LBXSGL"], "BIOPRO_I")
        diq_df = safe_select(diq_df, ["SEQN", "DIQ010"], "DIQ_I")
        ghb_df = safe_select(ghb_df, ["SEQN", "LBXGH"], "GHB_I")
        bpx_df = safe_select(bpx_df, ["SEQN", "BPXSY1"], "BPX_I")
        bmx_df = safe_select(bmx_df, ["SEQN", "BMXBMI"], "BMX_I")
        ins_df = safe_select(ins_df, ["SEQN", "LBXIN"], "INS_I")

        # Step 3: Merge all 7 DataFrames sequentially using left joins from DEMO
        logger.info("Merging all 7 files on SEQN (left join from DEMO)...")
        merged = demo_df
        for df in [biopro_df, diq_df, ghb_df, bpx_df, bmx_df, ins_df]:
            merged = pd.merge(merged, df, on="SEQN", how="left")

        logger.info(f"Merged DataFrame: {len(merged)} rows, {len(merged.columns)} columns")

        # Step 4: Add ingestion timestamp
        merged["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

        # Step 5: Compute null rate
        null_rate_pct = merged.isnull().mean().mean() * 100

        # Step 6: Write to Bronze Parquet
        bronze_path = config["paths"]["bronze_nhanes"]
        os.makedirs(bronze_path, exist_ok=True)
        table = pa.Table.from_pandas(merged)
        pq.write_to_dataset(
            table,
            root_path=bronze_path,
            existing_data_behavior="overwrite_or_ignore"
        )
        logger.info(f"Bronze NHANES Parquet written to: {bronze_path}")

        # Step 7: Log run
        log_run(
            log_path=config["paths"]["ingestion_log"],
            source="nhanes",
            rows_ingested=len(merged),
            null_rate_pct=null_rate_pct,
            status="SUCCESS"
        )

        # Step 8: Print summary
        print("\n" + "=" * 60)
        print("NHANES Bronze Ingestion — Summary")
        print("=" * 60)
        print(f"Total rows in merged DataFrame: {len(merged)}")
        print(f"Overall null rate: {null_rate_pct:.2f}%")
        print("\nNull rate per key column:")
        for col in ["LBXSGL", "LBXGH", "BPXSY1", "BMXBMI", "LBXIN"]:
            if col in merged.columns:
                col_null = merged[col].isnull().mean() * 100
                print(f"  {col}: {col_null:.2f}%")
            else:
                print(f"  {col}: column not present")

        # Completeness check: rows with all 4 key biomarkers present
        key_cols = ["LBXSGLU", "LBXGH", "BPXOSY1", "BMXBMI"]
        key_cols_present = [c for c in key_cols if c in merged.columns]
        complete_rows = merged[key_cols_present].notna().all(axis=1).sum()
        print(f"\nRows with all 4 key biomarkers present: {complete_rows}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"NHANES ingestion FAILED: {e}")
        log_run(
            log_path=config["paths"]["ingestion_log"],
            source="nhanes",
            rows_ingested=0,
            null_rate_pct=0.0,
            status="FAILED"
        )
        raise


if __name__ == "__main__":
    main()
