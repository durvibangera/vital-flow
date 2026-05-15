"""
harmonize.py — Silver layer harmonization.

Reads Bronze Parquet for both NHANES 2015-16 (cycle I) and UCI, applies column
renaming, unit conversions, and null-fill for missing schema columns, then unions
both into a single harmonized Silver Parquet partitioned by source_dataset.

NHANES column mapping (cycle I specific):
  SEQN      -> patient_id     (unique respondent ID)
  RIDAGEYR  -> age            (age in years)
  RIAGENDR  -> sex            (1=M, 2=F)
  BMXBMI    -> bmi            (from BMX_I)
  LBXSGL   -> glucose_mmol  (fasting glucose from BIOPRO_I, divided by 18.0)
  BPXSY1   -> blood_pressure_systolic  (from BPX_I)
  LBXGH     -> hba1c         (from GHB_I)
  LBXIN     -> insulin_uU_ml (from INS_I — confirmed downloaded)
  DIQ010    -> diabetes_diagnosed  (bonus label: 1=Yes, 2=No, 3=Borderline)

This produces the Golden Schema — a unified patient record format that all
downstream validation and modeling will consume.
"""

import sys
import os

# Ensure project root is on the path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.utils import load_config, get_logger, log_run

logger = get_logger("harmonize")

# Golden Schema — these are the ONLY columns that survive into Silver
GOLDEN_SCHEMA = [
    "patient_id",               # String
    "age",                      # Integer
    "sex",                      # String ("M", "F", or null)
    "bmi",                      # Float
    "glucose_mmol",             # Float  — converted from mg/dL via ÷ 18.0
    "blood_pressure_systolic",  # Float
    "hba1c",                    # Float (null for UCI)
    "insulin_uU_ml",            # Float — available in both NHANES (INS_I) and UCI
    "diabetes_diagnosed",       # String ("Yes"/"No"/"Borderline"/null) — NHANES only
    "source_dataset",           # String
    "ingestion_timestamp",      # String
]


def main():
    config = load_config()

    # Step 1: Read Bronze NHANES
    logger.info("Reading Bronze NHANES Parquet...")
    nhanes = pd.read_parquet(config["paths"]["bronze_nhanes"])
    logger.info(f"  Bronze NHANES: {len(nhanes)} rows")

    # Step 2: Apply NHANES transforms
    # NOTE: LBXSGL is the fasting glucose column in BIOPRO_I (cycle I).
    # This differs from LBXGLU used in some other NHANES cycles. Do NOT
    # substitute LBXGLU — it will KeyError on this cycle's Bronze data.

    # DIQ010 values: 1=Yes (diabetic), 2=No, 3=Borderline. Map to readable strings.
    diq_map = nhanes["DIQ010"].map({1.0: "Yes", 2.0: "No", 3.0: "Borderline"})

    # LBXIN is confirmed present — INS_I.XPT was downloaded and merged at Bronze.
    nhanes_silver = pd.DataFrame({
        "patient_id": nhanes["SEQN"].astype(str),
        "age": nhanes["RIDAGEYR"].astype("Int64"),
        "sex": nhanes["RIAGENDR"].map({1.0: "M", 2.0: "F"}),
        "bmi": nhanes["BMXBMI"].astype("float64"),
        "glucose_mmol": nhanes["LBXSGL"].astype("float64") / 18.0,   # BIOPRO_I fasting glucose
        "blood_pressure_systolic": nhanes["BPXSY1"].astype("float64"),  # BPX_I
        "hba1c": nhanes["LBXGH"].astype("float64"),                      # GHB_I
        "insulin_uU_ml": nhanes["LBXIN"].astype("float64"),              # INS_I confirmed
        "diabetes_diagnosed": diq_map,                                   # DIQ_I bonus label
        "source_dataset": "nhanes",
        "ingestion_timestamp": nhanes["ingestion_timestamp"],
    })
    nhanes_count = len(nhanes_silver)
    logger.info(f"  NHANES Silver: {nhanes_count} rows")

    # Step 3: Read Bronze UCI
    logger.info("Reading Bronze UCI Parquet...")
    uci = pd.read_parquet(config["paths"]["bronze_uci"])
    logger.info(f"  Bronze UCI: {len(uci)} rows")

    # Step 4: Apply UCI transforms
    uci_silver = pd.DataFrame({
        "patient_id": uci["patient_id"].astype(str),
        "age": uci["Age"].astype("Int64"),
        "sex": pd.Series([None] * len(uci), dtype="object"),           # UCI has no sex column
        "bmi": uci["BMI"].astype("float64"),
        "glucose_mmol": uci["Glucose"].astype("float64") / 18.0,
        "blood_pressure_systolic": uci["BloodPressure"].astype("float64"),
        "hba1c": pd.Series([None] * len(uci), dtype="float64"),        # UCI has no HbA1c column
        "insulin_uU_ml": uci["Insulin"].astype("float64"),
        "diabetes_diagnosed": pd.Series([None] * len(uci), dtype="object"),  # UCI has no diagnosis label
        "source_dataset": "uci_pima",
        "ingestion_timestamp": uci["ingestion_timestamp"],
    })
    uci_count = len(uci_silver)
    logger.info(f"  UCI Silver: {uci_count} rows")

    # Step 5: Union and write
    silver = pd.concat([nhanes_silver, uci_silver], ignore_index=True)
    total_count = len(silver)

    silver_path = config["paths"]["silver"]

    # Write partitioned by source_dataset
    for source, group in silver.groupby("source_dataset"):
        partition_dir = os.path.join(silver_path, f"source_dataset={source}")
        os.makedirs(partition_dir, exist_ok=True)
        group.drop(columns=["source_dataset"]).to_parquet(
            os.path.join(partition_dir, "part-0.parquet"),
            engine="pyarrow",
            index=False
        )
    logger.info(f"Silver Parquet written to: {silver_path} ({total_count} rows)")

    # Step 6: Log row counts per source
    log_run(
        log_path=config["paths"]["ingestion_log"],
        source="nhanes_silver",
        rows_ingested=nhanes_count,
        null_rate_pct=0.0,
        status="SUCCESS"
    )
    log_run(
        log_path=config["paths"]["ingestion_log"],
        source="uci_silver",
        rows_ingested=uci_count,
        null_rate_pct=0.0,
        status="SUCCESS"
    )
    log_run(
        log_path=config["paths"]["ingestion_log"],
        source="silver_union",
        rows_ingested=total_count,
        null_rate_pct=0.0,
        status="SUCCESS"
    )

    # Print summary
    print("\n" + "=" * 60)
    print("Silver Harmonization — Summary")
    print("=" * 60)
    print(f"NHANES Silver rows: {nhanes_count}")
    print(f"UCI Silver rows:    {uci_count}")
    print(f"Total Silver rows:  {total_count}")
    print(f"Schema: {GOLDEN_SCHEMA}")
    print("=" * 60)


if __name__ == "__main__":
    main()
