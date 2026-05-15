"""
vital_flow_pipeline.py — Prefect Orchestration for VITAL-Flow.

Orchestrates the full Medallion Architecture pipeline as a managed Prefect Flow:
  Task 1: Bronze NHANES ingestion   (ingest_nhanes.py logic)
  Task 2: Bronze UCI ingestion      (ingest_uci.py logic)
  Task 3: Silver harmonization      (harmonize.py logic)
  Task 4: Quality Gate validation   (clinical_validation_suite.py logic)

Run with:
  python pipeline/vital_flow_pipeline.py

Monitor at:
  prefect server start   (then open http://localhost:4200)
"""

import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pyreadstat
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from scripts.utils import load_config, log_run


# ─────────────────────────────────────────────────────────────
# TASK 1: Bronze NHANES Ingestion
# ─────────────────────────────────────────────────────────────

@task(name="Bronze NHANES Ingestion", retries=2, retry_delay_seconds=10)
def ingest_nhanes(config: dict) -> int:
    """
    Reads 7 NHANES 2015-16 XPT files, merges on SEQN, writes Bronze Parquet.
    Returns the number of rows ingested.
    """
    logger = get_run_logger()
    logger.info("Starting Bronze NHANES ingestion...")

    def safe_select(df, columns, source_name):
        present = [c for c in columns if c in df.columns]
        missing = [c for c in columns if c not in df.columns]
        for col in missing:
            logger.warning(f"Column '{col}' not found in {source_name} — filling with NaN")
        result = df[present].copy()
        for col in missing:
            result[col] = pd.NA
        return result

    demo_df, _   = pyreadstat.read_xport(config["paths"]["raw_nhanes_demo"])
    biopro_df, _ = pyreadstat.read_xport(config["paths"]["raw_nhanes_biopro"])
    diq_df, _    = pyreadstat.read_xport(config["paths"]["raw_nhanes_diq"])
    ghb_df, _    = pyreadstat.read_xport(config["paths"]["raw_nhanes_ghb"])
    bpx_df, _    = pyreadstat.read_xport(config["paths"]["raw_nhanes_bpx"])
    bmx_df, _    = pyreadstat.read_xport(config["paths"]["raw_nhanes_bmx"])
    ins_df, _    = pyreadstat.read_xport(config["paths"]["raw_nhanes_ins"])

    logger.info(f"  DEMO_I: {len(demo_df)} rows | BIOPRO_I: {len(biopro_df)} rows | INS_I: {len(ins_df)} rows")

    demo_df   = safe_select(demo_df,   ["SEQN", "RIDAGEYR", "RIAGENDR"], "DEMO_I")
    biopro_df = safe_select(biopro_df, ["SEQN", "LBXSGL"],               "BIOPRO_I")
    diq_df    = safe_select(diq_df,    ["SEQN", "DIQ010"],                "DIQ_I")
    ghb_df    = safe_select(ghb_df,    ["SEQN", "LBXGH"],                 "GHB_I")
    bpx_df    = safe_select(bpx_df,    ["SEQN", "BPXSY1"],                "BPX_I")
    bmx_df    = safe_select(bmx_df,    ["SEQN", "BMXBMI"],                "BMX_I")
    ins_df    = safe_select(ins_df,    ["SEQN", "LBXIN"],                  "INS_I")

    merged = demo_df
    for df in [biopro_df, diq_df, ghb_df, bpx_df, bmx_df, ins_df]:
        merged = pd.merge(merged, df, on="SEQN", how="left")

    merged["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    null_rate_pct = merged.isnull().mean().mean() * 100

    bronze_path = config["paths"]["bronze_nhanes"]
    os.makedirs(bronze_path, exist_ok=True)
    table = pa.Table.from_pandas(merged)
    pq.write_to_dataset(table, root_path=bronze_path, existing_data_behavior="overwrite_or_ignore")

    log_run(config["paths"]["ingestion_log"], "nhanes", len(merged), null_rate_pct, "SUCCESS")
    logger.info(f"  Bronze NHANES written: {len(merged)} rows, null_rate={null_rate_pct:.2f}%")
    return len(merged)


# ─────────────────────────────────────────────────────────────
# TASK 2: Bronze UCI Ingestion
# ─────────────────────────────────────────────────────────────

@task(name="Bronze UCI Ingestion", retries=2, retry_delay_seconds=10)
def ingest_uci(config: dict) -> int:
    """
    Reads UCI Pima CSV, fixes zero-as-null, generates patient_id, writes Bronze Parquet.
    Returns the number of rows ingested.
    """
    logger = get_run_logger()
    logger.info("Starting Bronze UCI ingestion...")

    df = pd.read_csv(config["paths"]["raw_uci"], na_values=["?"])

    for col in config["uci_impossible_zero_columns"]:
        if col in df.columns:
            df[col] = df[col].replace(0, pd.NA)

    df["patient_id"] = "uci_" + pd.Series(range(len(df))).astype(str).str.zfill(6)
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    null_rate_pct = df.isnull().mean().mean() * 100

    bronze_path = config["paths"]["bronze_uci"]
    os.makedirs(bronze_path, exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=bronze_path, existing_data_behavior="overwrite_or_ignore")

    log_run(config["paths"]["ingestion_log"], "uci", len(df), null_rate_pct, "SUCCESS")
    logger.info(f"  Bronze UCI written: {len(df)} rows, null_rate={null_rate_pct:.2f}%")
    return len(df)


# ─────────────────────────────────────────────────────────────
# TASK 3: Silver Harmonization
# ─────────────────────────────────────────────────────────────

@task(name="Silver Harmonization", retries=1)
def harmonize(config: dict, nhanes_rows: int, uci_rows: int) -> int:
    """
    Reads Bronze Parquet for both sources, applies Golden Schema transforms,
    unions, and writes Silver Parquet partitioned by source_dataset.
    Returns total Silver row count.
    """
    logger = get_run_logger()
    logger.info("Starting Silver harmonization...")

    nhanes = pd.read_parquet(config["paths"]["bronze_nhanes"])
    uci    = pd.read_parquet(config["paths"]["bronze_uci"])

    logger.info(f"  Bronze NHANES: {len(nhanes)} rows | Bronze UCI: {len(uci)} rows")

    diq_map = nhanes["DIQ010"].map({1.0: "Yes", 2.0: "No", 3.0: "Borderline"})

    nhanes_silver = pd.DataFrame({
        "patient_id":              nhanes["SEQN"].astype(str),
        "age":                     nhanes["RIDAGEYR"].astype("Int64"),
        "sex":                     nhanes["RIAGENDR"].map({1.0: "M", 2.0: "F"}),
        "bmi":                     nhanes["BMXBMI"].astype("float64"),
        "glucose_mmol":            nhanes["LBXSGL"].astype("float64") / 18.0,
        "blood_pressure_systolic": nhanes["BPXSY1"].astype("float64"),
        "hba1c":                   nhanes["LBXGH"].astype("float64"),
        "insulin_uU_ml":           nhanes["LBXIN"].astype("float64"),
        "diabetes_diagnosed":      diq_map,
        "source_dataset":          "nhanes",
        "ingestion_timestamp":     nhanes["ingestion_timestamp"],
    })

    uci_silver = pd.DataFrame({
        "patient_id":              uci["patient_id"].astype(str),
        "age":                     uci["Age"].astype("Int64"),
        "sex":                     pd.Series([None] * len(uci), dtype="object"),
        "bmi":                     uci["BMI"].astype("float64"),
        "glucose_mmol":            uci["Glucose"].astype("float64") / 18.0,
        "blood_pressure_systolic": uci["BloodPressure"].astype("float64"),
        "hba1c":                   pd.Series([None] * len(uci), dtype="float64"),
        "insulin_uU_ml":           uci["Insulin"].astype("float64"),
        "diabetes_diagnosed":      pd.Series([None] * len(uci), dtype="object"),
        "source_dataset":          "uci_pima",
        "ingestion_timestamp":     uci["ingestion_timestamp"],
    })

    silver = pd.concat([nhanes_silver, uci_silver], ignore_index=True)
    silver_path = config["paths"]["silver"]

    for source, group in silver.groupby("source_dataset"):
        partition_dir = os.path.join(silver_path, f"source_dataset={source}")
        os.makedirs(partition_dir, exist_ok=True)
        group.drop(columns=["source_dataset"]).to_parquet(
            os.path.join(partition_dir, "part-0.parquet"),
            engine="pyarrow", index=False
        )

    log_run(config["paths"]["ingestion_log"], "silver_union", len(silver), 0.0, "SUCCESS")
    logger.info(f"  Silver written: {len(silver)} total rows")
    return len(silver)


# ─────────────────────────────────────────────────────────────
# TASK 4: Quality Gate Validation
# ─────────────────────────────────────────────────────────────

@task(name="Quality Gate — Clinical Validation", retries=1)
def validate(config: dict, silver_rows: int) -> dict:
    """
    Applies 7 row-level clinical rules. Routes passing rows to Gold,
    failing rows to Quarantine with failure_reason tag.
    Returns a dict with gold_rows and quarantine_rows counts.
    """
    logger = get_run_logger()
    logger.info("Starting Quality Gate validation...")

    bounds = config["clinical_bounds"]
    silver_df = pd.read_parquet(config["paths"]["silver"])
    logger.info(f"  Silver input: {len(silver_df)} rows")

    fm = {}
    fm["patient_id_null"]     = silver_df["patient_id"].isna()
    fm["source_dataset_null"] = silver_df["source_dataset"].isna()
    fm["age_oob"]             = ~silver_df["age"].between(bounds["age_min"], bounds["age_max"])
    fm["bmi_oob"]             = ~silver_df["bmi"].between(bounds["bmi_min"], bounds["bmi_max"])
    fm["glucose_oob"]         = ~silver_df["glucose_mmol"].between(bounds["glucose_mmol_min"], bounds["glucose_mmol_max"])
    fm["bp_oob"]              = ~silver_df["blood_pressure_systolic"].between(bounds["bp_systolic_min"], bounds["bp_systolic_max"])
    fm["insulin_oob"]         = ~silver_df["insulin_uU_ml"].between(bounds["insulin_min"], bounds["insulin_max"])

    any_fail = pd.concat(fm.values(), axis=1).any(axis=1)

    failing_df = silver_df[any_fail].copy()
    failing_df["failure_reason"] = [
        ", ".join([r for r, m in fm.items() if m.iloc[i]])
        for i in range(len(silver_df)) if any_fail.iloc[i]
    ]
    passing_df = silver_df[~any_fail].copy()

    # Write Gold
    gold_dir = config["paths"]["gold"]
    os.makedirs(gold_dir, exist_ok=True)
    passing_df.to_parquet(os.path.join(gold_dir, "gold_validated.parquet"), engine="pyarrow", index=False)

    # Write Quarantine
    q_dir = config["paths"]["quarantine"]
    os.makedirs(q_dir, exist_ok=True)
    failing_df.to_parquet(os.path.join(q_dir, "quarantine.parquet"), engine="pyarrow", index=False)

    log_run(config["paths"]["ingestion_log"], "gold",       len(passing_df), 0.0, "SUCCESS")
    log_run(config["paths"]["ingestion_log"], "quarantine", len(failing_df), 0.0, "SUCCESS")

    # Halt if Gold is too small
    if len(passing_df) < bounds["min_gold_rows"]:
        raise RuntimeError(
            f"HALTED: Gold table has {len(passing_df)} rows — below minimum of {bounds['min_gold_rows']}"
        )

    logger.info(f"  Gold: {len(passing_df)} rows | Quarantine: {len(failing_df)} rows")
    return {"gold_rows": len(passing_df), "quarantine_rows": len(failing_df)}


# ─────────────────────────────────────────────────────────────
# TASK 5: Pipeline Summary Artifact
# ─────────────────────────────────────────────────────────────

@task(name="Publish Run Summary")
def publish_summary(nhanes_rows: int, uci_rows: int, silver_rows: int, results: dict):
    """Publishes a Markdown run summary as a Prefect artifact."""
    quarantine_rate = results["quarantine_rows"] / silver_rows * 100
    summary = f"""
# VITAL-Flow Pipeline Run Summary

| Stage | Rows |
|:---|---:|
| Bronze NHANES | {nhanes_rows:,} |
| Bronze UCI | {uci_rows:,} |
| Silver (Union) | {silver_rows:,} |
| **Gold (Validated)** | **{results['gold_rows']:,}** |
| Quarantine | {results['quarantine_rows']:,} |

**Quarantine Rate:** {quarantine_rate:.1f}%
**Pipeline Version:** VITAL-Flow v1.1
**Status:** ✅ SUCCESS
"""
    create_markdown_artifact(
        key="pipeline-run-summary",
        markdown=summary,
        description="VITAL-Flow pipeline run metrics"
    )


# ─────────────────────────────────────────────────────────────
# MAIN FLOW
# ─────────────────────────────────────────────────────────────

@flow(
    name="VITAL-Flow Pipeline",
    description="Medallion Architecture: Bronze → Silver → Gold for NHANES + UCI clinical data."
)
def vital_flow_pipeline():
    """
    Full orchestrated pipeline:
      1. Bronze NHANES ingestion
      2. Bronze UCI ingestion       (runs in parallel with NHANES)
      3. Silver harmonization       (depends on both Bronze tasks)
      4. Quality Gate validation    (depends on Silver)
      5. Publish run summary        (depends on all above)
    """
    config = load_config()

    # Tasks 1 & 2 run concurrently (no dependency between them)
    nhanes_rows = ingest_nhanes(config)
    uci_rows    = ingest_uci(config)

    # Task 3 depends on both Bronze tasks completing
    silver_rows = harmonize(config, nhanes_rows, uci_rows)

    # Task 4 depends on Silver
    results = validate(config, silver_rows)

    # Task 5: publish summary artifact
    publish_summary(nhanes_rows, uci_rows, silver_rows, results)

    return results


if __name__ == "__main__":
    vital_flow_pipeline()
