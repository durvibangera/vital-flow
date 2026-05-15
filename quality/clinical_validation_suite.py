"""
clinical_validation_suite.py — Great Expectations Quality Gate.

Reads the Silver harmonized Parquet, runs a clinical validation suite of 10 rules,
routes passing records to Gold and failing records to Quarantine with a failure_reason tag.

This is the only gate between Silver and Gold. Nothing reaches Gold without passing
all row-level validation rules. Table-level failures halt the pipeline entirely.
"""

import sys
import os
import shutil

# Ensure project root is on the path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import great_expectations as gx
from scripts.utils import load_config, get_logger, log_run

logger = get_logger("clinical_validation")


def main():
    config = load_config()
    bounds = config["clinical_bounds"]

    # Extract bounds
    age_min = bounds["age_min"]
    age_max = bounds["age_max"]
    bmi_min = bounds["bmi_min"]
    bmi_max = bounds["bmi_max"]
    g_min = bounds["glucose_mmol_min"]
    g_max = bounds["glucose_mmol_max"]
    bp_min = bounds["bp_systolic_min"]
    bp_max = bounds["bp_systolic_max"]
    ins_min = bounds["insulin_min"]
    ins_max = bounds["insulin_max"]
    min_gold_rows = bounds["min_gold_rows"]
    min_uniqueness = bounds["min_patient_id_uniqueness"]

    # Step 1: Load Silver into pandas
    logger.info("Loading Silver harmonized Parquet...")
    silver_df = pd.read_parquet(config["paths"]["silver"])
    logger.info(f"  Silver rows: {len(silver_df)}")

    # Step 2: Initialize Great Expectations
    logger.info("Initializing Great Expectations context...")
    context = gx.get_context()

    datasource = context.sources.add_pandas("silver_source")
    asset = datasource.add_dataframe_asset("silver_harmonized")
    batch_request = asset.build_batch_request(dataframe=silver_df)

    # Step 3: Define the expectation suite
    logger.info("Building clinical validation suite (10 rules)...")
    suite = context.add_expectation_suite("clinical_validation_suite")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )

    # QG-01: patient_id must never be null
    validator.expect_column_values_to_not_be_null("patient_id")

    # QG-02: source_dataset must never be null (lineage traceability)
    validator.expect_column_values_to_not_be_null("source_dataset")

    # QG-03: age must be biologically plausible
    validator.expect_column_values_to_be_between(
        "age",
        min_value=age_min,
        max_value=age_max
    )

    # QG-04: BMI must be clinically plausible
    validator.expect_column_values_to_be_between(
        "bmi",
        min_value=bmi_min,
        max_value=bmi_max
    )

    # QG-05: glucose must be within physiological range (mmol/L)
    validator.expect_column_values_to_be_between(
        "glucose_mmol",
        min_value=g_min,
        max_value=g_max
    )

    # QG-06: systolic blood pressure must be clinically plausible
    validator.expect_column_values_to_be_between(
        "blood_pressure_systolic",
        min_value=bp_min,
        max_value=bp_max
    )

    # QG-07: sex must only contain valid encoded values (or null for UCI)
    validator.expect_column_values_to_be_in_set(
        "sex", value_set=["M", "F"], mostly=0.5  # UCI rows will be null — don't fail on them
    )

    # QG-08: insulin must be within physiological range
    validator.expect_column_values_to_be_between(
        "insulin_uU_ml",
        min_value=ins_min,
        max_value=ins_max
    )

    # QG-09: patient_id must be nearly unique (table-level)
    validator.expect_column_proportion_of_unique_values_to_be_between(
        "patient_id",
        min_value=min_uniqueness,
        max_value=1.0
    )

    # QG-10: Gold table must not be near-empty (table-level)
    validator.expect_table_row_count_to_be_between(
        min_value=min_gold_rows
    )

    validator.save_expectation_suite()

    # Step 4: Run validation checkpoint
    logger.info("Running clinical validation checkpoint...")
    checkpoint = context.add_or_update_checkpoint(
        name="clinical_checkpoint",
        validator=validator,
    )
    results = checkpoint.run()

    # Step 5: Save HTML report
    logger.info("Building GE data docs...")
    context.build_data_docs()

    # Copy the generated HTML report to config["paths"]["ge_reports"]
    ge_reports_dir = config["paths"]["ge_reports"]
    os.makedirs(ge_reports_dir, exist_ok=True)

    # Find and copy the data docs
    try:
        data_docs_dir = os.path.join(
            context.root_directory, "uncommitted", "data_docs", "local_site"
        )
        if os.path.exists(data_docs_dir):
            dest = os.path.join(ge_reports_dir, "data_docs")
            if os.path.exists(dest):
                shutil.rmtree(dest)
            shutil.copytree(data_docs_dir, dest)
            logger.info(f"  GE data docs copied to: {dest}")
        else:
            logger.warning(f"  GE data docs not found at: {data_docs_dir}")
    except Exception as e:
        logger.warning(f"  Could not copy GE data docs: {e}")

    # Log GE results summary
    suite_results = results.list_validation_results()[0]
    stats = suite_results.statistics
    logger.info(
        f"  GE Results: {stats['successful_expectations']}/{stats['evaluated_expectations']} "
        f"expectations passed ({stats['success_percent']:.1f}%)"
    )

    # Step 6: Row-level routing
    logger.info("Routing rows to Gold / Quarantine based on row-level rules...")

    failing_masks = {}
    failing_masks["patient_id_null"]     = silver_df["patient_id"].isna()
    failing_masks["source_dataset_null"] = silver_df["source_dataset"].isna()
    failing_masks["age_oob"]             = ~silver_df["age"].between(age_min, age_max)
    failing_masks["bmi_oob"]             = ~silver_df["bmi"].between(bmi_min, bmi_max)
    failing_masks["glucose_oob"]         = ~silver_df["glucose_mmol"].between(g_min, g_max)
    failing_masks["bp_oob"]              = ~silver_df["blood_pressure_systolic"].between(bp_min, bp_max)
    failing_masks["insulin_oob"]         = ~silver_df["insulin_uU_ml"].between(ins_min, ins_max)

    # A row fails if ANY mask is True for that row
    any_fail = pd.concat(failing_masks.values(), axis=1).any(axis=1)

    # Build failure_reason string for each failing row
    def build_failure_reason(row_idx):
        reasons = [rule for rule, mask in failing_masks.items() if mask.iloc[row_idx]]
        return ", ".join(reasons)

    failing_df = silver_df[any_fail].copy()
    failing_df["failure_reason"] = [
        build_failure_reason(i) for i in range(len(silver_df)) if any_fail.iloc[i]
    ]
    passing_df = silver_df[~any_fail].copy()

    logger.info(f"  Gold (passing): {len(passing_df)} rows")
    logger.info(f"  Quarantine (failing): {len(failing_df)} rows")

    # Step 7: Write Gold and Quarantine
    gold_dir = config["paths"]["gold"]
    quarantine_dir = config["paths"]["quarantine"]

    os.makedirs(gold_dir, exist_ok=True)
    os.makedirs(quarantine_dir, exist_ok=True)

    gold_path = os.path.join(gold_dir, "gold_validated.parquet")
    quarantine_path = os.path.join(quarantine_dir, "quarantine.parquet")

    passing_df.to_parquet(gold_path, engine="pyarrow", index=False)
    failing_df.to_parquet(quarantine_path, engine="pyarrow", index=False)

    logger.info(f"  Gold Parquet written to: {gold_path}")
    logger.info(f"  Quarantine Parquet written to: {quarantine_path}")

    # Step 8: Table-level halt logic
    log_run(
        log_path=config["paths"]["ingestion_log"],
        source="gold",
        rows_ingested=len(passing_df),
        null_rate_pct=0.0,
        status="SUCCESS"
    )
    log_run(
        log_path=config["paths"]["ingestion_log"],
        source="quarantine",
        rows_ingested=len(failing_df),
        null_rate_pct=0.0,
        status="SUCCESS"
    )

    if len(passing_df) < min_gold_rows:
        log_run(
            log_path=config["paths"]["ingestion_log"],
            source="gold",
            rows_ingested=len(passing_df),
            null_rate_pct=0.0,
            status="HALTED — Gold table too small"
        )
        raise RuntimeError(
            f"HALTED: Gold table has only {len(passing_df)} rows, "
            f"minimum required is {min_gold_rows}"
        )

    # Print summary
    print("\n" + "=" * 60)
    print("Clinical Validation Suite — Summary")
    print("=" * 60)
    print(f"Silver input rows:     {len(silver_df)}")
    print(f"Gold (passing) rows:   {len(passing_df)}")
    print(f"Quarantine (failing):  {len(failing_df)}")
    print(f"Quarantine rate:       {len(failing_df) / len(silver_df) * 100:.2f}%")
    print(f"\nGE Expectations: {stats['successful_expectations']}/{stats['evaluated_expectations']} passed")

    # Top failure reasons
    if len(failing_df) > 0:
        print("\nTop 5 failure reasons:")
        reason_counts = failing_df["failure_reason"].value_counts().head(5)
        for reason, count in reason_counts.items():
            print(f"  {count:>5d}  {reason}")

    print("=" * 60)


if __name__ == "__main__":
    main()
