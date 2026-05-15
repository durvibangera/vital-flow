"""
app.py — VITAL-Flow Feasibility Tracker Dashboard.

A Streamlit dashboard that monitors pipeline health and data completeness
across the VITAL-Flow Medallion Architecture. Reads from Gold Parquet,
Quarantine Parquet, and the ingestion log CSV.

Run with: streamlit run dashboard/app.py
"""

import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scripts.utils import load_config

# ─── Page config ───
st.set_page_config(
    page_title="VITAL-Flow Feasibility Tracker",
    page_icon="🏥",
    layout="wide"
)

# ─── Load config ───
config = load_config()


# ─── Cached data loaders ───
@st.cache_data(ttl=60)
def load_gold():
    path = config["paths"]["gold"]
    parquet_file = os.path.join(path, "gold_validated.parquet")
    if os.path.exists(parquet_file):
        return pd.read_parquet(parquet_file)
    elif os.path.exists(path):
        return pd.read_parquet(path)
    return None


@st.cache_data(ttl=60)
def load_quarantine():
    path = config["paths"]["quarantine"]
    parquet_file = os.path.join(path, "quarantine.parquet")
    if os.path.exists(parquet_file):
        return pd.read_parquet(parquet_file)
    elif os.path.exists(path):
        return pd.read_parquet(path)
    return None


@st.cache_data(ttl=60)
def load_ingestion_log():
    path = config["paths"]["ingestion_log"]
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


# ─── Sidebar ───
with st.sidebar:
    st.title("🏥 VITAL-Flow")
    st.caption("Feasibility Tracker Dashboard")
    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption(f"Pipeline Version: {config['pipeline']['pipeline_version']}")
    st.caption(f"NHANES Cycle: {config['pipeline']['nhanes_cycle']}")

# ─── Main content ───
st.title("🏥 VITAL-Flow Feasibility Tracker")
st.info(
    "Data shown reflects the most recent pipeline run. "
    "Gold layer only — all records have passed clinical validation."
)

# Check if pipeline has been run
gold_df = load_gold()
quarantine_df = load_quarantine()
log_df = load_ingestion_log()

if gold_df is None or quarantine_df is None:
    st.warning(
        "Pipeline has not been run yet. "
        "Execute scripts in order: ingest → harmonize → validate."
    )
    st.stop()

# ─────────────────────────────────────────────────────────────
# Panel 1 — Pipeline Funnel
# ─────────────────────────────────────────────────────────────
st.header("📊 Panel 1 — Pipeline Funnel")

if log_df is not None:
    # Build funnel data from ingestion log
    stages = []
    colors = []

    # Bronze NHANES
    nhanes_rows = log_df[log_df["source"] == "nhanes"]
    if len(nhanes_rows) > 0:
        stages.append(("Bronze NHANES", nhanes_rows.iloc[-1]["rows_ingested"]))
        colors.append("#CD7F32")

    # Bronze UCI
    uci_rows = log_df[log_df["source"] == "uci"]
    if len(uci_rows) > 0:
        stages.append(("Bronze UCI", uci_rows.iloc[-1]["rows_ingested"]))
        colors.append("#CD7F32")

    # Silver
    silver_rows = log_df[log_df["source"] == "silver_union"]
    if len(silver_rows) > 0:
        stages.append(("Silver", silver_rows.iloc[-1]["rows_ingested"]))
        colors.append("#C0C0C0")

    # Gold
    gold_rows = log_df[log_df["source"] == "gold"]
    if len(gold_rows) > 0:
        stages.append(("Gold", gold_rows.iloc[-1]["rows_ingested"]))
        colors.append("#FFD700")

    # Quarantine
    q_rows = log_df[log_df["source"] == "quarantine"]
    if len(q_rows) > 0:
        stages.append(("Quarantine", q_rows.iloc[-1]["rows_ingested"]))
        colors.append("#E05252")

    if stages:
        stage_names = [s[0] for s in stages]
        stage_counts = [s[1] for s in stages]
        fig_funnel = go.Figure(data=[
            go.Bar(
                x=stage_names,
                y=stage_counts,
                marker_color=colors,
                text=stage_counts,
                textposition="auto"
            )
        ])
        fig_funnel.update_layout(
            title="Pipeline Funnel — Record Counts by Stage",
            xaxis_title="Stage",
            yaxis_title="Row Count",
            template="plotly_dark"
        )
        st.plotly_chart(fig_funnel, use_container_width=True)
else:
    st.warning("No ingestion log found.")

# ─────────────────────────────────────────────────────────────
# Panel 2 — Biomarker Completeness Scorecard
# ─────────────────────────────────────────────────────────────
st.header("🧬 Panel 2 — Biomarker Completeness (Gold Layer)")

biomarkers = ["glucose_mmol", "blood_pressure_systolic", "bmi", "hba1c", "insulin_uU_ml"]
total_rows = len(gold_df)

cols = st.columns(5)
for i, bm in enumerate(biomarkers):
    if bm in gold_df.columns:
        completeness = (gold_df[bm].notna().sum() / total_rows) * 100
    else:
        completeness = 0.0
    cols[i].metric(
        label=bm.replace("_", " ").title(),
        value=f"{completeness:.1f}%",
        delta=None
    )

# Grouped bar chart by source
if "source_dataset" in gold_df.columns:
    completeness_data = []
    for source in gold_df["source_dataset"].unique():
        source_df = gold_df[gold_df["source_dataset"] == source]
        for bm in biomarkers:
            if bm in source_df.columns:
                comp = (source_df[bm].notna().sum() / len(source_df)) * 100
            else:
                comp = 0.0
            completeness_data.append({
                "Source": source,
                "Biomarker": bm.replace("_", " ").title(),
                "Completeness (%)": round(comp, 1)
            })

    comp_df = pd.DataFrame(completeness_data)
    fig_comp = px.bar(
        comp_df,
        x="Biomarker",
        y="Completeness (%)",
        color="Source",
        barmode="group",
        template="plotly_dark",
        color_discrete_sequence=["#FFD700", "#4ECDC4"]
    )
    fig_comp.update_layout(title="Biomarker Completeness by Source Dataset")
    st.plotly_chart(fig_comp, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# Panel 3 — Quarantine Rate by Source
# ─────────────────────────────────────────────────────────────
st.header("🚫 Panel 3 — Quarantine Rate by Source")

if quarantine_df is not None and len(quarantine_df) > 0:
    # Compute rates
    qr_data = []
    for source in set(list(gold_df["source_dataset"].unique()) + list(quarantine_df["source_dataset"].unique())):
        g_count = len(gold_df[gold_df["source_dataset"] == source])
        q_count = len(quarantine_df[quarantine_df["source_dataset"] == source])
        total = g_count + q_count
        rate = (q_count / total * 100) if total > 0 else 0
        qr_data.append({"Source": source, "Quarantine Rate (%)": round(rate, 2), "Quarantined": q_count, "Total": total})

    qr_df = pd.DataFrame(qr_data)
    fig_qr = px.bar(
        qr_df,
        x="Source",
        y="Quarantine Rate (%)",
        color="Source",
        text="Quarantine Rate (%)",
        template="plotly_dark",
        color_discrete_sequence=["#E05252", "#FF6B6B"]
    )
    fig_qr.update_layout(title="Quarantine Rate by Source Dataset")
    st.plotly_chart(fig_qr, use_container_width=True)

    # Top 5 failure reasons
    st.subheader("Top 5 Failure Reasons")
    if "failure_reason" in quarantine_df.columns:
        top_reasons = quarantine_df["failure_reason"].value_counts().head(5).reset_index()
        top_reasons.columns = ["Failure Reason", "Count"]
        st.dataframe(top_reasons, use_container_width=True)
    else:
        st.info("No failure_reason column found in quarantine data.")
else:
    st.info("No quarantined records found.")

# ─────────────────────────────────────────────────────────────
# Panel 4 — Biomarker Distributions
# ─────────────────────────────────────────────────────────────
st.header("📈 Panel 4 — Biomarker Distributions — Gold Layer (by Source)")

col1, col2 = st.columns(2)

with col1:
    if "glucose_mmol" in gold_df.columns:
        fig_glucose = px.histogram(
            gold_df.dropna(subset=["glucose_mmol"]),
            x="glucose_mmol",
            color="source_dataset",
            barmode="overlay",
            nbins=50,
            template="plotly_dark",
            title="Glucose (mmol/L) Distribution",
            opacity=0.7,
            color_discrete_sequence=["#FFD700", "#4ECDC4"]
        )
        st.plotly_chart(fig_glucose, use_container_width=True)

with col2:
    if "bmi" in gold_df.columns:
        fig_bmi = px.histogram(
            gold_df.dropna(subset=["bmi"]),
            x="bmi",
            color="source_dataset",
            barmode="overlay",
            nbins=50,
            template="plotly_dark",
            title="BMI (kg/m²) Distribution",
            opacity=0.7,
            color_discrete_sequence=["#FFD700", "#4ECDC4"]
        )
        st.plotly_chart(fig_bmi, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# Panel 5 — Last Run Metadata
# ─────────────────────────────────────────────────────────────
st.header("📋 Panel 5 — Last Run Metadata")

if log_df is not None:
    # Most recent row per source
    latest = log_df.sort_values("timestamp").groupby("source").tail(1)
    display_cols = ["source", "timestamp", "rows_ingested", "null_rate_pct", "status"]
    available = [c for c in display_cols if c in latest.columns]
    st.table(latest[available].reset_index(drop=True))

st.caption(f"Pipeline Version: {config['pipeline']['pipeline_version']}")
