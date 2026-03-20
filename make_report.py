"""
scripts/make_report.py
======================
Merges het-site counts with sample metadata and produces:
  - A .parquet file with columns: SampleID, Age, Ancestry, IQ, Cohort, Het_Count
  - A .pdf report with summary boxplots

Called by Snakemake rule `make_report`.
The `snakemake` object is injected automatically at runtime.
"""

import os
import sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages

# ── Inputs / outputs from Snakemake ──────────────────────────
cohort   = snakemake.wildcards.cohort
counts_f = snakemake.input.tsv
meta_f   = snakemake.input.meta
out_parq = snakemake.output.parquet
out_pdf  = snakemake.output.pdf
log_f    = snakemake.log[0]
min_dp   = snakemake.config["min_dp"]
min_gq   = snakemake.config["min_gq"]


def log(msg):
    with open(log_f, "a") as fh:
        fh.write(msg + "\n")
    print(msg, file=sys.stderr)


log(f"[make_report] Starting {cohort}")

# ── Load data ─────────────────────────────────────────────────
meta   = pd.read_csv(meta_f,   sep="\t")
counts = pd.read_csv(counts_f, sep="\t")

# ── Merge ─────────────────────────────────────────────────────
df = pd.merge(meta, counts, on="SampleID", how="inner")
df["Cohort"] = cohort

# Enforce column order as specified in the task
df = df[["SampleID", "Age", "Ancestry", "IQ", "Cohort", "Het_Count"]]

log(f"[make_report] {cohort}: {len(df)} samples after merge")
log(f"[make_report] Het_Count stats:\n{df['Het_Count'].describe().to_string()}")

# ── Write Parquet ─────────────────────────────────────────────
os.makedirs(os.path.dirname(out_parq), exist_ok=True)
df.to_parquet(out_parq, index=False)
log(f"[make_report] Parquet written → {out_parq}")

# ── PDF Report ────────────────────────────────────────────────
PALETTE = {"Age": "#5b9bd5", "Het_Count": "#ed7d31", "IQ": "#70ad47"}

with PdfPages(out_pdf) as pdf:

    # -- Page 1: Overview boxplots ----------------------------
    fig = plt.figure(figsize=(14, 8))
    fig.suptitle(
        f"Summary Report: {cohort}   (n={len(df)} samples)\n"
        f"QC filters: DP > {min_dp}, GQ ≥ {min_gq}",
        fontsize=14, fontweight="bold", y=0.98,
    )
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

    # Het_Count boxplot (large, top-left span 2)
    ax0 = fig.add_subplot(gs[0, :2])
    sns.boxplot(data=df, x="Ancestry", y="Het_Count",
                palette="Set2", ax=ax0)
    ax0.set_title("Het_Count by Ancestry", fontweight="bold")
    ax0.set_xlabel("Ancestry")
    ax0.set_ylabel("Het Site Count")
    ax0.tick_params(axis="x", rotation=30)

    # Sample count by Ancestry (top-right)
    ax1 = fig.add_subplot(gs[0, 2])
    ancestry_counts = df["Ancestry"].value_counts()
    ax1.bar(ancestry_counts.index, ancestry_counts.values,
            color=sns.color_palette("Set2", len(ancestry_counts)))
    ax1.set_title("Samples per Ancestry", fontweight="bold")
    ax1.set_xlabel("Ancestry")
    ax1.set_ylabel("Count")
    ax1.tick_params(axis="x", rotation=30)

    # Age distribution (bottom-left)
    ax2 = fig.add_subplot(gs[1, 0])
    sns.boxplot(data=df, y="Age", color=PALETTE["Age"], ax=ax2)
    ax2.set_title("Age Distribution", fontweight="bold")

    # IQ distribution (bottom-middle)
    ax3 = fig.add_subplot(gs[1, 1])
    sns.boxplot(data=df, y="IQ", color=PALETTE["IQ"], ax=ax3)
    ax3.set_title("IQ Distribution", fontweight="bold")

    # Het_Count overall (bottom-right)
    ax4 = fig.add_subplot(gs[1, 2])
    sns.boxplot(data=df, y="Het_Count", color=PALETTE["Het_Count"], ax=ax4)
    ax4.set_title("Het_Count Overall", fontweight="bold")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

    # -- Page 2: Het_Count vs Age scatter ---------------------
    fig2, axes2 = plt.subplots(1, 2, figsize=(12, 5))
    fig2.suptitle(f"{cohort}: Het_Count relationships", fontsize=13, fontweight="bold")

    sns.scatterplot(data=df, x="Age", y="Het_Count",
                    hue="Ancestry", palette="Set2",
                    alpha=0.8, ax=axes2[0])
    axes2[0].set_title("Het_Count vs Age")

    sns.scatterplot(data=df, x="IQ", y="Het_Count",
                    hue="Ancestry", palette="Set2",
                    alpha=0.8, ax=axes2[1])
    axes2[1].set_title("Het_Count vs IQ")

    pdf.savefig(fig2, bbox_inches="tight")
    plt.close(fig2)

    # -- Page 3: Summary stats table --------------------------
    fig3, ax5 = plt.subplots(figsize=(10, 4))
    ax5.axis("off")
    summary = df.groupby("Ancestry").agg(
        N         =("SampleID",  "count"),
        Age_mean  =("Age",       "mean"),
        IQ_mean   =("IQ",        "mean"),
        Het_mean  =("Het_Count", "mean"),
        Het_median=("Het_Count", "median"),
        Het_std   =("Het_Count", "std"),
    ).round(1).reset_index()
    tbl = ax5.table(
        cellText    = summary.values,
        colLabels   = summary.columns,
        cellLoc     = "center",
        loc         = "center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.2, 1.6)
    ax5.set_title(f"{cohort}: Summary statistics by Ancestry",
                  fontweight="bold", pad=20)
    pdf.savefig(fig3, bbox_inches="tight")
    plt.close(fig3)

    # -- PDF metadata -----------------------------------------
    d = pdf.infodict()
    d["Title"]   = f"Het-Site Report: {cohort}"
    d["Author"]  = "het-site-pipeline"
    d["Subject"] = f"QC: DP>{min_dp}, GQ>={min_gq}"

log(f"[make_report] PDF written → {out_pdf}")
