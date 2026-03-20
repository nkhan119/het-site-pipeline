"""
scripts/merge_all_cohorts.py
============================
Concatenates all per-cohort Parquet files into a single
cross-cohort Parquet file and prints summary statistics.

Called by Snakemake rule `merge_all_cohorts`.
"""

import os
import sys
import pandas as pd

parquets = snakemake.input
out_path = snakemake.output[0]
log_f    = snakemake.log[0]


def log(msg):
    with open(log_f, "a") as fh:
        fh.write(msg + "\n")
    print(msg, file=sys.stderr)


log(f"[merge_all_cohorts] Merging {len(parquets)} cohort(s)")

frames = []
for f in parquets:
    df = pd.read_parquet(f)
    log(f"  - {f}: {len(df)} samples")
    frames.append(df)

merged = pd.concat(frames, ignore_index=True)

os.makedirs(os.path.dirname(out_path), exist_ok=True)
merged.to_parquet(out_path, index=False)

log(f"\n[merge_all_cohorts] Total: {len(merged)} samples across {merged['Cohort'].nunique()} cohort(s)")
log("\nPer-cohort Het_Count summary:")
log(merged.groupby("Cohort")[["Het_Count"]].describe().round(1).to_string())
log(f"\nOutput → {out_path}")
