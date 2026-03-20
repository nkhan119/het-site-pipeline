#!/bin/bash
#SBATCH --account=def-group-name
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=24:00:00
#SBATCH --mail-user=nadeem.khan@inrs.ca
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --job-name=het_pipeline
#SBATCH --output=logs/slurm/het_pipeline.%j.out
#SBATCH --error=logs/slurm/het_pipeline.%j.err

# ============================================================
# het-site-pipeline — SLURM submission script (Narval / CC)
#
# This script is the Snakemake controller job.
# Each pipeline rule is submitted as its own SLURM job.
#
# Submit with:
#   sbatch submit_pipeline.sh
# ============================================================

set -euo pipefail

# ── Working directory ─────────────────────────────────────────
cd $SLURM_SUBMIT_DIR

# ── Create log directories ────────────────────────────────────
mkdir -p logs/slurm benchmarks

# ── Print run info ────────────────────────────────────────────
echo "============================================"
echo "Job ID     : $SLURM_JOB_ID"
echo "Node       : $SLURMD_NODENAME"
echo "Submit dir : $SLURM_SUBMIT_DIR"
echo "Start time : $(date)"
echo "Snakemake  : $(snakemake --version)"
echo "Python     : $(python3 --version)"
echo "bcftools   : $(bcftools --version | head -1)"
echo "============================================"

# ── Dry run first ─────────────────────────────────────────────
echo ""
echo "--- Dry run ---"
snakemake -n \
    --executor slurm \
    --jobs 500 \
    --default-resources \
        slurm_account=def--group-name \
        mem_mb=4000 \
        runtime=60 \
    --latency-wait 60

# ── Real run ──────────────────────────────────────────────────
echo ""
echo "--- Starting pipeline ---"
snakemake \
    --executor slurm \
    --jobs 500 \
    --default-resources \
        slurm_account=def--group-name \
        mem_mb=4000 \
        runtime=60 \
    --latency-wait 60 \
    --rerun-incomplete \
    --keep-going

# ── Done ──────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "Pipeline finished: $(date)"
echo "Results in: $SLURM_SUBMIT_DIR/results/"
echo "============================================"

