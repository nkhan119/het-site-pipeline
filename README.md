# het-site-pipeline

A reproducible Snakemake pipeline that:

1. Takes gVCF files and per-cohort metadata as input
2. Filters variants to **heterozygous sites (0/1, 1/0)** passing QC (DP > 20, GQ ≥ 30)
3. Counts passing sites per individual
4. Merges counts with metadata to produce a structured dataset: `SampleID, Age, Ancestry, IQ, Cohort, Het_Count`
5. Outputs one `.parquet` file per cohort + a cross-cohort merged parquet
6. Generates a `.pdf` report per cohort with summary visualisations

---

## Pipeline DAG

```
count_het_sites_by_chrom     per (cohort × sample × chrom) — fully parallelised
          │
          ▼
  gather_het_counts           sum chroms → one Het_Count per sample
          │
          ▼
    merge_counts              all samples → per-cohort TSV
          │
          ▼
     make_report              parquet + PDF per cohort
          │
          ▼
  merge_all_cohorts           single cross-cohort parquet
          │
          ▼
  benchmark_summary           wall time + RAM report
```

---

## Requirements

```bash
conda env create -f environment.yaml
conda activate het-site-pipeline
```

Dependencies: `snakemake>=8`, `bcftools>=1.17`, `pandas`, `pyarrow`, `matplotlib`, `seaborn`

---

## Input structure

```
Cohort_A/
├── metadata.tsv            # Required columns: SampleID, Age, Ancestry, IQ
├── sample1.gvcf.gz
├── sample1.gvcf.gz.tbi    # tabix index — see Step 1
└── ...
Cohort_B/
└── ...
config.yaml
Snakefile
scripts/
```

**metadata.tsv** format:

| SampleID | Age | Ancestry | IQ  |
|----------|-----|----------|-----|
| sampleA  | 32  | EUR      | 108 |
| sampleB  | 27  | AFR      | 115 |

---

## Quick start

```bash
# Step 1 — Index gVCFs (once only)
for GVCF in Cohort_A/*.gvcf.gz Cohort_B/*.gvcf.gz; do
    bcftools index --tbi "$GVCF"
done

# Step 2 — Dry run (check jobs without running)
snakemake -n --cores 8

# Step 3 — Quick test (2 chromosomes only)
snakemake --cores 8 --config chroms="[chr1,chr22]"

# Step 4 — Full run
snakemake --cores 8 --forceall
```

---

## Outputs

```
results/
├── Cohort_A/
│   ├── Cohort_A_final.parquet      # SampleID, Age, Ancestry, IQ, Cohort, Het_Count
│   └── Cohort_A_Report.pdf         # Boxplots, scatterplots, summary table
├── Cohort_B/
│   ├── Cohort_B_final.parquet
│   └── Cohort_B_Report.pdf
├── all_cohorts_merged.parquet      # All cohorts in one file
└── benchmarks/
    └── benchmark_summary.tsv       # Per-job wall time + peak RAM
```

---

## Adding samples or cohorts

```bash
# New sample in existing cohort
cp new.gvcf.gz Cohort_A/
bcftools index --tbi Cohort_A/new.gvcf.gz
snakemake --cores 8                 # only new sample processed

# New cohort
mkdir Cohort_C
cp *.gvcf.gz metadata.tsv Cohort_C/
for f in Cohort_C/*.gvcf.gz; do bcftools index --tbi $f; done
# Add "- Cohort_C" under cohorts: in config.yaml
snakemake --cores 8                 # existing cohorts skipped
```

---

## Configuration

| Parameter    | Default      | Description                         |
|--------------|--------------|-------------------------------------|
| `cohorts`    | see yaml     | Cohort folder names                 |
| `results_dir`| `results`    | Output root                         |
| `min_dp`     | `20`         | FORMAT/DP threshold (strict >)      |
| `min_gq`     | `30`         | FORMAT/GQ threshold (>=)            |
| `chroms`     | chr1–22,X,Y  | Chromosomes for scatter/gather      |

Override at runtime:
```bash
snakemake --cores 8 --config min_dp=30 min_gq=40
snakemake --cores 8 --config cohorts="Cohort_A,Cohort_B,Cohort_C"
```

---

## HPC / SLURM

```bash
snakemake \
  --executor slurm \
  --jobs 500 \
  --default-resources slurm_partition=normal mem_mb=4000 runtime=60
```

No Snakefile changes needed — same pipeline runs locally or on a cluster.

---

## Useful commands

| Task | Command |
|---|---|
| Dry run | `snakemake -n --cores 8` |
| Full run | `snakemake --cores 8` |
| Force rerun all | `snakemake --cores 8 --forceall` |
| Rerun one rule | `snakemake --cores 8 --forcerun make_report` |
| Unlock after crash | `snakemake --unlock` |
| Visualise DAG | `snakemake --dag \| dot -Tpng > dag.png` |
