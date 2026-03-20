# ============================================================
# het-site-pipeline
# ============================================================
# Input:  gVCF files grouped into cohort folders
#         + one metadata.tsv per cohort
# Output: per-cohort .parquet + .pdf report
#         + cross-cohort merged .parquet
#
# Usage:
#   snakemake --cores 8
#   snakemake --cores 8 --config cohorts="Cohort_A,Cohort_B,Cohort_C"
#   snakemake --executor slurm --jobs 500   # HPC
# ============================================================

configfile: "config.yaml"

# ── Parse config ─────────────────────────────────────────────
_cohorts  = config["cohorts"]
COHORTS   = _cohorts.split(",") if isinstance(_cohorts, str) else _cohorts
RESULTS   = config.get("results_dir", "results")
CHROMS    = config.get("chroms", [f"chr{i}" for i in list(range(1,23)) + ["X","Y"]])
MIN_DP    = config.get("min_dp", 20)
MIN_GQ    = config.get("min_gq", 30)

# Auto-retry on SLURM preemption / spot cancellation
restart_times: 3


# ── Discover samples from disk ────────────────────────────────
def get_samples(cohort):
    import glob, os
    gvcfs = glob.glob(f"{cohort}/*.gvcf.gz")
    if not gvcfs:
        raise ValueError(f"No .gvcf.gz files found in '{cohort}/'")
    return [os.path.basename(g).replace(".gvcf.gz", "") for g in gvcfs]


# ── Final targets ─────────────────────────────────────────────
rule all:
    input:
        # Per-cohort outputs
        expand(f"{RESULTS}/{{cohort}}/{{cohort}}_final.parquet", cohort=COHORTS),
        expand(f"{RESULTS}/{{cohort}}/{{cohort}}_Report.pdf",    cohort=COHORTS),
        # Cross-cohort merged parquet
        f"{RESULTS}/all_cohorts_merged.parquet",
        # Benchmark summary
        f"{RESULTS}/benchmarks/benchmark_summary.tsv",


# ═══════════════════════════════════════════════════════════════
# RULE 1 — Count het sites per sample per chromosome (scatter)
#
# Filters: genotype == het (0/1, 1/0), DP > MIN_DP, GQ >= MIN_GQ
# Parallelises: one job per (cohort, sample, chrom)
# ═══════════════════════════════════════════════════════════════
rule count_het_sites_by_chrom:
    input:
        gvcf = "{cohort}/{sample}.gvcf.gz",
        tbi  = "{cohort}/{sample}.gvcf.gz.tbi",
    output:
        temp("scatter/{cohort}/{sample}.{chrom}.txt"),
    params:
        dp = MIN_DP,
        gq = MIN_GQ,
    resources:
        mem_mb  = 2000,
        runtime = 30,
    retries: 3
    benchmark:
        "benchmarks/{cohort}/{sample}.{chrom}.tsv"
    log:
        "logs/{cohort}/{sample}.{chrom}.log"
    shell:
        """
        COUNT=$(bcftools view \
                    --genotype het \
                    --include 'FORMAT/DP>{params.dp} && FORMAT/GQ>={params.gq}' \
                    --regions {wildcards.chrom} \
                    --output-type u \
                    {input.gvcf} 2>>{log} \
                | bcftools view -H 2>>{log} \
                | wc -l)
        echo -e "{wildcards.sample}\t{wildcards.chrom}\t$COUNT" > {output}
        """


# ═══════════════════════════════════════════════════════════════
# RULE 2 — Sum chromosome counts → one total per sample (gather)
# ═══════════════════════════════════════════════════════════════
rule gather_het_counts:
    input:
        lambda wc: expand(
            "scatter/{cohort}/{sample}.{chrom}.txt",
            cohort=wc.cohort, sample=wc.sample, chrom=CHROMS,
        ),
    output:
        temp("{cohort}/{sample}.het_count.txt"),
    resources:
        mem_mb  = 500,
        runtime = 5,
    benchmark:
        "benchmarks/{cohort}/{sample}.gather.tsv"
    log:
        "logs/{cohort}/{sample}.gather.log"
    shell:
        """
        TOTAL=$(awk '{{sum+=$3}} END{{print sum+0}}' {input})
        echo -e "{wildcards.sample}\t$TOTAL" > {output}
        echo "[gather] {wildcards.sample}: $TOTAL het sites" >> {log}
        """


# ═══════════════════════════════════════════════════════════════
# RULE 3 — Merge all per-sample counts into a cohort-level TSV
# ═══════════════════════════════════════════════════════════════
rule merge_counts:
    input:
        lambda wc: expand(
            "{cohort}/{sample}.het_count.txt",
            cohort=wc.cohort,
            sample=get_samples(wc.cohort),
        ),
    output:
        temp("{cohort}_counts.tsv"),
    resources:
        mem_mb  = 1000,
        runtime = 10,
    benchmark:
        "benchmarks/{cohort}/merge_counts.tsv"
    log:
        "logs/{cohort}/merge_counts.log"
    shell:
        """
        echo -e "SampleID\tHet_Count" > {output}
        cat {input} >> {output}
        echo "[merge_counts] {wildcards.cohort}: $(tail -n+2 {output} | wc -l) samples" >> {log}
        """


# ═══════════════════════════════════════════════════════════════
# RULE 4 — Merge counts + metadata → parquet + PDF report
#
# Output columns: SampleID, Age, Ancestry, IQ, Cohort, Het_Count
# ═══════════════════════════════════════════════════════════════
rule make_report:
    input:
        tsv  = "{cohort}_counts.tsv",
        meta = "{cohort}/metadata.tsv",
    output:
        parquet = f"{RESULTS}/{{cohort}}/{{cohort}}_final.parquet",
        pdf     = f"{RESULTS}/{{cohort}}/{{cohort}}_Report.pdf",
    resources:
        mem_mb  = 4000,
        runtime = 20,
    benchmark:
        "benchmarks/{cohort}/make_report.tsv"
    log:
        "logs/{cohort}/make_report.log"
    script:
        "scripts/make_report.py"


# ═══════════════════════════════════════════════════════════════
# RULE 5 — Concatenate all cohort parquets → one cross-cohort file
#
# Re-runs automatically when new cohorts are added to config.yaml
# ═══════════════════════════════════════════════════════════════
rule merge_all_cohorts:
    input:
        expand(f"{RESULTS}/{{cohort}}/{{cohort}}_final.parquet", cohort=COHORTS),
    output:
        f"{RESULTS}/all_cohorts_merged.parquet",
    resources:
        mem_mb  = 8000,
        runtime = 30,
    benchmark:
        "benchmarks/merge_all_cohorts.tsv"
    log:
        "logs/merge_all_cohorts.log"
    script:
        "scripts/merge_all_cohorts.py"


# ═══════════════════════════════════════════════════════════════
# RULE 6 — Aggregate all benchmark TSVs into one summary table
# ═══════════════════════════════════════════════════════════════
rule benchmark_summary:
    input:
        expand(f"{RESULTS}/{{cohort}}/{{cohort}}_final.parquet", cohort=COHORTS),
        f"{RESULTS}/all_cohorts_merged.parquet",
    output:
        f"{RESULTS}/benchmarks/benchmark_summary.tsv",
    run:
        import glob, os, pandas as pd
        rows = []
        for f in glob.glob("benchmarks/**/*.tsv", recursive=True):
            try:
                df = pd.read_csv(f, sep="\t")
                df["benchmark_file"] = os.path.relpath(f)
                rows.append(df)
            except Exception:
                pass
        os.makedirs(os.path.dirname(output[0]), exist_ok=True)
        if rows:
            out = pd.concat(rows, ignore_index=True)
            out.to_csv(output[0], sep="\t", index=False)
            if "s" in out.columns:
                print("\n── Top 10 slowest jobs ──")
                print(out.nlargest(10, "s")[["benchmark_file","s","max_rss"]].to_string())
        else:
            open(output[0], "w").close()
