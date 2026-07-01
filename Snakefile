"""
MBSF Mortality Denominator Pipeline - Snakemake Workflow
"""

# Imports
import os
from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

# Load Hydra configuration
config_dir = os.path.join(os.getcwd(), "conf")
with initialize_config_dir(version_base=None, config_dir=config_dir):
    cfg = compose(config_name="config")

run_cfg = OmegaConf.to_container(cfg.run, resolve=True)

# Helper: ensure value is always a list
def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


# Determine years to process
if run_cfg.get("year") is not None:
    years = [int(y) for y in ensure_list(run_cfg["year"])]
else:
    years = list(range(int(run_cfg["year_start"]), int(run_cfg["year_end"]) + 1))

# Determine temporal frequencies and geoboundaries
temporal_frequencies = ensure_list(run_cfg.get("temporal_frequency"))
geoboundaries = ensure_list(run_cfg.get("geoboundaries"))

# Paths from config
normalization_dir = cfg.datapaths.dirs.input.normalization
intermediate_denom_dir = cfg.datapaths.dirs.intermediate.denom
output_dirs = cfg.datapaths.dirs.output
denom_dir = output_dirs.denom

# Logs directory
log_dir = os.path.join(os.getcwd(), "logs", "rules")
os.makedirs(log_dir, exist_ok=True)

# Final QC report path
qc_report_dir = os.path.join(denom_dir, "qc_reports")
qc_report_file = os.path.join(qc_report_dir, "qc_report.json")

# Build list of all final workflow parquet targets
all_targets = expand(os.path.join(denom_dir, "denom_{year}.parquet"), year=years)

for geo in geoboundaries:
    for freq in temporal_frequencies:
        output_dir = output_dirs[geo][freq]
        strat_pattern = (
            "strat_sparse_counts_{year}.parquet"
            if freq == "daily"
            else "strat_counts_{year}.parquet"
        )

        all_targets += expand(
            os.path.join(output_dir, "counts_{year}.parquet"),
            year=years,
        )
        all_targets += expand(
            os.path.join(output_dir, strat_pattern),
            year=years,
        )

# Add final QC report to workflow targets
all_targets += [qc_report_file]


rule all:
    input:
        all_targets

rule denom_intermediate:
    input:
        enroll=os.path.join(normalization_dir, "enrollments_{year}.parquet"),
        beneficiaries=os.path.join(normalization_dir, "beneficiaries_{year}.parquet"),
    output:
        done=os.path.join(intermediate_denom_dir, "denom_intermediate_{year}.parquet"),
    log:
        os.path.join(log_dir, "denom_intermediate_{year}.log")
    shell:
        """
        python src/05_denom.py \
            run.year={wildcards.year} \
            datapaths.dirs.intermediate.denom={intermediate_denom_dir} \
            datapaths.dirs.input.normalization={normalization_dir} \
            &> {log}
        """

rule denom:
    input:
        denom_intermediate=os.path.join(
            intermediate_denom_dir, "denom_intermediate_{year}.parquet"
        ),
    output:
        done=os.path.join(denom_dir, "denom_{year}.parquet"),
    log:
        os.path.join(log_dir, "denom_{year}.log")
    shell:
        """
        python src/06_zcta2county.py \
            run.year={wildcards.year} \
            datapaths.dirs.intermediate.denom={intermediate_denom_dir} \
            datapaths.dirs.output.denom={denom_dir} \
            &> {log}
        """

rule zcta_counts_yearly:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["zcta"]["yearly"], "counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "zcta_counts_yearly_{year}.log")
    params:
        out_dir=output_dirs["zcta"]["yearly"],
    shell:
        """
        python src/07_zcta_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=yearly \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.zcta.yearly={params.out_dir} \
            &> {log}
        """

rule zcta_counts_daily:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["zcta"]["daily"], "counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "zcta_counts_daily_{year}.log")
    params:
        out_dir=output_dirs["zcta"]["daily"],
    shell:
        """
        python src/07_zcta_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=daily \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.zcta.daily={params.out_dir} \
            &> {log}
        """

rule zcta_strat_counts_yearly:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["zcta"]["yearly"], "strat_counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "zcta_strat_counts_yearly_{year}.log")
    params:
        out_dir=output_dirs["zcta"]["yearly"],
    shell:
        """
        python src/08_zcta_strat_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=yearly \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.zcta.yearly={params.out_dir} \
            &> {log}

        rm -f duckpond_zcta_{wildcards.year}_yearly.db
        """

rule zcta_strat_counts_daily:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["zcta"]["daily"], "strat_sparse_counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "zcta_strat_counts_daily_{year}.log")
    params:
        out_dir=output_dirs["zcta"]["daily"],
    shell:
        """
        python src/08_zcta_strat_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=daily \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.zcta.daily={params.out_dir} \
            &> {log}

        rm -f duckpond_zcta_{wildcards.year}_daily.db
        """

rule county_counts_yearly:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["county"]["yearly"], "counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "county_counts_yearly_{year}.log")
    params:
        out_dir=output_dirs["county"]["yearly"],
    shell:
        """
        python src/09_county_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=yearly \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.county.yearly={params.out_dir} \
            &> {log}
        """

rule county_counts_daily:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["county"]["daily"], "counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "county_counts_daily_{year}.log")
    params:
        out_dir=output_dirs["county"]["daily"],
    shell:
        """
        python src/09_county_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=daily \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.county.daily={params.out_dir} \
            &> {log}
        """

rule county_strat_counts_yearly:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["county"]["yearly"], "strat_counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "county_strat_counts_yearly_{year}.log")
    params:
        out_dir=output_dirs["county"]["yearly"],
    shell:
        """
        python src/10_county_strat_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=yearly \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.county.yearly={params.out_dir} \
            &> {log}

        rm -f duckpond_county_{wildcards.year}_yearly.db
        """

rule county_strat_counts_daily:
    input:
        denom=os.path.join(denom_dir, "denom_{year}.parquet"),
    output:
        out=os.path.join(output_dirs["county"]["daily"], "strat_sparse_counts_{year}.parquet"),
    log:
        os.path.join(log_dir, "county_strat_counts_daily_{year}.log")
    params:
        out_dir=output_dirs["county"]["daily"],
    shell:
        """
        python src/10_county_strat_counts.py \
            run.year={wildcards.year} \
            run.temporal_frequency=daily \
            run.output_file={output.out} \
            datapaths.dirs.output.denom={denom_dir} \
            datapaths.dirs.output.county.daily={params.out_dir} \
            &> {log}

        rm -f duckpond_county_{wildcards.year}_daily.db
        """

rule qc_report:
    input:
        parquet_files=[p for p in all_targets if p.endswith(".parquet")]
    output:
        report=qc_report_file,
    log:
        os.path.join(log_dir, "qc_report.log")
    shell:
        """
        mkdir -p {qc_report_dir}

        python src/11_qc_report.py \
            &> {log}
        """