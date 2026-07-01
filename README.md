[![](<https://img.shields.io/badge/Dataverse-10.7910/DVN/SYNPBS-orange>)](https://www.doi.org/10.7910/DVN/Y1WNU7)

# mbsf_mortality_denom

Pipeline to build the Medicare MBSF mortality denominator and derived counts at ZCTA and county levels.

## Medicare Data

In the United States, when individuals reach the age of 65, they become eligible for the Medicare federal health insurance program. Medicare also covers individuals under 65 if they have certain disabilities or End-Stage Renal Disease (ESRD) or Amyotrophic Lateral Sclerosis (ALS).

The Center for Medicare & Medicaid Services (CMS) is the federal agency, who is in charge of managing the Medicare and Medicaid data, makes predefined datasets available for purchase to researchers after they enter a Data usage agreement through [Research Data Assistance Center (ResDAC)](https://resdac.org/). A beneficiary-level data from CMS/Resdac is the [Master Beneficiary
Summary File (MBSF)](https://www.resdac.org/cms-data/files/mbsf-base).

Raw MBSF data contains basic demographic information of individuals, an indicator of death, and some information on their medicare enrollment.

We refer to the mortality denominator as the clean subset of MBSF health data that's representative of the older population. The subset is comprised of US elder population population and includes mortalilty; that is, individuals under 65 who qualified because of a disability are excluded. The raw MBSF data is previously cleaned and harmonized using the data pipelines in https://github.com/NSAPH-Data-Platform; specifically, the [Medicare data ingestion and processing pipeline](https://nsaph-data-platform.github.io/nsaph-platform-docs/common/cms/doc/pipeline/medicare.html). More details on data processing are found in the [Data Processing Section](#processing-rules) of this document.

## Overview

This repository processes harmonized CMS/ResDAC MBSF data into:

- yearly denominator files at beneficiary-year level,
- ZCTA-level counts (yearly and daily),
- county-level counts (yearly and daily),
- stratified versions of ZCTA and county counts.

The workflow is orchestrated with Snakemake and implemented as numbered Python steps in [src](src).

## Data scope

- **Time coverage:** 2000-2021 (default configured range)
- **Unit:** beneficiary-year (denominator outputs)
- **State coverage:** all U.S. states, excluding territories `GU`, `MH`, `MP`, and `VI`
- **Population focus:** Medicare beneficiaries meeting denominator inclusion criteria (elder-focused cohort)

## Pipeline steps

The workflow defined in [Snakefile](Snakefile) runs the following scripts:

1. [src/01_prep_enroll.py](src/01_prep_enroll.py) — prepare yearly enrollment parquet.
2. [src/02_bene_db.py](src/02_bene_db.py) — build consolidated beneficiary table.
3. [src/03_prep_bene.py](src/03_prep_bene.py) — prepare yearly beneficiary parquet.
4. [src/04_fix_enroll_zip.py](src/04_fix_enroll_zip.py) — clean/fix enrollment ZIP values.
5. [src/05_denom.py](src/05_denom.py) — create base mortality denominator for a year.
6. [src/06_zcta2county.py](src/06_zcta2county.py) — append geography mappings.
7. [src/07_zcta_counts.py](src/07_zcta_counts.py) — ZCTA counts (yearly/daily).
8. [src/08_zcta_strat_counts.py](src/08_zcta_strat_counts.py) — ZCTA stratified counts (yearly/daily).
9. [src/09_county_counts.py](src/09_county_counts.py) — county counts (yearly/daily).
10. [src/10_county_strat_counts.py](src/10_county_strat_counts.py) — county stratified counts (yearly/daily).

SLURM templates for each step are provided as matching `.sbatch` files in [jobs](jobs).

### Normalization stage details

The first four steps form the normalization layer that prepares clean, year-specific inputs for denominator construction. In [src/01_prep_enroll.py](src/01_prep_enroll.py), the workflow reads yearly enrollment parquet partitions, standardizes key enrollment columns, casts ZIP/HMO/buy-in/dual fields to consistent types, and replaces legacy beneficiary identifiers with cleaned IDs from the beneficiary crosswalk when a match exists. In [src/02_bene_db.py](src/02_bene_db.py), the pipeline builds a consolidated beneficiary table by grouping beneficiary records across all available source data, keeping the earliest date of birth, the latest date of death, aggregated demographic codes (`race`, `race_rti`, `sex`), and original reason for entitlement, while also using enrollment history to truncate implausible death dates at age 115 when needed.

That consolidated table is then converted into a year-specific beneficiary extract in [src/03_prep_bene.py](src/03_prep_bene.py), which keeps only beneficiaries observed in the selected enrollment year and joins back their demographic variables for that year’s processing. Finally, [src/04_fix_enroll_zip.py](src/04_fix_enroll_zip.py) performs enrollment ZIP cleanup by joining enrollment ZIPs to the ZIP→ZCTA crosswalk, using 2003 enrollment ZIPs as a reference for imputation, selecting the closest historical ZIP per beneficiary when the current ZIP is unmapped, and writing the corrected yearly enrollment parquet back for downstream denominator generation.

## Inputs and lineage

Primary inputs:

1. CMS/ResDAC MBSF files obtained under data use agreement.
2. Cleaned MBSF outputs from the NSAPH Medicare normalization and data cleaning pipeline.
3. ZIP/ZCTA and county crosswalk resources (including master ZIP→ZCTA mappings).

Internal data path notes are documented in:

- [mbsf_mortality_denom.yaml](conf/datapaths/mbsf_mortality_denom.yaml)

## Environment and datapaths setup

### 1) Activate environment

```bash
# activate environment
```

### 2) Create local data links / folder structure

This repo expects a local `data/` directory in the repo root that contains a per-dataset folder
(based on your Hydra `datapaths.name`):

```
data/<datapaths.name>/
  input  -> symlink to real data location
  output -> symlink or directory for parquet outputs
```

Create directories and symlinks from config:

```bash
python src/create_dir_paths datapaths=<dataset_datapaths_config>
```

Example datapaths config (`conf/datapaths/mbsf_mortality_denom.yaml`):

```yaml
name: mortality_mbsf_denom
dirs:
  input: "/path/to/cleaned/data"
  output: "/path/to/medicare/mbsf_mortality_denom/denom"
```

## Pipeline Configuration & Execution

### Configure Paths & Settings

Pipeline configuration is managed using **Hydra** under the [`conf/`](conf) directory.

### Key configuration files

- [`conf/config.yaml`](conf/config.yaml) — main pipeline configuration  
- [`conf/datapaths/datapaths.yaml`](conf/datapaths/datapaths.yaml) — base directory structure  
- [`conf/datapaths/mbsf_mortality_denom.yaml`](conf/datapaths/mbsf_mortality_denom.yaml) — dataset-specific paths  
- [`conf/snakemake.yaml`](conf/snakemake.yaml) — *(optional)* execution subset for Snakemake  

Before running, make sure input/output directories resolve correctly for your environment (local symlinks or direct absolute paths).

### Configuration design

This pipeline separates **pipeline definition** from **execution control**.

#### `config.yaml` — Full pipeline definition

Defines the complete parameter space:

- year range (`year_start`, `year_end`)
- geoboundaries (`county`, `zcta`)
- temporal frequencies (`yearly`, `daily`)

This file is the **single source of truth** and is used by all scripts.

#### `snakemake.yaml` — Execution subset (optional)

Defines a subset of the pipeline to run when using Snakemake:

- selected years  
- selected geographies  
- selected temporal frequencies  

This allows:
- running smaller subsets
- debugging specific components
- avoiding full pipeline runs

### How they work together

- By default, the pipeline uses **only `config.yaml`**
- If you want to use Snakemake-specific overrides, enable it in `config.yaml`:

```yaml
defaults:
  - _self_
  - datapaths: mbsf_mortality_denom
  - snakemake   # enable this
```

- To disable Snakemake overrides, simply comment it out:

```yaml
# - snakemake
```

When enabled:
- values from `snakemake.yaml` **override** the corresponding fields in `config.yaml`

### Key idea

- **`config.yaml` → what is possible**
- **`snakemake.yaml` → what to run**

## Running the Pipeline

### 1. Run with SLURM 

Each pipeline step has an sbatch script under [`jobs/`](jobs):

```bash
sbatch jobs/07_zcta_counts.sbatch
```

### 2. Run individual scripts (Hydra)

```bash
python src/07_zcta_counts.py \
  run.year=2018 \
  run.temporal_frequency=yearly
```

### 3. Run full workflow with Snakemake

Use the provided SLURM wrapper:

```bash
sbatch snakemake.sbatch
```

Or run locally:

```bash
snakemake --cores 4
```

Dry run:

```bash
snakemake -n
```

## Outputs

Representative outputs include:

- `denom_{year}.parquet`
- `zcta_yearly/counts_{year}.parquet`
- `zcta_yearly/strat_counts_{year}.parquet`
- `zcta_daily/counts_{year}.parquet`
- `zcta_daily/strat_sparse_counts_{year}.parquet`
- `county_yearly/counts_{year}.parquet`
- `county_yearly/strat_counts_{year}.parquet`
- `county_daily/counts_{year}.parquet`
- `county_daily/strat_sparse_counts_{year}.parquet`

Exact destinations are defined by the active datapath configuration.

