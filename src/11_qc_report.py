import json
import os
from pathlib import Path

import duckdb
import hydra
import matplotlib.pyplot as plt
import pandas as pd
from omegaconf import DictConfig, ListConfig


METRICS = [
    "unique_beneficiaries",
    "death_count",
    "zcta_count",
    "county_count",
    "unique_bene_hmo",
    "unique_bene_buyin",
    "unique_bene_dual",
]

METRIC_LABELS = {
    "unique_beneficiaries": "Unique beneficiaries",
    "death_count": "Death count",
    "zcta_count": "ZCTA counts",
    "county_count": "County counts",
    "unique_bene_hmo": "Unique beneficiaries on HMO",
    "unique_bene_buyin": "Unique beneficiaries on Buy-in",
    "unique_bene_dual": "Unique beneficiaries on Dual",
}


def safe_exists(path):
    return bool(path) and os.path.exists(path)


def parquet_exists(path):
    return safe_exists(path) and str(path).endswith(".parquet")


def query_scalar(con, query):
    try:
        val = con.execute(query).fetchone()[0]
        if val is None:
            return None
        if isinstance(val, float):
            return float(val)
        return int(val)
    except Exception:
        return None


def describe_columns(con, path):
    try:
        return con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).df()["column_name"].tolist()
    except Exception:
        return []


def find_col(cols, candidates):
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def get_years(cfg: DictConfig):
    year_value = cfg.run.year

    if year_value is None:
        return list(range(int(cfg.run.year_start), int(cfg.run.year_end) + 1))

    if isinstance(year_value, ListConfig):
        return [int(y) for y in year_value]

    if isinstance(year_value, (list, tuple)):
        return [int(y) for y in year_value]

    return [int(year_value)]


def build_paths(cfg: DictConfig):
    dirs = cfg.datapaths.dirs
    return {
        "denom_dir": str(dirs.output.denom),
        "zcta_yearly_dir": str(dirs.output.zcta.yearly),
        "county_yearly_dir": str(dirs.output.county.yearly),
    }


def collect_year_metrics(con, year, paths):
    denom_path = os.path.join(paths["denom_dir"], f"denom_{year}.parquet")
    zcta_path = os.path.join(paths["zcta_yearly_dir"], f"counts_{year}.parquet")
    county_path = os.path.join(paths["county_yearly_dir"], f"counts_{year}.parquet")

    record = {
        "year": year,
        "files": {
            "denom": denom_path if safe_exists(denom_path) else None,
            "zcta_yearly_counts": zcta_path if safe_exists(zcta_path) else None,
            "county_yearly_counts": county_path if safe_exists(county_path) else None,
        },
        "metrics": {
            "unique_beneficiaries": None,
            "death_count": None,
            "zcta_count": None,
            "county_count": None,
            "unique_bene_hmo": None,
            "unique_bene_buyin": None,
            "unique_bene_dual": None,
        },
    }

    if parquet_exists(denom_path):
        cols = describe_columns(con, denom_path)

        bene_col = find_col(cols, ["bene_id", "BENE_ID"])
        if bene_col:
            record["metrics"]["unique_beneficiaries"] = query_scalar(
                con,
                f"SELECT COUNT(DISTINCT {bene_col}) FROM read_parquet('{denom_path}')",
            )

        dod_col = find_col(cols, ["dod"])
        if dod_col:
            record["metrics"]["death_count"] = query_scalar(
                con,
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{denom_path}')
                WHERE {dod_col} IS NOT NULL
                  AND EXTRACT(YEAR FROM {dod_col}) = {year}
                """,
            )

        hmo_col = find_col(cols, ["hmo"])
        if bene_col and hmo_col:
            record["metrics"]["unique_bene_hmo"] = query_scalar(
                con,
                f"""
                SELECT COUNT(DISTINCT {bene_col})
                FROM read_parquet('{denom_path}')
                WHERE {hmo_col} = 1
                """,
            )

        buyin_col = find_col(cols, ["buyin"])
        if bene_col and buyin_col:
            record["metrics"]["unique_bene_buyin"] = query_scalar(
                con,
                f"""
                SELECT COUNT(DISTINCT {bene_col})
                FROM read_parquet('{denom_path}')
                WHERE {buyin_col} = 1
                """,
            )

        dual_col = find_col(cols, ["dual"])
        if bene_col and dual_col:
            record["metrics"]["unique_bene_dual"] = query_scalar(
                con,
                f"""
                SELECT COUNT(DISTINCT {bene_col})
                FROM read_parquet('{denom_path}')
                WHERE {dual_col} = 1
                """,
            )

    if parquet_exists(zcta_path):
        cols = describe_columns(con, zcta_path)
        zcta_col = find_col(cols, ["zcta", "ZCTA", "zcta5ce10"])
        if zcta_col:
            record["metrics"]["zcta_count"] = query_scalar(
                con,
                f"SELECT COUNT(DISTINCT {zcta_col}) FROM read_parquet('{zcta_path}')",
            )
        else:
            record["metrics"]["zcta_count"] = query_scalar(
                con,
                f"SELECT COUNT(*) FROM read_parquet('{zcta_path}')",
            )

    if parquet_exists(county_path):
        cols = describe_columns(con, county_path)
        county_col = find_col(cols, ["county", "county_fips", "geoid", "GEOID"])
        if county_col:
            record["metrics"]["county_count"] = query_scalar(
                con,
                f"SELECT COUNT(DISTINCT {county_col}) FROM read_parquet('{county_path}')",
            )
        else:
            record["metrics"]["county_count"] = query_scalar(
                con,
                f"SELECT COUNT(*) FROM read_parquet('{county_path}')",
            )

    return record


def save_line_plot(df, metric, out_path):
    plot_df = df[["year", metric]].dropna().copy()
    if plot_df.empty:
        return None

    plt.figure(figsize=(8, 4.5))
    plt.plot(plot_df["year"], plot_df[metric], marker="o")
    plt.xlabel("Year")
    plt.ylabel(METRIC_LABELS[metric])
    plt.title(METRIC_LABELS[metric])
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path


def write_summary_md(out_path, df, plot_paths):
    lines = [
        "# QC Summary Report",
        "",
        "## Overview",
        "",
        f"- Years covered: {int(df['year'].min()) if not df.empty else 'NA'} to {int(df['year'].max()) if not df.empty else 'NA'}",
        f"- Total years with reports: {len(df)}",
        "",
        "## Metrics by year",
        "",
        "| Year | Unique Beneficiaries | Death Count | ZCTA Counts | County Counts | Unique Bene HMO | Unique Bene Buy-in | Unique Bene Dual |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for _, row in df.sort_values("year").iterrows():
        lines.append(
            f"| {int(row['year'])} | {row['unique_beneficiaries']} | {row['death_count']} | {row['zcta_count']} | {row['county_count']} | {row['unique_bene_hmo']} | {row['unique_bene_buyin']} | {row['unique_bene_dual']} |"
        )

    lines += ["", "## Top-level plots", ""]
    for metric in METRICS:
        path = plot_paths.get(metric)
        if path:
            lines.append(f"### {METRIC_LABELS[metric]}")
            lines.append("")
            lines.append(f"![]({Path(path).name})")
            lines.append("")

    Path(out_path).write_text("\n".join(lines))


@hydra.main(version_base=None, config_path="../conf", config_name="config")
def main(cfg: DictConfig):
    years = get_years(cfg)
    paths = build_paths(cfg)

    denom_dir = paths["denom_dir"]
    qc_dir = os.path.join(denom_dir, "qc_reports")
    os.makedirs(qc_dir, exist_ok=True)

    con = duckdb.connect()
    year_records = [collect_year_metrics(con, year, paths) for year in years]

    df = pd.DataFrame(
        [{"year": record["year"], **record["metrics"]} for record in year_records],
        columns=["year", *METRICS],
    ).sort_values("year")

    plot_paths = {}
    for metric in METRICS:
        plot_paths[metric] = save_line_plot(
            df, metric, os.path.join(qc_dir, f"{metric}.png")
        )

    summary_json = {
        "years": years,
        "qc_dir": qc_dir,
        "metrics": df.to_dict(orient="records"),
        "plots": {
            metric: os.path.basename(path) if path else None
            for metric, path in plot_paths.items()
        },
    }

    Path(os.path.join(qc_dir, "qc_summary.json")).write_text(
        json.dumps(summary_json, indent=2)
    )
    Path(os.path.join(qc_dir, "qc_report.json")).write_text(
        json.dumps(year_records, indent=2)
    )
    write_summary_md(os.path.join(qc_dir, "qc_summary.md"), df, plot_paths)

    con.close()
    print(f"QC outputs written to: {qc_dir}")