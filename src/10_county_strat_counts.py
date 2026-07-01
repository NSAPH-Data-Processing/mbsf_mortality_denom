import duckdb
import logging
import os
from omegaconf import DictConfig
import hydra

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_county_strat_counts_yearly(denom_prefix, year):
    return f"""
        WITH denom AS (
            SELECT
                bene_id,
                year,
                county,
                CASE
                    WHEN yod = {year} THEN 1
                    ELSE 0
                END AS dead,
                CASE
                    WHEN age_dob BETWEEN 65 AND 75 THEN '[65,75]'
                    WHEN age_dob BETWEEN 76 AND 85 THEN '[76,85]'
                    WHEN age_dob BETWEEN 86 AND 95 THEN '[86,95]'
                    WHEN age_dob BETWEEN 96 AND 105 THEN '[96,105]'
                    WHEN age_dob BETWEEN 106 AND 115 THEN '[106,115]'
                    ELSE NULL
                END AS age_grp,
                sex,
                race,
                race_rti,
                dual,
                hmo,
                buyin
            FROM '{denom_prefix}_{year}.parquet'
        ),
        agg_counts AS (
            SELECT
                county,
                year,
                age_grp,
                sex,
                race,
                race_rti,
                dual,
                hmo,
                buyin,
                COUNT(bene_id) AS n_bene,
                SUM(dead) AS n_dead
            FROM denom
            GROUP BY county, year, age_grp, sex, race, race_rti, dual, hmo, buyin
        )
        SELECT
            uc.county,
            uc.state,
            uc.year,
            s.age_grp,
            s.sex,
            s.race,
            s.race_rti,
            s.dual,
            s.hmo,
            s.buyin,
            COALESCE(a.n_bene, 0) AS n_bene,
            COALESCE(a.n_dead, 0) AS n_dead
        FROM unique_county uc
        CROSS JOIN all_strata s
        LEFT JOIN agg_counts a
            ON uc.county = a.county
            AND uc.year = a.year
            AND s.age_grp = a.age_grp
            AND s.sex = a.sex
            AND s.race = a.race
            AND s.race_rti = a.race_rti
            AND s.dual = a.dual
            AND s.hmo = a.hmo
            AND s.buyin = a.buyin
        ORDER BY uc.county, uc.state, uc.year, s.age_grp, s.sex, s.race, s.race_rti, s.dual, s.hmo, s.buyin
    """

def get_county_strat_counts_daily(denom_prefix, unique_county_prefix, year):
    return f"""
        WITH unique_county AS (
            SELECT DISTINCT county
            FROM '{unique_county_prefix}_{year}.parquet'
        ),
        denom AS (
            SELECT
                bene_id,
                county,
                state,
                dod AS date,
                CASE
                    WHEN age_dob BETWEEN 65 AND 75 THEN '[65,75]'
                    WHEN age_dob BETWEEN 76 AND 85 THEN '[76,85]'
                    WHEN age_dob BETWEEN 86 AND 95 THEN '[86,95]'
                    WHEN age_dob BETWEEN 96 AND 105 THEN '[96,105]'
                    WHEN age_dob BETWEEN 106 AND 115 THEN '[106,115]'
                    ELSE NULL
                END AS age_grp,
                sex,
                race,
                race_rti,
                dual,
                hmo,
                buyin
            FROM '{denom_prefix}_{year}.parquet' d
            WHERE d.dod IS NOT NULL
              AND EXTRACT(YEAR FROM d.dod) = {year}
        )
        SELECT
            d.county,
            d.state,
            d.date,
            d.age_grp,
            d.sex,
            d.race,
            d.race_rti,
            d.dual,
            d.hmo,
            d.buyin,
            COUNT(d.bene_id) AS n_dead
        FROM denom d
        JOIN unique_county uc
            ON uc.county = d.county
        GROUP BY d.county, d.state, d.date, d.age_grp, d.sex, d.race, d.race_rti, d.dual, d.hmo, d.buyin
        ORDER BY d.county, d.date
    """

def create_all_strata(conn):
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE all_strata AS
        SELECT *
        FROM (VALUES
            ('[65,75]'), ('[76,85]'), ('[86,95]'), ('[96,105]'), ('[106,115]')
        ) AS age(age_grp)
        CROSS JOIN (VALUES ('0'), ('1'), ('2')) AS sexes(sex)
        CROSS JOIN (VALUES ('0'), ('1'), ('2'), ('3'), ('4'), ('5'), ('6')) AS races(race)
        CROSS JOIN (VALUES ('0'), ('1'), ('2'), ('3'), ('4'), ('5'), ('6')) AS races_rti(race_rti)
        CROSS JOIN (VALUES (0), (1)) AS duals(dual)
        CROSS JOIN (VALUES (0), (1)) AS buyins(buyin)
        CROSS JOIN (VALUES (0), (1)) AS hmos(hmo)
    """)

def create_unique_county(conn, unique_county_prefix, fips2abbr, year):
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE unique_county AS
        SELECT
            uc.county,
            uc.year,
            fips.state_abb AS state
        FROM read_parquet('{unique_county_prefix}_{year}.parquet') AS uc
        LEFT JOIN read_parquet('{fips2abbr}') AS fips
            ON uc.state = fips.state_fips
    """)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    year = int(cfg.run.year)
    temporal_frequency = cfg.run.temporal_frequency
    output_file = cfg.run.get("output_file")

    if not output_file:
        if temporal_frequency == "yearly":
            output_file = os.path.join(
                cfg.datapaths.dirs.output.county.yearly,
                f"strat_counts_{year}.parquet",
            )
        elif temporal_frequency == "daily":
            output_file = os.path.join(
                cfg.datapaths.dirs.output.county.daily,
                f"strat_sparse_counts_{year}.parquet",
            )
        else:
            raise ValueError(f"Unsupported temporal_frequency: {temporal_frequency}")

    denom_prefix = f"{cfg.datapaths.dirs.output.denom}/denom"

    conn = duckdb.connect(f"duckpond_county_{year}_{temporal_frequency}.db")

    LOGGER.info(f"## Preparing {temporal_frequency} county strat counts ----")

    if temporal_frequency == "yearly":
        query = get_county_strat_counts_yearly(denom_prefix, year)
    elif temporal_frequency == "daily":
        query = get_county_strat_counts_daily(denom_prefix, year)
    else:
        raise ValueError(f"Unsupported temporal_frequency: {temporal_frequency}")

    conn.execute(f"CREATE TABLE county_strat_counts AS {query}")
    county_strat_counts = conn.table("county_strat_counts")

    LOGGER.info(f"county_strat_counts rows: {county_strat_counts.count('*').fetchone()}")
    LOGGER.info(f"county_strat_counts head: {county_strat_counts.limit(5).fetchdf()}")

    county_strat_counts.write_parquet(output_file)
    LOGGER.info(f"Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    main()