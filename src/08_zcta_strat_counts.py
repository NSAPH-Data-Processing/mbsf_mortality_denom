import duckdb
import logging
import os 
from omegaconf import DictConfig
import hydra

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def get_zcta_strat_counts_yearly(denom_prefix, year):
    return f"""
        WITH denom AS (
            SELECT
                bene_id,
                year,
                zcta,
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
                zcta,
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
            GROUP BY zcta, year, age_grp, sex, race, race_rti, dual, hmo, buyin
        )
        SELECT
            uz.zcta,
            uz.state,
            uz.year,
            s.age_grp,
            s.sex,
            s.race,
            s.race_rti,
            s.dual,
            s.hmo,
            s.buyin,
            COALESCE(a.n_bene, 0) AS n_bene,
            COALESCE(a.n_dead, 0) AS n_dead
        FROM unique_zcta uz
        CROSS JOIN all_strata s
        LEFT JOIN agg_counts a
            ON uz.zcta = a.zcta
            AND uz.year = a.year
            AND s.age_grp = a.age_grp
            AND s.sex = a.sex
            AND s.race = a.race
            AND s.race_rti = a.race_rti
            AND s.dual = a.dual
            AND s.hmo = a.hmo
            AND s.buyin = a.buyin
        ORDER BY uz.zcta, uz.state, uz.year, s.age_grp, s.sex, s.race, s.race_rti, s.dual, s.hmo, s.buyin
    """

def get_zcta_strat_counts_daily(denom_prefix, unique_zcta_prefix, year):
    return f"""
        WITH unique_zcta AS (
            SELECT DISTINCT zcta
            FROM '{unique_zcta_prefix}_{year}.parquet'
        ),
        denom AS (
            SELECT
                bene_id,
                zcta,
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
            FROM '{denom_prefix}_{year}.parquet'
            WHERE dod IS NOT NULL
              AND EXTRACT(YEAR FROM dod) = {year}
        )
        SELECT
            d.zcta,
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
        JOIN unique_zcta uz
            ON uz.zcta = d.zcta
        GROUP BY d.zcta, d.state, d.date, d.age_grp, d.sex, d.race, d.race_rti, d.dual, d.hmo, d.buyin
        ORDER BY d.zcta, d.date
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

def create_unique_zcta(conn, unique_zcta_prefix, fips2abbr, year):
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE unique_zcta AS
        SELECT
            uz.zcta,
            uz.year,
            fips.state_abb AS state
        FROM read_parquet('{unique_zcta_prefix}_{year}.parquet') AS uz
        LEFT JOIN read_parquet('{fips2abbr}') AS fips
            ON uz.county_state = fips.state_fips
    """)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    year = int(cfg.run.year)
    temporal_frequency = cfg.run.temporal_frequency
    output_file = cfg.run.get("output_file")

    if not output_file:
        if temporal_frequency == "yearly":
            output_file = os.path.join(
                cfg.datapaths.dirs.output.zcta.yearly,
                f"strat_counts_{year}.parquet",
            )
        elif temporal_frequency == "daily":
            output_file = os.path.join(
                cfg.datapaths.dirs.output.zcta.daily,
                f"strat_sparse_counts_{year}.parquet",
            )
        else:
            raise ValueError(f"Unsupported temporal_frequency: {temporal_frequency}")

    denom_prefix = f"{cfg.datapaths.dirs.output.denom}/denom"
    unique_zcta_prefix = (
        f"{cfg.datapaths.dirs.input.us_geoboundaries__census}"
        "/us_uniqueid__census/zcta_yearly/us_uniqueid__census__zcta_yearly_"
    )
    fips2abbr = (
        f"{cfg.datapaths.dirs.input.us_geoboundaries__census}"
        "/fips2abbr/fips_xwalk.parquet"
    )

    conn = duckdb.connect(f"duckpond_zcta_{year}_{temporal_frequency}.db")

    LOGGER.info(f"## Preparing {temporal_frequency} strat counts ----")

    if temporal_frequency == "yearly":
        create_all_strata(conn)
        create_unique_zcta(conn, unique_zcta_prefix, fips2abbr, year)
        query = get_zcta_strat_counts_yearly(denom_prefix, year)
    elif temporal_frequency == "daily":
        query = get_zcta_strat_counts_daily(denom_prefix, unique_zcta_prefix, year)
    else:
        raise ValueError(f"Unsupported temporal_frequency: {temporal_frequency}")

    conn.execute(f"CREATE TABLE zcta_strat_counts AS {query}")
    zcta_strat_counts = conn.table("zcta_strat_counts")

    LOGGER.info(f"zcta_strat_counts rows: {zcta_strat_counts.count('*').fetchone()}")
    LOGGER.info(f"zcta_strat_counts head: {zcta_strat_counts.limit(5).fetchdf()}")

    zcta_strat_counts.write_parquet(output_file)
    LOGGER.info(f"Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    main()
