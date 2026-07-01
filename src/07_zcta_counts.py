import duckdb
import logging
import os
from omegaconf import DictConfig
import hydra

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

def get_zcta_counts_yearly(denom_prefix, year, unique_zcta_prefix, fips2abbr):
    query = f"""
        WITH denom AS (
            SELECT
                bene_id,
                zcta,
                year,
                sex,
                race,
                race_rti,
                age_dob AS age,
                CASE
                    WHEN EXTRACT(YEAR FROM dod) = year THEN 1
                    ELSE 0
                END AS dead,
                dual,
                hmo,
                buyin
            FROM '{denom_prefix}_{year}.parquet'
        ),
        summary_table AS (
            SELECT
                d.zcta,
                d.year,
                AVG(CASE WHEN d.sex = 2 THEN 1.0 ELSE 0 END) AS female_pct,
                AVG(CASE WHEN d.sex = 1 THEN 1.0 ELSE 0 END) AS male_pct,
                AVG(d.age) AS mean_age,
                AVG(CASE WHEN d.age BETWEEN 65 AND 75 THEN 1.0 ELSE 0 END) AS age_65_75_pct,
                AVG(CASE WHEN d.age BETWEEN 76 AND 85 THEN 1.0 ELSE 0 END) AS age_76_85_pct,
                AVG(CASE WHEN d.age BETWEEN 86 AND 95 THEN 1.0 ELSE 0 END) AS age_86_95_pct,
                AVG(CASE WHEN d.age BETWEEN 96 AND 105 THEN 1.0 ELSE 0 END) AS age_96_105_pct,
                AVG(CASE WHEN d.age BETWEEN 106 AND 115 THEN 1.0 ELSE 0 END) AS age_106_115_pct,
                AVG(CASE WHEN d.race = 1 THEN 1.0 ELSE 0 END) AS race_white_pct,
                AVG(CASE WHEN d.race = 2 THEN 1.0 ELSE 0 END) AS race_black_pct,
                AVG(CASE WHEN d.race = 4 THEN 1.0 ELSE 0 END) AS race_asian_pct,
                AVG(CASE WHEN d.race = 5 THEN 1.0 ELSE 0 END) AS race_hispanic_pct,
                AVG(CASE WHEN d.race_rti = '0' THEN 1.0 ELSE 0 END) AS race_rti_unknown_pct,
                AVG(CASE WHEN d.race_rti = '1' THEN 1.0 ELSE 0 END) AS race_rti_white_pct,
                AVG(CASE WHEN d.race_rti = '2' THEN 1.0 ELSE 0 END) AS race_rti_black_pct,
                AVG(CASE WHEN d.race_rti = '3' THEN 1.0 ELSE 0 END) AS race_rti_other_pct,
                AVG(CASE WHEN d.race_rti = '4' THEN 1.0 ELSE 0 END) AS race_rti_asian_pct,
                AVG(CASE WHEN d.race_rti = '5' THEN 1.0 ELSE 0 END) AS race_rti_hispanic_pct,
                AVG(CASE WHEN d.race_rti = '6' THEN 1.0 ELSE 0 END) AS race_rti_ai_an_pct,
                COUNT(d.bene_id) AS n_bene,
                SUM(d.dead) AS n_dead,
                SUM(d.dead) / COUNT(d.bene_id) AS death_rate,
                AVG(CASE WHEN d.dual = 1 THEN 1.0 ELSE 0 END) AS dual_pct,
                AVG(CASE WHEN d.hmo = 1 THEN 1.0 ELSE 0 END) AS hmo_pct,
                AVG(CASE WHEN d.buyin = 1 THEN 1.0 ELSE 0 END) AS buyin_pct
            FROM denom d
            GROUP BY d.zcta, d.year
        ),
        zcta_mapped AS (
            SELECT
                uz.zcta,
                uz.year,
                fips.state_abb AS state
            FROM read_parquet('{unique_zcta_prefix}_{year}.parquet') AS uz
            LEFT JOIN read_parquet('{fips2abbr}') AS fips
            ON uz.county_state = fips.state_fips
        )
        SELECT
            z.zcta,
            z.state,
            z.year,
            COALESCE(s.n_bene, 0) AS n_bene,
            COALESCE(s.n_dead, 0) AS n_dead,
            s.death_rate,
            s.female_pct,
            s.male_pct,
            s.dual_pct,
            s.hmo_pct,
            s.buyin_pct,
            s.mean_age,
            s.age_65_75_pct,
            s.age_76_85_pct,
            s.age_86_95_pct,
            s.age_96_105_pct,
            s.age_106_115_pct,
            s.race_white_pct,
            s.race_black_pct,
            s.race_hispanic_pct,
            s.race_asian_pct,
            s.race_rti_unknown_pct,
            s.race_rti_white_pct,
            s.race_rti_black_pct,
            s.race_rti_other_pct,
            s.race_rti_asian_pct,
            s.race_rti_hispanic_pct,
            s.race_rti_ai_an_pct
        FROM zcta_mapped AS z
        FULL OUTER JOIN summary_table s
        ON z.zcta = s.zcta AND z.year = s.year
        WHERE z.zcta IS NOT NULL
    """
    return query

def get_zcta_counts_daily(denom_prefix, year, unique_zcta_prefix, fips2abbr):
    query = f"""
        WITH denom AS (
            SELECT
                bene_id,
                zcta,
                dod AS date,
                year
            FROM '{denom_prefix}_{year}.parquet'
            WHERE dod IS NOT NULL
                AND EXTRACT(YEAR FROM dod) = {year}
        ),
        summary_table AS (
            SELECT
                d.zcta,
                d.date,
                d.year,
                COUNT(d.bene_id) AS n_dead
            FROM denom d
            GROUP BY d.zcta, d.date, d.year
        ),
        unique_zcta AS (
            SELECT zcta, year, fips.state_abb AS state
            FROM read_parquet('{unique_zcta_prefix}_{year}.parquet') AS z
            LEFT JOIN read_parquet('{fips2abbr}') AS fips
            ON z.county_state = fips.state_fips
        ),
        all_dates AS (
            SELECT UNNEST(GENERATE_SERIES(DATE '{year}-01-01', DATE '{year}-12-31', INTERVAL '1 day'))::DATE AS date
        ),
        all_zcta_dates AS (
            SELECT z.zcta, z.state, d.date, z.year
            FROM unique_zcta z
            CROSS JOIN all_dates d
        )
        SELECT
            a.zcta,
            a.state,
            a.year,
            a.date,
            COALESCE(s.n_dead, 0) AS n_dead
        FROM all_zcta_dates a
        LEFT JOIN summary_table s
        ON a.zcta = s.zcta AND a.date = s.date AND a.year = s.year
        ORDER BY a.zcta, a.date
    """
    return query

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    year = int(cfg.run.year)
    temporal_frequency = cfg.run.temporal_frequency
    output_file = cfg.run.get("output_file")

    # Determine default output location if not explicitly provided
    if not output_file:
        if temporal_frequency == "yearly":
            output_file = os.path.join(
                cfg.datapaths.dirs.output.zcta.yearly,
                f"counts_{year}.parquet",
            )
        elif temporal_frequency == "daily":
            output_file = os.path.join(
                cfg.datapaths.dirs.output.zcta.daily,
                f"counts_{year}.parquet",
            )
        else:
            raise ValueError(f"Unsupported temporal_frequency: {temporal_frequency}")

    denom_prefix = f"{cfg.datapaths.dirs.output.denom}/denom"
    fips2abbr = (
        f"{cfg.datapaths.dirs.input.us_geoboundaries__census}"
        "/fips2abbr/fips_xwalk.parquet"
    )
    unique_zcta_prefix = (
        f"{cfg.datapaths.dirs.input.us_geoboundaries__census}"
        "/us_uniqueid__census/zcta_yearly/us_uniqueid__census__zcta_yearly_"
    )

    conn = duckdb.connect()

    LOGGER.info("Preparing %s counts", temporal_frequency)

    if temporal_frequency == "yearly":
        query = get_zcta_counts_yearly(
            denom_prefix,
            year,
            unique_zcta_prefix,
            fips2abbr,
        )
    elif temporal_frequency == "daily":
        query = get_zcta_counts_daily(
            denom_prefix,
            year,
            unique_zcta_prefix,
            fips2abbr,
        )
    else:
        raise ValueError(f"Unsupported temporal_frequency: {temporal_frequency}")

    conn.execute(f"CREATE OR REPLACE TABLE zcta_counts AS {query}")
    zcta_counts = conn.table("zcta_counts")

    LOGGER.info("zcta_counts rows: %s", zcta_counts.count("*").fetchone())
    LOGGER.info("zcta_counts head:\n%s", zcta_counts.limit(5).fetchdf())

    LOGGER.info("Writing output to %s", output_file)
    zcta_counts.write_parquet(output_file)

    conn.close()


if __name__ == "__main__":
    main()