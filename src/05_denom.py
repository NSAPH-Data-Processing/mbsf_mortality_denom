import duckdb
import logging
from omegaconf import DictConfig
import hydra

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_denom_query(year, bene_prefix, enroll_prefix):
    query = f"""
        WITH bene AS (
            SELECT 
                e.bene_id,
                e.year,
                e.zip,
                e.county,
                b.dob,
                b.dod,
                e.year - b.yob AS age_dob, 
                b.yob,
                EXTRACT(YEAR FROM b.dod) AS yod,
                CASE 
                    WHEN b.sex LIKE '%,%' THEN '0'
                    ELSE b.sex
                END AS sex_,
                CASE 
                    WHEN b.race LIKE '%,%' THEN '0'
                    ELSE b.race
                END AS race_,
                CASE 
                    WHEN b.race_rti LIKE '%,%' THEN '0'
                    WHEN b.race_rti = 'X' THEN '0'
                    WHEN b.race_rti IS NULL THEN '0'
                    ELSE b.race_rti
                END AS race_rti_,
                e.hmo,
                e.hmo_indicators, 
                e.hmo_cvg_count,
                COALESCE(e.dual, e.buyin) AS dual_,
                e.dual_indicators,
                e.dual_cvg_count,
                e.buyin,
                e.buyin_indicators,
                e.buyin_cvg_count,
                b.orec,
                e.curec
            FROM read_parquet('{enroll_prefix}_{year}.parquet') AS e
            INNER JOIN read_parquet('{bene_prefix}_{year}.parquet') AS b
            USING (bene_id)
        ) 
        SELECT 
            bene_id,
            year,
            zip,
            county,
            dob,
            age_dob,
            yob,
            dod,
            yod,
            sex_ as sex,
            race_ as race,
            race_rti_ as race_rti, 
            hmo,
            hmo_indicators,
            hmo_cvg_count,
            dual_ as dual,
            dual_indicators,
            dual_cvg_count,
            buyin,
            buyin_indicators,
            buyin_cvg_count,
            orec,
            curec
        FROM bene
        WHERE
        -- #### elder population criteria ####
            age_dob >= 65 AND age_dob <= 115
        -- #### Drop the records (the observations seen after the bene died) ####
            AND (year <= yod OR yod IS NULL)
        ORDER BY bene_id, zip
    """
    return query

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):

    year = int(cfg.run.year)

    bene_prefix = f"{cfg.datapaths.dirs.input.normalization}/beneficiaries"
    enroll_prefix = f"{cfg.datapaths.dirs.input.normalization}/enrollments"
    output_prefix = f"{cfg.datapaths.dirs.intermediate.denom}/denom"

    conn = duckdb.connect()

    LOGGER.info("## Preparing denom ----")
    query = get_denom_query(year, bene_prefix, enroll_prefix)

    conn.execute(f"CREATE OR REPLACE TABLE denom AS {query}")
    denom_tbl = conn.table("denom")

    LOGGER.info(f"Intermediate denom nrows: {denom_tbl.count('*').fetchone()}")
    LOGGER.info(f"Intermediate denom head: \n{denom_tbl.limit(5).fetchdf()}")

    LOGGER.info("## Drop duplicates ----")

    conn.execute("""
        CREATE OR REPLACE TABLE denom AS
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY bene_id, year, dob, age_dob, yob, yod, sex, race, race_rti
                ) AS rn
            FROM denom
        )
        WHERE rn = 1
    """)

    denom_tbl = conn.table("denom")

    LOGGER.info(f"Output denom nrows: {denom_tbl.count('*').fetchone()}")
    LOGGER.info(f"Output denom head: {denom_tbl.limit(5).fetchdf()}")

    LOGGER.info("## Writing output denom ----")

    output_file = f"{output_prefix}_{year}.parquet"
    denom_tbl.write_parquet(output_file)

    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    main()

