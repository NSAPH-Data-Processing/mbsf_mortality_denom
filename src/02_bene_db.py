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

def get_bene_db(bene_prefix, bene_xwalk, enroll_prefix):
    """
    Build a SQL query to:
    - Load beneficiary data from dorieh/medicare part-0.
    - Replace original bene_id using the crosswalk.
    - Aggregate demographic fields (DOB, DOD, race, sex, etc.) by bene_id.
    - Fix dod for bene's age 115 
    """

    query = f"""
        WITH bene_raw AS (
            SELECT 
                COALESCE(x.bene_id, b.bene_id) AS bene_id,
                MIN(b.dob) AS dob,
                EXTRACT(YEAR FROM MIN(b.dob)) AS yob,
                MAX(b.dod) AS dod_,
                STRING_AGG(DISTINCT b.race, ',') AS race,
                STRING_AGG(DISTINCT b.race_rti, ',') AS race_rti,
                STRING_AGG(DISTINCT b.sex, ',') AS sex,
                MAX(b.orec) AS orec
            FROM read_parquet('{bene_prefix}') b
            LEFT JOIN read_parquet('{bene_xwalk}') x
                ON b.bene_id = x.bid
            GROUP BY COALESCE(x.bene_id, b.bene_id)
        ),
        enroll_summary AS (
            SELECT 
                bene_id, 
                MAX(year) AS last_enrollment_year
            FROM read_parquet('{enroll_prefix}_*.parquet')
            GROUP BY bene_id
        ),
        bene_unified AS (
            SELECT
            br.bene_id,
            br.dob,
            br.yob,
            br.dod_,
            e.last_enrollment_year,
            br.race,
            br.race_rti,
            br.sex,
            br.orec
        FROM bene_raw AS br
        LEFT JOIN enroll_summary AS e
            ON br.bene_id = e.bene_id
        )
        SELECT
            bene_id,
            dob,
            yob,
            CASE
                -- Has DOD and age > 115 → truncate to Dec 31 of 115th year
                WHEN dod_ IS NOT NULL
                    AND DATE_DIFF('year', dob, dod_) > 115
                THEN MAKE_DATE(yob + 115, 12, 31)
                -- No DOD, but we actually see them in/after their 115th year
                WHEN dod_ IS NULL
                    AND last_enrollment_year IS NOT NULL
                    AND last_enrollment_year >= yob + 115
                THEN MAKE_DATE(yob + 115, 12, 31)
                ELSE dod_
            END AS dod,
            race,
            race_rti,
            sex,
            orec
        FROM bene_unified
    """
    return query 

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    bene_prefix = f"{cfg.datapaths.dirs.input.medicare.beneficiaries}/part-0.parquet"
    bene_xwalk = os.path.join(cfg.datapaths.dirs.input.bene_xwalk,"bene_xwalk.parquet")
    enroll_prefix = f"{cfg.datapaths.dirs.input.normalization}/enrollments"
    output_prefix = f"{cfg.datapaths.dirs.input.normalization}/part-0"

    os.makedirs(os.path.dirname(output_prefix), exist_ok=True)

    conn = duckdb.connect()
    query = f"""
        COPY (
            {get_bene_db(bene_prefix, bene_xwalk, enroll_prefix)}
        ) TO '{output_prefix}.parquet'
        (FORMAT PARQUET)
    """
    conn.execute(query)
    LOGGER.info(f"Output written to: {output_prefix}.parquet")
    conn.close()

if __name__ == '__main__':
    main()