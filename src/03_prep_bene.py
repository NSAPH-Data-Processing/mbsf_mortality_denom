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

def prep_bene(year, bene_prefix, enroll_prefix):
    """
    Build a SQL query to prepare beneficiary demographic data for a specific year.
    The query:
    - Reads enrollment data files (e.g. enroll_prefix_*.parquet) for a speific year.
    - Extracts unique bene_ids from enrollment.
    - Joins those bene_ids with the full beneficiary dataset.

    The output includes beneficiaries who were enrolled in the given year,
    along with their demographic information.
    """

    LOGGER.info("Fetch beneficiaries for year %s", year)
    
    query = f"""
            WITH enroll AS (
                SELECT DISTINCT 
                    bene_id, 
                    year
                FROM read_parquet('{enroll_prefix}_*.parquet')
                WHERE year = {year}
            )
            SELECT 
                e.bene_id, 
                e.year,
                b.dob,
                EXTRACT(YEAR FROM b.dob) AS yob,
                b.dod,
                b.race,
                b.race_rti, 
                b.sex, 
                b.orec
            FROM 
                read_parquet('{bene_prefix}') AS b
            INNER JOIN enroll AS e
            USING (bene_id)
    """
    return query

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    year = cfg.run.year
    bene_prefix = f"{cfg.datapaths.dirs.input.normalization}/part-0.parquet"
    enroll_prefix = f"{cfg.datapaths.dirs.input.normalization}/enrollments"
    output_prefix = f"{cfg.datapaths.dirs.input.normalization}/beneficiaries"

    os.makedirs(os.path.dirname(output_prefix), exist_ok=True)

    conn = duckdb.connect()

    query = f"""
        COPY (
            {prep_bene(year, bene_prefix, enroll_prefix)}
        ) TO '{output_prefix}_{year}.parquet' (FORMAT PARQUET)
    """

    conn.execute(query)
    LOGGER.info("Output written to: %s_%s.parquet", output_prefix, year)

    conn.close()

if __name__ == '__main__':
    main()