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

def get_enroll(year, enroll_prefix, bene_xwalk):
    """
    Build SQL query to:
    - Load enrollment data for given year from partitioned Parquet files.
    - Replace enrollment bene_id with cleaned bene_id from crosswalk (if matched).
    - Select relevant enrollment columns for output.
    """
    LOGGER.info(f"##Constructing enrollment query with bene_id replacement----")
    query = f"""
            SELECT 
                COALESCE(b.bene_id,e.bene_id) AS bene_id,
                e.year,
                CAST(e.zip AS CHAR) AS zip,
                e.fips5 AS county,
                e.curec,
                CAST(e.hmo AS INTEGER) AS hmo,
                e.hmo_indicators,
                e.hmo_cvg_count,
                CAST(e.buyin AS INTEGER) AS buyin,
                e.buyin_indicators,
                e.buyin_cvg_count,
                CAST(e.dual AS INTEGER) AS dual,
                e.dual_indicators,
                e.dual_cvg_count
            FROM read_parquet('{enroll_prefix}/year={year}/*.parquet', hive_partitioning=true) e
            LEFT JOIN read_parquet('{bene_xwalk}') b
                ON e.bene_id = b.bid
            WHERE e.year = {year}
    """
    return query

def run_prep_enroll(year, enroll_prefix, bene_xwalk, normalization_dir):
    os.makedirs(normalization_dir, exist_ok=True)

    conn = duckdb.connect()
    output_file = f"{normalization_dir}/enrollments_{year}.parquet"

    query = f"""
        COPY (
            {get_enroll(year, enroll_prefix, bene_xwalk)}
        ) TO '{output_file}' 
        (FORMAT PARQUET);
    """
    LOGGER.info(f"Running DuckDB query for year {year}")    
    conn.execute(query)
    LOGGER.info(f"Output written to: {output_file}")
    conn.close()

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    run_prep_enroll(
        year=int(cfg.run.year),
        enroll_prefix=cfg.datapaths.dirs.input.medicare.enrollments,
        bene_xwalk=os.path.join(cfg.datapaths.dirs.input.bene_xwalk,"bene_xwalk.parquet"),
        normalization_dir=cfg.datapaths.dirs.input.normalization,
    )

if __name__ == '__main__':
    main()