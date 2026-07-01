import duckdb
import logging
from omegaconf import DictConfig
import hydra

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    conn = duckdb.connect()

    zip2zcta_prefix = f"{cfg.datapaths.dirs.input.zip2zcta}/xwalk/zip2zcta_master_xwalk.parquet"
    enroll2003 = f"{cfg.datapaths.dirs.input.normalization}/enrollments_2003.parquet"
    enroll_prefix = f"{cfg.datapaths.dirs.input.normalization}/enrollments"
    year = int(cfg.run.year)

    LOGGER.info("Loading zip2zcta crosswalk...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE zip2zcta AS
        SELECT zip, zcta
        FROM read_parquet('{zip2zcta_prefix}')
    """)

    LOGGER.info("Loading 2003 enrollment ZIPs for imputation...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE df_2003_raw AS
        SELECT bene_id, zip
        FROM read_parquet('{enroll2003}')
    """)

    shape_2003_raw = conn.execute('SELECT COUNT(*) FROM df_2003_raw').fetchone()[0]
    LOGGER.info(f"df_2003_raw count before join: {shape_2003_raw}")

    conn.execute("""
        CREATE OR REPLACE TABLE df_2003 AS
        SELECT d.bene_id, d.zip AS zip_2003, z.zcta
        FROM df_2003_raw d
        LEFT JOIN zip2zcta z
            ON d.zip = z.zip
    """)

    shape_2003_after_join = conn.execute('SELECT COUNT(*) FROM df_2003').fetchone()[0]
    LOGGER.info(f"df_2003 count after join with zip2zcta: {shape_2003_after_join}")

    df_dup_2003 = conn.execute("""
        SELECT COUNT(DISTINCT bene_id) AS duplicate_bene_count
        FROM (
            SELECT bene_id
            FROM df_2003
            GROUP BY bene_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()

    duplicate_bene_count = df_dup_2003[0]
    LOGGER.info(f"Found {duplicate_bene_count} duplicated bene_id(s) in df_2003.")

    LOGGER.info(f"Processing year {year}...")

    LOGGER.info(f"Joining ZIP and ZCTA for {year}...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE df AS
        SELECT d.*, z.zcta
        FROM read_parquet('{enroll_prefix}_{year}.parquet') AS d
        LEFT JOIN zip2zcta z
            ON d.zip = z.zip
    """)

    LOGGER.info(f"shape df: {conn.execute('SELECT COUNT(*) FROM df').fetchone()[0]}")

    LOGGER.info(f"Joining with 2003 ZIPs for {year}...")
    conn.execute("""
        CREATE OR REPLACE TABLE df_join AS
        SELECT
            d.*,
            df_2003.zip_2003,
            ABS(CAST(d.zip AS INTEGER) - CAST(df_2003.zip_2003 AS INTEGER)) AS zip_delta
        FROM df AS d
        LEFT JOIN df_2003
            ON d.bene_id = df_2003.bene_id
    """)

    LOGGER.info(f"shape df after join: {conn.execute('SELECT COUNT(*) FROM df_join').fetchone()[0]}")

    LOGGER.info("Deduplicating df_join to keep closest zip_2003 per (bene_id, zip)...")
    conn.execute("""
        CREATE OR REPLACE TABLE df_join_dedup AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY bene_id, zip
                    ORDER BY zip_delta ASC NULLS LAST
                ) AS rn
            FROM df_join
        )
        WHERE rn = 1
    """)

    LOGGER.info(f"shape df after deduplicating df_join: {conn.execute('SELECT COUNT(*) FROM df_join_dedup').fetchone()[0]}")

    LOGGER.info(f"Applying ZIP imputation logic for {year}...")
    conn.execute("""
        CREATE OR REPLACE TABLE df_final AS
        SELECT
            d.* EXCLUDE(zip, zcta, zip_delta, zip_2003, rn),
            CASE
                WHEN d.zcta IS NOT NULL THEN CAST(d.zip AS CHAR)
                WHEN d.zcta IS NULL AND d.zip_2003 IS NOT NULL AND d.zip_delta <= 100 THEN CAST(d.zip_2003 AS CHAR)
                ELSE CAST(d.zip AS CHAR)
            END AS zip
        FROM df_join_dedup AS d
    """)

    LOGGER.info(f"shape df final: {conn.execute('SELECT COUNT(*) FROM df_final').fetchone()[0]}")

    LOGGER.info("Checking imputation success...")
    result = conn.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (
                WHERE zcta IS NULL
                AND zip_2003 IS NOT NULL
                AND zip_delta IS NOT NULL
                AND zip_delta <= 100
            ) AS imputed_zip_count
        FROM df_join_dedup
    """).fetchdf()

    # total = result["total"].iloc[0]
    # imputed = result["imputed_zip_count"].iloc[0]
    # LOGGER.info(f"ZIP imputation summary for {year}: {imputed} / {total} rows updated ({imputed/total:.2%})")

    output_path = f"{enroll_prefix}_{year}.parquet"
    LOGGER.info(f"Saving final output for {year} to {output_path}...")
    conn.execute(f"""COPY df_final TO '{output_path}' (FORMAT 'parquet')""")

    conn.close()
    LOGGER.info("Done.")

if __name__ == '__main__':
    main() 