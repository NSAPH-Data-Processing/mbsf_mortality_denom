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

    year = int(cfg.run.year)

    denom_prefix = f"{cfg.datapaths.dirs.intermediate.denom}/denom"
    output_prefix = f"{cfg.datapaths.dirs.output.denom}/denom"
    zip2zcta_prefix = f"{cfg.datapaths.dirs.input.zip2zcta}/xwalk/zip2zcta_master_xwalk.parquet"
    fips2abbr = f"{cfg.datapaths.dirs.input.us_geoboundaries__census}/fips2abbr/fips_xwalk.parquet"
    zcta2county_prefix = f"{cfg.datapaths.dirs.input.us_geoboundaries__census}/us_xwalks__census/zcta2county_yearly/us_xwalks__census__zcta2county_yearly_"

    conn = duckdb.connect(f"{output_prefix}_{year}.db")

    LOGGER.info("## Incorporating zip2zcta ----")
    conn.execute(f"""
        CREATE OR REPLACE TABLE denom_zcta AS 
        SELECT * EXCLUDE (zip, zip_denom) FROM (
            SELECT d.*, z.zcta
            FROM (
                SELECT *, LPAD(zip, 5, '0') AS zip_denom
                FROM read_parquet('{denom_prefix}_{year}.parquet')
                WHERE zip IS NOT NULL and zip != '0') d
            LEFT JOIN (
                SELECT zip AS zip_xwalk, zcta
                FROM read_parquet('{zip2zcta_prefix}')) z
            ON d.zip_denom = z.zip_xwalk
        WHERE zcta is NOT NULL)
    """)

    LOGGER.info("## Incorporating county & state & Pad county code to 5 digits----")
    conn.execute("""
        CREATE OR REPLACE TABLE denom_county AS 
        SELECT * EXCLUDE(county),
            CASE WHEN county IS NULL THEN NULL 
            ELSE LPAD(county, 5, '0') 
        END AS county_denom
        FROM denom_zcta
    """)

    LOGGER.info("## Load zcta2county xwalk----")
    conn.execute(f"""
        CREATE OR REPLACE TABLE zcta2county AS 
        SELECT 
            zcta AS zcta_xwalk,
            county AS county_xwalk,
            county_state AS state_xwalk,
            pop_pct
        FROM read_parquet('{zcta2county_prefix}{year}.parquet')
    """)

    LOGGER.info("## Get distinct counties per ZCTA")
    conn.execute("""
        CREATE OR REPLACE TABLE distinct_counties_by_zcta AS 
        SELECT zcta_xwalk, LIST(DISTINCT county_xwalk) AS county_xwalk_set 
        FROM zcta2county
        GROUP BY zcta_xwalk
    """)

    LOGGER.info("## Identify top (most populous) county and state for each ZCTA")
    conn.execute("""
        CREATE OR REPLACE TABLE top_county_per_zcta AS
        SELECT zcta_xwalk, county_xwalk AS top_county, state_xwalk AS top_state
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY zcta_xwalk ORDER BY pop_pct DESC) AS rn
            FROM zcta2county)
        WHERE rn = 1
    """)

    LOGGER.info("## Add initial state using direct zcta & county match---")
    conn.execute("""
        CREATE OR REPLACE TABLE denom_state AS 
        SELECT d.*, z.state_xwalk AS state_initial
        FROM denom_county d
        LEFT JOIN zcta2county z
        ON d.zcta = z.zcta_xwalk AND d.county_denom = z.county_xwalk
    """)

    LOGGER.info("## Validate county_denom among zcta2county mappings")
    conn.execute("""
        CREATE OR REPLACE TABLE county_validation AS
        SELECT d.bene_id,
            d.zcta,
            d.county_denom,
            s.county_xwalk_set,
            CASE 
                WHEN s.county_xwalk_set IS NOT NULL AND ARRAY_CONTAINS(s.county_xwalk_set, d.county_denom)
                    THEN TRUE 
                ELSE FALSE 
            END AS valid_mapping
        FROM denom_state d
        LEFT JOIN distinct_counties_by_zcta s ON d.zcta = s.zcta_xwalk
    """)

    LOGGER.info("## Identify unmatched beneficiaries with invalid county mapping")
    conn.execute("""
        CREATE OR REPLACE TABLE unmatched_bene_ids AS
        SELECT bene_id
        FROM county_validation
        WHERE NOT valid_mapping
    """)

    LOGGER.info("## Replace unmatched counties using top county per ZCTA")
    conn.execute("""
        CREATE OR REPLACE TABLE denom_county_state AS
        SELECT d.* EXCLUDE(county_denom, state_initial),
            CASE 
                WHEN u.bene_id IS NOT NULL THEN COALESCE(t.top_county, d.county_denom)
                ELSE d.county_denom
            END AS county,
            CASE 
                WHEN u.bene_id IS NOT NULL THEN COALESCE(t.top_state, d.state_initial)
                ELSE d.state_initial
            END AS state
        FROM denom_state d
        LEFT JOIN unmatched_bene_ids u ON d.bene_id = u.bene_id
        LEFT JOIN top_county_per_zcta t ON d.zcta = t.zcta_xwalk
    """)

    LOGGER.info("## Load FIPS to State Abbreviations Mapping----")
    conn.execute(f"""
        CREATE OR REPLACE TABLE state_fips AS 
        SELECT CAST(state_fips AS VARCHAR) AS state_fips,
               state_abb,
               state_name 
        FROM read_parquet('{fips2abbr}')
    """)

    LOGGER.info("## Map state FIPS to State Abbreviation ----")
    conn.execute("""
        CREATE OR REPLACE TABLE denom AS
        SELECT d.* EXCLUDE(state), s.state_abb as state
        FROM denom_county_state d
        LEFT JOIN state_fips s
        ON d.state = s.state_fips
        WHERE s.state_abb NOT IN ('GU', 'MH', 'MP', 'VI', 'AS')
    """)

    LOGGER.info("## Count how many rows used fallback (top county per ZCTA)")
    replaced_count = conn.execute("SELECT COUNT(*) FROM unmatched_bene_ids").fetchone()[0]
    LOGGER.info(f"## Number of rows replaced using most populous county/state: {replaced_count}")

    LOGGER.info("## Count number of rows in denom")
    row_count = conn.execute("SELECT COUNT(*) FROM denom").fetchone()[0]
    LOGGER.info(f"## Final number of rows in denom: {row_count}")

    LOGGER.info("## Writing output denom ----")
    output_file = f"{output_prefix}_{year}.parquet"
    conn.execute(f"COPY denom TO '{output_file}' (FORMAT PARQUET)")
    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()


if __name__ == "__main__":
    main()