import duckdb
import argparse
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_bid2bene_id(conn, bene_xwalk):
    """Process BID to BENE_ID crosswalk data with deduplication and cleaning."""
    
    LOGGER.info("Dropping and creating final result tables.")
    conn.execute("""
        DROP TABLE IF EXISTS bid_to_bene_id;
        CREATE TABLE bid_to_bene_id AS 
        SELECT * FROM (SELECT NULL::VARCHAR AS BID, NULL::VARCHAR AS BENE_ID) WHERE FALSE;

        DROP TABLE IF EXISTS bid_to_bene_id_failed;
        CREATE TABLE bid_to_bene_id_failed AS 
        SELECT * FROM (SELECT NULL::VARCHAR AS BID, NULL::VARCHAR AS BENE_ID, NULL::VARCHAR AS REASON) WHERE FALSE;
    """)

    LOGGER.info("Loading and cleaning BID column from parquet file.")
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW input_data AS
        SELECT
            CASE
                WHEN TRIM(CAST(BID AS VARCHAR)) = '' THEN NULL
                ELSE LPAD(TRIM(CAST(BID AS VARCHAR)), 9, '0')
            END AS BID,
            BENE_ID
        FROM read_parquet('{bene_xwalk}', union_by_name := TRUE)
    """)

    LOGGER.info("Identifying NULL BID rows.")
    conn.execute("""
        INSERT INTO bid_to_bene_id_dirty
        SELECT BID, BENE_ID, 'NULL BID'
        FROM input_data
        WHERE BID IS NULL OR TRIM(BID) = '';
    """)

    LOGGER.info("Filtering out NULL BIDs to create clean input.")
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW input_clean AS
        SELECT *
        FROM input_data
        WHERE BID IS NOT NULL AND TRIM(BID) != '';
    """)

    LOGGER.info("Finding duplicate BID values in cleaned input.")
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW bid_counts AS
        SELECT BID, COUNT(*) AS cnt
        FROM input_clean
        GROUP BY BID
        HAVING COUNT(*) > 1;
    """)

    LOGGER.info("Inserting duplicate BIDs into failed table")
    conn.execute("""
        INSERT INTO bid_to_bene_id_failed
        SELECT c.BID, c.BENE_ID, 'DUPLICATE BID'
        FROM input_clean c
        JOIN bid_counts b ON c.BID = b.BID;
    """)

    LOGGER.info("Extracting unique (non-duplicate, non-null) BID rows.")
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW input_unique AS
        SELECT *
        FROM input_clean
        WHERE BID NOT IN (SELECT BID FROM bid_counts);
    """)

    LOGGER.info("Inserting unique rows into final clean table.")
    conn.execute("""INSERT INTO bid_to_bene_id SELECT * FROM input_unique""")

def main(args):
    conn = duckdb.connect("bid_bene.db")
    get_bid2bene_id(conn, args.bene_xwalk)

    conn.execute(f"COPY bid_to_bene_id TO '{args.bene_xwalk_output}' (FORMAT PARQUET)")
    LOGGER.info(f"Clean output written to: {args.bene_xwalk_output}")

    conn.execute(f"COPY bid_to_bene_id_failed TO '{args.failed_output}' (FORMAT PARQUET)")
    LOGGER.info(f"Failed output written to: {args.failed_output}")

    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process BID to BENE_ID crosswalk data")
    parser.add_argument('--bene_xwalk', default="./data/input/bene_xwalk_2007/bene_xwalk_2007.parquet")
    parser.add_argument('--bene_xwalk_output', default='./data/output/normalization_cms/bene_xwalk_2007/bene_xwalk.parquet')
    parser.add_argument('--failed_output', default='./data/output/normalization_cms/bene_xwalk_2007/bid_to_bene_id_failed.parquet')
    args = parser.parse_args()
    main(args)