
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS snfn_aggregate_daily (
    fixture_no TEXT NOT NULL,
    workstation_name TEXT NOT NULL,
    sn TEXT NOT NULL,
    pn TEXT,
    model TEXT NOT NULL,
    error_code TEXT NOT NULL,
    error_disc TEXT,
    history_station_end_time DATE NOT NULL,
    PRIMARY KEY (sn, fixture_no, model, workstation_name, error_code, history_station_end_time)
);
'''

AGGREGATE_SQL = '''
SELECT DISTINCT
    fixture_no,
    workstation_name,
    sn,
    pn,
    model,
    CONCAT('EC', RIGHT(failure_reasons, 3)) AS error_code,
    failure_note AS error_disc,
    history_station_end_time
FROM testboard_master_log
WHERE history_station_end_time IS NOT NULL
AND history_station_passing_status = 'FAIL'
ORDER BY history_station_end_time DESC;
'''

INSERT_SQL = '''
INSERT INTO snfn_aggregate_daily (
    fixture_no, workstation_name, sn, pn, model, error_code, error_disc, history_station_end_time
) VALUES %s
ON CONFLICT (sn, fixture_no, model, workstation_name, error_code, history_station_end_time) 
DO UPDATE SET
    error_code = EXCLUDED.error_code,
    error_disc = EXCLUDED.error_disc,
    pn = EXCLUDED.pn;
'''

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print("Creating snfn_aggregate_daily table if not exists...")
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

            print("Aggregating and upserting snfn report data using temporary table...")
            
            # Use a single SQL operation with temporary table to avoid Python-level deduplication issues
            upsert_sql = '''
            -- Create temporary table with aggregated data
            CREATE TEMP TABLE temp_snfn_aggregate AS
            SELECT DISTINCT
                fixture_no,
                workstation_name,
                sn,
                pn,
                model,
                CONCAT('EC', RIGHT(failure_reasons, 3)) AS error_code,
                failure_note AS error_disc,
                history_station_end_time::DATE
            FROM testboard_master_log
            WHERE history_station_end_time IS NOT NULL
            AND history_station_passing_status = 'FAIL';
            
            -- Get count for reporting
            SELECT COUNT(*) FROM temp_snfn_aggregate;
            '''
            
            cur.execute(upsert_sql)
            count_result = cur.fetchone()
            rows_to_process = count_result[0] if count_result else 0
            print(f"Found {rows_to_process} unique rows to process.")
            
            if rows_to_process > 0:
                # Now upsert from temp table to main table
                final_upsert_sql = '''
                INSERT INTO snfn_aggregate_daily (
                    fixture_no, workstation_name, sn, pn, model, error_code, error_disc, history_station_end_time
                )
                SELECT 
                    fixture_no, workstation_name, sn, pn, model, error_code, error_disc, history_station_end_time
                FROM temp_snfn_aggregate
                ON CONFLICT (sn, fixture_no, model, workstation_name, error_code, history_station_end_time) 
                DO UPDATE SET
                    error_code = EXCLUDED.error_code,
                    error_disc = EXCLUDED.error_disc,
                    pn = EXCLUDED.pn;
                '''
                
                cur.execute(final_upsert_sql)
                conn.commit()
                print(f"SNFN report aggregation complete. Processed {rows_to_process} rows.")
            else:
                print("No data to aggregate.")
                
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()