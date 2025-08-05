
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
AND history_station_passing_status = 'Fail'
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

            print("Aggregating snfn report data from testboard_master_log...")
            cur.execute(AGGREGATE_SQL)
            rows = cur.fetchall()
            print(f"Aggregated {len(rows)} rows.")

            if rows:
                # Remove duplicates from the result set to prevent ON CONFLICT issues
                # Create a set to track unique combinations of primary key fields
                seen = set()
                unique_values = []
                
                for r in rows:
                    # Primary key: (sn, fixture_no, model, workstation_name, error_code, history_station_end_time)
                    pk_tuple = (r[2], r[0], r[4], r[1], r[5], r[7])  # sn, fixture_no, model, workstation_name, error_code, history_station_end_time
                    
                    if pk_tuple not in seen:
                        seen.add(pk_tuple)
                        # Match the column order in INSERT_SQL: fixture_no, workstation_name, sn, pn, model, error_code, error_disc, history_station_end_time
                        unique_values.append((
                            r[0],  # fixture_no
                            r[1],  # workstation_name
                            r[2],  # sn
                            r[3],  # pn
                            r[4],  # model
                            r[5],  # error_code
                            r[6],  # error_disc
                            r[7]   # history_station_end_time
                        ))
                
                print(f"Removed {len(rows) - len(unique_values)} duplicate rows.")
                
                if unique_values:
                    execute_values(cur, INSERT_SQL, unique_values)
                    conn.commit()
                    print(f"SNFN report aggregation complete. Upserted {len(unique_values)} unique rows.")
                else:
                    print("No unique rows to insert after deduplication.")
            else:
                print("No data to aggregate.")
                
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()