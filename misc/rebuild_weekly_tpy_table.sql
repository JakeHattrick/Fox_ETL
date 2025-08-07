-- Backup existing data
CREATE TABLE weekly_tpy_metrics_backup AS SELECT * FROM weekly_tpy_metrics;

-- Drop existing table
DROP TABLE weekly_tpy_metrics;

-- Create new main metrics table
CREATE TABLE weekly_tpy_metrics (
    -- Primary key and time identifiers
    week_id character varying(10) PRIMARY KEY,
    week_start date,
    week_end date,
    days_in_week integer,

    -- Weekly first pass yield metrics (traditional)
    weekly_first_pass_yield_traditional_parts_started integer,
    weekly_first_pass_yield_traditional_first_pass_success integer,
    weekly_first_pass_yield_traditional_first_pass_yield numeric(5,2),
    
    -- Weekly first pass yield metrics (completed only)
    weekly_first_pass_yield_completed_only_active_parts integer,
    weekly_first_pass_yield_completed_only_first_pass_success integer,
    weekly_first_pass_yield_completed_only_first_pass_yield numeric(5,2),
    
    -- Weekly first pass yield breakdown
    weekly_first_pass_yield_breakdown_parts_completed integer,
    weekly_first_pass_yield_breakdown_parts_failed integer,
    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo integer,
    weekly_first_pass_yield_breakdown_total_parts integer,
    
    -- Weekly overall yield metrics
    weekly_overall_yield_total_parts integer,
    weekly_overall_yield_completed_parts integer,
    weekly_overall_yield_overall_yield numeric(5,2),
    
    -- Weekly throughput yield metrics
    weekly_throughput_yield_station_metrics text,
    weekly_throughput_yield_average_yield numeric(5,2),
    
    -- Station metrics
    total_stations integer,
    best_station_name character varying(50),
    best_station_yield numeric(5,2),
    worst_station_name character varying(50),
    worst_station_yield numeric(5,2),
    
    -- Metadata
    created_at timestamp without time zone DEFAULT now()
);

-- Create new model-specific metrics table
CREATE TABLE weekly_tpy_model_metrics (
    -- Primary key
    week_id character varying(10),
    model character varying(50),
    
    -- Hardcoded TPY metrics
    hardcoded_stations text,
    hardcoded_tpy numeric(5,2),
    
    -- Dynamic TPY metrics
    dynamic_stations text,
    dynamic_tpy numeric(5,2),
    dynamic_station_count integer,
    
    -- Metadata
    created_at timestamp without time zone DEFAULT now(),
    
    -- Constraints
    PRIMARY KEY (week_id, model),
    FOREIGN KEY (week_id) REFERENCES weekly_tpy_metrics(week_id) ON DELETE CASCADE
);

-- Migrate main metrics data
INSERT INTO weekly_tpy_metrics (
    week_id,
    week_start,
    week_end,
    days_in_week,
    weekly_first_pass_yield_traditional_parts_started,
    weekly_first_pass_yield_traditional_first_pass_success,
    weekly_first_pass_yield_traditional_first_pass_yield,
    weekly_first_pass_yield_completed_only_active_parts,
    weekly_first_pass_yield_completed_only_first_pass_success,
    weekly_first_pass_yield_completed_only_first_pass_yield,
    weekly_first_pass_yield_breakdown_parts_completed,
    weekly_first_pass_yield_breakdown_parts_failed,
    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo,
    weekly_first_pass_yield_breakdown_total_parts,
    weekly_overall_yield_total_parts,
    weekly_overall_yield_completed_parts,
    weekly_overall_yield_overall_yield,
    weekly_throughput_yield_station_metrics,
    weekly_throughput_yield_average_yield,
    total_stations,
    best_station_name,
    best_station_yield,
    worst_station_name,
    worst_station_yield,
    created_at
)
SELECT 
    week_id,
    week_start,
    week_end,
    days_in_week,
    weekly_first_pass_yield_traditional_parts_started,
    weekly_first_pass_yield_traditional_first_pass_success,
    weekly_first_pass_yield_traditional_first_pass_yield,
    weekly_first_pass_yield_completed_only_active_parts,
    weekly_first_pass_yield_completed_only_first_pass_success,
    weekly_first_pass_yield_completed_only_first_pass_yield,
    weekly_first_pass_yield_breakdown_parts_completed,
    weekly_first_pass_yield_breakdown_parts_failed,
    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo,
    weekly_first_pass_yield_breakdown_total_parts,
    weekly_overall_yield_total_parts,
    weekly_overall_yield_completed_parts,
    weekly_overall_yield_overall_yield,
    weekly_throughput_yield_station_metrics,
    weekly_throughput_yield_average_yield,
    total_stations,
    best_station_name,
    best_station_yield,
    worst_station_name,
    worst_station_yield,
    created_at
FROM weekly_tpy_metrics_backup;

-- Migrate SXM4 data
INSERT INTO weekly_tpy_model_metrics (
    week_id,
    model,
    hardcoded_stations,
    hardcoded_tpy,
    dynamic_stations,
    dynamic_tpy,
    dynamic_station_count
)
SELECT 
    week_id,
    'Tesla SXM4',
    weekly_tpy_hardcoded_sxm4_stations,
    weekly_tpy_hardcoded_sxm4_tpy,
    weekly_tpy_dynamic_sxm4_stations,
    weekly_tpy_dynamic_sxm4_tpy,
    weekly_tpy_dynamic_sxm4_station_count
FROM weekly_tpy_metrics_backup
WHERE weekly_tpy_hardcoded_sxm4_stations IS NOT NULL 
   OR weekly_tpy_dynamic_sxm4_stations IS NOT NULL;

-- Migrate SXM5 data
INSERT INTO weekly_tpy_model_metrics (
    week_id,
    model,
    hardcoded_stations,
    hardcoded_tpy,
    dynamic_stations,
    dynamic_tpy,
    dynamic_station_count
)
SELECT 
    week_id,
    'Tesla SXM5',
    weekly_tpy_hardcoded_sxm5_stations,
    weekly_tpy_hardcoded_sxm5_tpy,
    weekly_tpy_dynamic_sxm5_stations,
    weekly_tpy_dynamic_sxm5_tpy,
    weekly_tpy_dynamic_sxm5_station_count
FROM weekly_tpy_metrics_backup
WHERE weekly_tpy_hardcoded_sxm5_stations IS NOT NULL 
   OR weekly_tpy_dynamic_sxm5_stations IS NOT NULL;

-- Verify the migration
SELECT COUNT(*) as total_weeks FROM weekly_tpy_metrics;
SELECT model, COUNT(*) as records FROM weekly_tpy_model_metrics GROUP BY model;

-- If everything looks good, you can drop the backup table:
-- DROP TABLE weekly_tpy_metrics_backup;