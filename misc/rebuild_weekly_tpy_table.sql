-- Backup existing data
CREATE TABLE weekly_tpy_metrics_backup AS SELECT * FROM weekly_tpy_metrics;

-- Drop existing table
DROP TABLE weekly_tpy_metrics;

-- Create new flexible structure
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
    weekly_throughput_yield_station_metrics jsonb,
    weekly_throughput_yield_average_yield numeric(5,2),
    weekly_throughput_yield_model_specific jsonb,  -- Store all model data here
    
    -- Station metrics
    total_stations integer,
    best_station_name character varying(50),
    best_station_yield numeric(5,2),
    worst_station_name character varying(50),
    worst_station_yield numeric(5,2),
    
    -- Metadata
    created_at timestamp without time zone DEFAULT now()
);

-- Migrate existing data
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
    weekly_throughput_yield_model_specific,
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
    jsonb_build_object(
        'SXM4', jsonb_build_object(
            'hardcoded', jsonb_build_object(
                'stations', weekly_tpy_hardcoded_sxm4_stations,
                'tpy', weekly_tpy_hardcoded_sxm4_tpy
            ),
            'dynamic', jsonb_build_object(
                'stations', weekly_tpy_dynamic_sxm4_stations,
                'tpy', weekly_tpy_dynamic_sxm4_tpy,
                'stationCount', weekly_tpy_dynamic_sxm4_station_count
            )
        ),
        'SXM5', jsonb_build_object(
            'hardcoded', jsonb_build_object(
                'stations', weekly_tpy_hardcoded_sxm5_stations,
                'tpy', weekly_tpy_hardcoded_sxm5_tpy
            ),
            'dynamic', jsonb_build_object(
                'stations', weekly_tpy_dynamic_sxm5_stations,
                'tpy', weekly_tpy_dynamic_sxm5_tpy,
                'stationCount', weekly_tpy_dynamic_sxm5_station_count
            )
        )
    ) as weekly_throughput_yield_model_specific,
    total_stations,
    best_station_name,
    best_station_yield,
    worst_station_name,
    worst_station_yield,
    created_at
FROM weekly_tpy_metrics_backup;

-- Verify the migration
SELECT 
    week_id,
    weekly_throughput_yield_model_specific->>'SXM4' as sxm4_data,
    weekly_throughput_yield_model_specific->>'SXM5' as sxm5_data
FROM weekly_tpy_metrics
LIMIT 1;

-- If everything looks good, you can drop the backup table:
-- DROP TABLE weekly_tpy_metrics_backup;