-- Drop existing tables if they exist
DROP TABLE IF EXISTS weekly_tpy_model_metrics;
DROP TABLE IF EXISTS weekly_tpy_metrics;
DROP TABLE IF EXISTS weekly_tpy_metrics_backup;

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

-- Verify tables are empty
SELECT COUNT(*) as total_weeks FROM weekly_tpy_metrics;
SELECT COUNT(*) as total_model_records FROM weekly_tpy_model_metrics;