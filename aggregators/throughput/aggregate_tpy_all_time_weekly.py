#!/usr/bin/env python3
import psycopg2
import json
from datetime import datetime, timedelta
import argparse

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

# [Previous functions remain the same until aggregate_weekly_tpy_for_week]

def aggregate_weekly_tpy_for_week(week_id):
    """Aggregate weekly TPY metrics for a specific week"""
    print(f"\nAGGREGATING WEEKLY TPY FOR: {week_id}")
    print("=" * 60)
    
    week_start, week_end = get_week_date_range(week_id)
    
    weekly_first_pass_yield = calculate_weekly_first_pass_yield_from_raw(week_start, week_end)
    
    model_specific_yields = calculate_model_specific_throughput_yields(week_start, week_end)
    
    hardcoded_tpy = calculate_hardcoded_tpy(model_specific_yields)
    
    dynamic_tpy = calculate_dynamic_tpy(model_specific_yields)
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    SUM(total_parts) as total_parts,
                    SUM(passed_parts) as passed_parts
                FROM daily_tpy_metrics 
                WHERE date_id >= %s AND date_id <= %s
            """, (week_start, week_end))
            
            daily_result = cur.fetchone()
            total_parts_overall = daily_result[0] if daily_result[0] else 0
            total_passed_parts = daily_result[1] if daily_result[1] else 0
            overall_yield = (total_passed_parts / total_parts_overall * 100) if total_parts_overall > 0 else 0
            
            weekly_station_metrics = model_specific_yields["overall"]
            avg_throughput_yield = round(sum(s["throughputYield"] for s in weekly_station_metrics.values()) / len(weekly_station_metrics), 2) if weekly_station_metrics else 0
            
            best_station = None
            worst_station = None
            if weekly_station_metrics:
                best_station = max(weekly_station_metrics.items(), key=lambda x: x[1]["throughputYield"])
                worst_station = min(weekly_station_metrics.items(), key=lambda x: x[1]["throughputYield"])
            
            # Insert main weekly metrics
            cur.execute("""
                INSERT INTO weekly_tpy_metrics (
                    week_id, week_start, week_end, days_in_week,
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
                    worst_station_yield
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (week_id) DO UPDATE SET
                    week_start = EXCLUDED.week_start,
                    week_end = EXCLUDED.week_end,
                    days_in_week = EXCLUDED.days_in_week,
                    weekly_first_pass_yield_traditional_parts_started = EXCLUDED.weekly_first_pass_yield_traditional_parts_started,
                    weekly_first_pass_yield_traditional_first_pass_success = EXCLUDED.weekly_first_pass_yield_traditional_first_pass_success,
                    weekly_first_pass_yield_traditional_first_pass_yield = EXCLUDED.weekly_first_pass_yield_traditional_first_pass_yield,
                    weekly_first_pass_yield_completed_only_active_parts = EXCLUDED.weekly_first_pass_yield_completed_only_active_parts,
                    weekly_first_pass_yield_completed_only_first_pass_success = EXCLUDED.weekly_first_pass_yield_completed_only_first_pass_success,
                    weekly_first_pass_yield_completed_only_first_pass_yield = EXCLUDED.weekly_first_pass_yield_completed_only_first_pass_yield,
                    weekly_first_pass_yield_breakdown_parts_completed = EXCLUDED.weekly_first_pass_yield_breakdown_parts_completed,
                    weekly_first_pass_yield_breakdown_parts_failed = EXCLUDED.weekly_first_pass_yield_breakdown_parts_failed,
                    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo = EXCLUDED.weekly_first_pass_yield_breakdown_parts_stuck_in_limbo,
                    weekly_first_pass_yield_breakdown_total_parts = EXCLUDED.weekly_first_pass_yield_breakdown_total_parts,
                    weekly_overall_yield_total_parts = EXCLUDED.weekly_overall_yield_total_parts,
                    weekly_overall_yield_completed_parts = EXCLUDED.weekly_overall_yield_completed_parts,
                    weekly_overall_yield_overall_yield = EXCLUDED.weekly_overall_yield_overall_yield,
                    weekly_throughput_yield_station_metrics = EXCLUDED.weekly_throughput_yield_station_metrics,
                    weekly_throughput_yield_average_yield = EXCLUDED.weekly_throughput_yield_average_yield,
                    total_stations = EXCLUDED.total_stations,
                    best_station_name = EXCLUDED.best_station_name,
                    best_station_yield = EXCLUDED.best_station_yield,
                    worst_station_name = EXCLUDED.worst_station_name,
                    worst_station_yield = EXCLUDED.worst_station_yield;
            """, (
                week_id, week_start, week_end, 7,  # days_in_week = 7
                weekly_first_pass_yield["traditional"]["partsStarted"],
                weekly_first_pass_yield["traditional"]["firstPassSuccess"],
                weekly_first_pass_yield["traditional"]["firstPassYield"],
                weekly_first_pass_yield["completedOnly"]["activeParts"],
                weekly_first_pass_yield["completedOnly"]["firstPassSuccess"],
                weekly_first_pass_yield["completedOnly"]["firstPassYield"],
                weekly_first_pass_yield["breakdown"]["partsCompleted"],
                weekly_first_pass_yield["breakdown"]["partsFailed"],
                weekly_first_pass_yield["breakdown"]["partsStuckInLimbo"],
                weekly_first_pass_yield["breakdown"]["totalParts"],
                total_parts_overall,
                total_passed_parts,
                round(overall_yield, 2),
                json.dumps(weekly_station_metrics),
                avg_throughput_yield,
                len(weekly_station_metrics),
                best_station[0] if best_station else None,
                best_station[1]["throughputYield"] if best_station else None,
                worst_station[0] if worst_station else None,
                worst_station[1]["throughputYield"] if worst_station else None
            ))

            # Insert model-specific metrics
            for model_name, model_data in hardcoded_tpy.items():
                if model_data["stations"]:  # Only insert if we have data for this model
                    cur.execute("""
                        INSERT INTO weekly_tpy_model_metrics (
                            week_id,
                            model,
                            hardcoded_stations,
                            hardcoded_tpy,
                            dynamic_stations,
                            dynamic_tpy,
                            dynamic_station_count
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (week_id, model) DO UPDATE SET
                            hardcoded_stations = EXCLUDED.hardcoded_stations,
                            hardcoded_tpy = EXCLUDED.hardcoded_tpy,
                            dynamic_stations = EXCLUDED.dynamic_stations,
                            dynamic_tpy = EXCLUDED.dynamic_tpy,
                            dynamic_station_count = EXCLUDED.dynamic_station_count;
                    """, (
                        week_id,
                        f"Tesla {model_name}" if model_name != "SXM6" else "SXM6",
                        json.dumps(model_data["stations"]),
                        model_data["tpy"],
                        json.dumps(dynamic_tpy[model_name]["stations"]),
                        dynamic_tpy[model_name]["tpy"],
                        dynamic_tpy[model_name]["stationCount"]
                    ))

            conn.commit()
            print(f"\nWeekly TPY aggregation complete for {week_id}!")
            
    finally:
        conn.close()

# [Rest of the file remains the same]