#!/usr/bin/env python3
"""
Script to extract performance metrics from client.log files
Extracts: avg latency, total stripe build time, avg stripe build time, stripe build time per MB
"""

import re
import json
import sys
from typing import List, Dict, Optional

def extract_upload_metrics(log_file_path: str) -> List[Dict]:
    """
    Extract upload metrics from log file
    
    Args:
        log_file_path: Path to the client.log file
        
    Returns:
        List of dictionaries containing extracted metrics for each upload
    """
    try:
        with open(log_file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: Log file {log_file_path} not found")
        return []
    
    # Split content by upload reports
    upload_sections = re.split(r'={95}\s*UPLOAD REPORT', content)
    
    metrics_list = []
    
    for i, section in enumerate(upload_sections[1:], 1):  # Skip first empty section
        try:
            # Extract mode and file info
            mode_match = re.search(r'Mode:\s+(\w+)', section)
            size_match = re.search(r'Size:\s+\(([\d.]+)\s+MB\)', section)
            
            # Extract average latency
            avg_latency_match = re.search(r'AVERAGE.*?\|\s+([\d.]+)s', section)
            
            # Extract stripe build time metrics
            total_stripe_match = re.search(r'Total stripe build time:\s+([\d.]+)s', section)
            avg_stripe_match = re.search(r'Average stripe build time:\s+([\d.]+)s', section)
            stripe_per_mb_match = re.search(r'Stripe build time per MB:\s+([\d.]+)s/MB', section)
            
            # Extract total upload duration
            total_duration_match = re.search(r'TOTAL UPLOAD DURATION.*?\|\s+([\d.]+)s', section)
            
            # Validate we have all required metrics
            if all([mode_match, avg_latency_match, total_stripe_match, avg_stripe_match, stripe_per_mb_match]):
                metrics = {
                    'upload_number': i,
                    'mode': mode_match.group(1),
                    'size_mb': float(size_match.group(1)) if size_match else None,
                    'avg_latency': float(avg_latency_match.group(1)),
                    'total_stripe_build_time': float(total_stripe_match.group(1)),
                    'avg_stripe_build_time': float(avg_stripe_match.group(1)),
                    'stripe_build_time_per_mb': float(stripe_per_mb_match.group(1)),
                    'total_upload_duration': float(total_duration_match.group(1)) if total_duration_match else None
                }
                metrics_list.append(metrics)
            else:
                print(f"Warning: Incomplete metrics for upload section {i}")
                
        except Exception as e:
            print(f"Error processing upload section {i}: {e}")
            continue
    
    return metrics_list

def print_metrics_table(metrics_list: List[Dict]):
    """Print metrics in a formatted table"""
    if not metrics_list:
        print("No metrics found")
        return
    
    print("\n" + "="*120)
    print(f"{'#':<3} | {'Mode':<10} | {'File':<35} | {'Size(MB)':<10} | {'Avg Latency':<12} | {'Total Stripe':<12} | {'Avg Stripe':<11} | {'Stripe/MB':<11}")
    print("-"*120)
    
    for metrics in metrics_list:
        print(f"{metrics['upload_number']:<3} | {metrics['mode']:<10} | {metrics['file'][:35]:<35} | {metrics['size_mb']:<10.2f} | "
              f"{metrics['avg_latency']:<12.4f} | {metrics['total_stripe_build_time']:<12.4f} | "
              f"{metrics['avg_stripe_build_time']:<11.4f} | {metrics['stripe_build_time_per_mb']:<11.4f}")
    
    print("="*120)

def export_to_csv(metrics_list: List[Dict], output_file: str):
    """Export metrics to CSV file"""
    if not metrics_list:
        print("No metrics to export")
        return
    
    import csv
    
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['upload_number', 'mode', 'size_mb', 'avg_latency', 
                     'total_stripe_build_time', 'avg_stripe_build_time', 'stripe_build_time_per_mb', 'total_upload_duration']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for metrics in metrics_list:
            # Remove file field from metrics dict if it exists
            metrics_copy = {k: v for k, v in metrics.items() if k != 'file'}
            writer.writerow(metrics_copy)
    
    print(f"Metrics exported to {output_file}")

def export_to_json(metrics_list: List[Dict], output_file: str):
    """Export metrics to JSON file"""
    with open(output_file, 'w') as jsonfile:
        json.dump(metrics_list, jsonfile, indent=2)
    
    print(f"Metrics exported to {output_file}")

def analyze_performance(metrics_list: List[Dict]):
    """Analyze performance across different modes"""
    if not metrics_list:
        return
    
    # Group by mode
    modes = {}
    for metrics in metrics_list:
        mode = metrics['mode']
        if mode not in modes:
            modes[mode] = []
        modes[mode].append(metrics)
    
    print("\n" + "="*80)
    print("PERFORMANCE ANALYSIS BY MODE")
    print("="*80)
    
    for mode, mode_metrics in modes.items():
        avg_latency = sum(m['avg_latency'] for m in mode_metrics) / len(mode_metrics)
        avg_stripe_time = sum(m['avg_stripe_build_time'] for m in mode_metrics) / len(mode_metrics)
        avg_stripe_per_mb = sum(m['stripe_build_time_per_mb'] for m in mode_metrics) / len(mode_metrics)
        
        print(f"\n{mode.upper()} MODE ({len(mode_metrics)} uploads):")
        print(f"  Average Latency: {avg_latency:.4f}s")
        print(f"  Average Stripe Build Time: {avg_stripe_time:.4f}s")
        print(f"  Average Stripe Time per MB: {avg_stripe_per_mb:.4f}s/MB")

def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_metrics.py <log_file_path> [output.csv]")
        print("Default: outputs to metrics.csv")
        print("Examples:")
        print("  python extract_metrics.py client/logs/client.log")
        print("  python extract_metrics.py client/logs/client.log my_metrics.csv")
        sys.exit(1)
    
    log_file = sys.argv[1]
    
    # Extract metrics
    metrics = extract_upload_metrics(log_file)
    
    if not metrics:
        print("No metrics found in log file")
        return
    
    # Determine output file
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        output_file = 'metrics.csv'
    
    # Sort metrics by size_mb (ascending)
    sorted_metrics = sorted(metrics, key=lambda x: x['size_mb'] if x['size_mb'] is not None else 0)
    
    # Export to CSV
    export_to_csv(sorted_metrics, output_file)
    
    # Print summary
    print(f"Extracted {len(metrics)} upload records to {output_file}")
    print(f"Columns: upload_number, mode, size_mb, avg_latency, total_stripe_build_time, avg_stripe_build_time, stripe_build_time_per_mb, total_upload_duration")

if __name__ == "__main__":
    main()
