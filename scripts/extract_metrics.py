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
            file_match = re.search(r'File:\s+(.*?)\s+=>', section)
            
            # Extract average latency and throughputs
            # Format: AVERAGE | 0.3347s | 139.80 MB/s | 26.19 MB/s
            avg_line_match = re.search(r'AVERAGE\s+\|\s+([\d.]+)s\s+\|\s+([\d.]+) MB/s\s+\|\s+([\d.|N/A]+)', section)
            
            # Extract total upload duration
            total_duration_match = re.search(r'TOTAL UPLOAD DURATION.*?\|\s+([\d.]+)s', section)
            
            # Extract stripe build time metrics
            stripes_processed = re.search(r'Total stripes processed:\s+(\d+)', section)
            total_stripe = re.search(r'Total stripe build time:\s+([\d.]+)s', section)
            avg_stripe = re.search(r'Average stripe build time:\s+([\d.]+)s', section)
            stripe_per_mb = re.search(r'Stripe build time per MB:\s+([\d.]+)s/MB', section)
            
            # Extract encoding metrics
            stripes_encoded = re.search(r'Total stripes encoded:\s+(\d+)', section)
            total_encoding = re.search(r'Total encoding time:\s+([\d.]+)s', section)
            avg_encoding = re.search(r'Average encoding time per stripe:\s+([\d.]+)s', section)
            encoding_per_mb = re.search(r'Encoding time per MB:\s+([\d.]+)s/MB', section)
            
            # Extract network metrics
            net_tests = re.search(r'Total network tests:\s+(\d+)', section)
            avg_net_tp = re.search(r'Average network throughput:\s+([\d.]+) MB/s', section)
            total_net_tp = re.search(r'Total network throughput:\s+([\d.]+) MB/s', section)
            
            if mode_match and avg_line_match:
                net_tp_str = avg_line_match.group(3).replace(' MB/s', '').strip()
                try:
                    net_tp = float(net_tp_str) if net_tp_str != 'N/A' else 0.0
                except ValueError:
                    net_tp = 0.0

                metrics = {
                    'upload_number': i,
                    'mode': mode_match.group(1),
                    'file': file_match.group(1) if file_match else "unknown",
                    'size_mb': float(size_match.group(1)) if size_match else None,
                    'avg_latency': float(avg_line_match.group(1)),
                    'avg_throughput': float(avg_line_match.group(2)),
                    'avg_net_tp_from_table': net_tp,
                    'total_duration': float(total_duration_match.group(1)) if total_duration_match else None,
                    
                    # Stripe Build Metrics
                    'stripes_processed': int(stripes_processed.group(1)) if stripes_processed else 0,
                    'total_stripe_build_time': float(total_stripe.group(1)) if total_stripe else 0.0,
                    'avg_stripe_build_time': float(avg_stripe.group(1)) if avg_stripe else 0.0,
                    'stripe_build_time_per_mb': float(stripe_per_mb.group(1)) if stripe_per_mb else 0.0,
                    
                    # Encoding Metrics
                    'stripes_encoded': int(stripes_encoded.group(1)) if stripes_encoded else 0,
                    'total_encoding_time': float(total_encoding.group(1)) if total_encoding else 0.0,
                    'avg_encoding_time_per_stripe': float(avg_encoding.group(1)) if avg_encoding else 0.0,
                    'encoding_time_per_mb': float(encoding_per_mb.group(1)) if encoding_per_mb else 0.0,
                    
                    # Network Metrics
                    'total_network_tests': int(net_tests.group(1)) if net_tests else 0,
                    'avg_network_throughput': float(avg_net_tp.group(1)) if avg_net_tp else 0.0,
                    'total_network_throughput': float(total_net_tp.group(1)) if total_net_tp else 0.0
                }
                metrics_list.append(metrics)
            else:
                print(f"Warning: Missing core metrics for upload section {i}")
                
        except Exception as e:
            print(f"Error processing upload section {i}: {e}")
            continue
    
    return metrics_list

def print_metrics_table(metrics_list: List[Dict]):
    """Print metrics in a formatted table"""
    if not metrics_list:
        print("No metrics found")
        return
    
    print("\n" + "="*160)
    header = f"{'#':<3} | {'Mode':<10} | {'Size(MB)':<10} | {'Latency':<10} | {'TP(MB/s)':<10} | {'Net TP':<10} | {'Stripe(s)':<10} | {'Encode(s)':<10} | {'Duration':<10}"
    print(header)
    print("-" * len(header))
    
    for metrics in metrics_list:
        print(f"{metrics['upload_number']:<3} | {metrics['mode']:<10} | {metrics['size_mb']:<10.2f} | "
              f"{metrics['avg_latency']:<10.4f} | {metrics['avg_throughput']:<10.2f} | {metrics['avg_net_tp_from_table']:<10.2f} | "
              f"{metrics['total_stripe_build_time']:<10.4f} | {metrics['total_encoding_time']:<10.4f} | {metrics['total_duration']:<10.4f}")
    
    print("="*160)

def export_to_csv(metrics_list: List[Dict], output_file: str):
    """Export metrics to CSV file"""
    if not metrics_list:
        print("No metrics to export")
        return
    
    import csv
    
    with open(output_file, 'w', newline='') as csvfile:
        if metrics_list:
            fieldnames = list(metrics_list[0].keys())
            # Ensure 'file' is present if it was skipped in old code
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for metrics in metrics_list:
                writer.writerow(metrics)
    
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
        avg_tp = sum(m['avg_throughput'] for m in mode_metrics) / len(mode_metrics)
        avg_stripe_time = sum(m['avg_stripe_build_time'] for m in mode_metrics) / len(mode_metrics)
        avg_encode_time = sum(m['avg_encoding_time_per_stripe'] for m in mode_metrics) / len(mode_metrics)
        
        print(f"\n{mode.upper()} MODE ({len(mode_metrics)} uploads):")
        print(f"  Average Latency:      {avg_latency:.4f}s")
        print(f"  Average Throughput:   {avg_tp:.2f} MB/s")
        print(f"  Average Stripe Time:  {avg_stripe_time:.4f}s")
        print(f"  Average Encode Time:  {avg_encode_time:.4f}s")

def main():
    # Determine log file
    if len(sys.argv) >= 2:
        log_file = sys.argv[1]
    else:
        log_file = 'client/logs/client.log'
        print(f"No log file specified. Using default: {log_file}")
    
    # Determine output file
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        output_file = 'metrics.csv'
    
    # Extract metrics
    metrics = extract_upload_metrics(log_file)
    
    if not metrics:
        if len(sys.argv) < 2:
            print("Usage: python extract_metrics.py <log_file_path> [output.csv]")
        return
    
    # Sort metrics by size_mb (ascending)
    sorted_metrics = sorted(metrics, key=lambda x: (x['size_mb'] if x['size_mb'] is not None else 0, x['mode']))
    
    # Export to CSV
    export_to_csv(sorted_metrics, output_file)
    
    # Print summary table to console
    print_metrics_table(sorted_metrics)
    
    # Print analysis
    analyze_performance(sorted_metrics)
    
    print(f"\nExtracted {len(metrics)} upload records to {output_file}")

if __name__ == "__main__":
    main()
