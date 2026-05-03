#!/usr/bin/env python3
"""
Script to plot Stripe Build + Encoding time comparison.
Generates 3 separate graphs (one per mode) showing Total and Average times.
"""

import sys
import os

try:
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    from matplotlib.ticker import ScalarFormatter
except ImportError:
    print("Error: pandas, seaborn, and matplotlib are required.")
    print("Please install them using: pip install pandas seaborn matplotlib")
    sys.exit(1)

def plot_combined_metrics(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if df.empty:
        print("No data found in CSV")
        return

    # Filter for sizes >= 1MB
    df = df[df['size_mb'] >= 1]
    
    # Calculate combined metrics
    # Total Time = Stripe Build Time + Encoding Time
    df['total_time'] = df['total_stripe_build_time'] + df['total_encoding_time']
    # Average Time = Avg Stripe Build + Avg Encoding per stripe
    df['avg_time'] = df['avg_stripe_build_time'] + df['avg_encoding_time_per_stripe']

    # Sort by size
    df = df.sort_values(by='size_mb')

    modes = ['SINGLE', 'PARALLEL', 'BLOCK_PARALLEL']
    sns.set_theme(style="whitegrid", context="talk")

    # Create a figure with 2 subplots (Total and Average)
    fig, axes = plt.subplots(1, 2, figsize=(20, 9), sharey=False)
    
    # Custom colors for modes
    mode_colors = {
        'SINGLE': '#3498db',         # Blue
        'PARALLEL': '#2ecc71',       # Green
        'BLOCK_PARALLEL': '#e74c3c'  # Red
    }

    import matplotlib.ticker as ticker

    # Plot 1: Total Time (Stripe Build + Encoding)
    ax1 = axes[0]
    for mode in modes:
        mode_df = df[df['mode'] == mode]
        if not mode_df.empty:
            sns.lineplot(
                data=mode_df, x='size_mb', y='total_time', 
                marker='o', markersize=10, linewidth=3, label=f'{mode}',
                color=mode_colors.get(mode), ax=ax1
            )
    
    ax1.set_title('Total Time (Build + Encode)', fontsize=18, fontweight='bold')
    ax1.set_xlabel('File Size (MB)', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Total Time (seconds)', fontsize=14, fontweight='bold')
    ax1.set_xscale('log', base=2)
    ax1.xaxis.set_major_formatter(ScalarFormatter())
    ax1.set_xticks(sorted(df['size_mb'].unique()))
    ax1.set_xlim(left=df['size_mb'].min())
    ax1.set_ylim(bottom=0)
    
    # Set Y-axis scale to 2 seconds per unit
    ax1.yaxis.set_major_locator(ticker.MultipleLocator(2))
    
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.legend(title='Mode')

    # Plot 2: Average Time per Stripe
    ax2 = axes[1]
    for mode in modes:
        mode_df = df[df['mode'] == mode]
        if not mode_df.empty:
            sns.lineplot(
                data=mode_df, x='size_mb', y='avg_time', 
                marker='s', markersize=10, linewidth=3, label=f'{mode}',
                color=mode_colors.get(mode), ax=ax2
            )

    ax2.set_title('Average Time per Stripe', fontsize=18, fontweight='bold')
    ax2.set_xlabel('File Size (MB)', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Avg Time (seconds)', fontsize=14, fontweight='bold')
    ax2.set_xscale('log', base=2)
    ax2.xaxis.set_major_formatter(ScalarFormatter())
    ax2.set_xticks(sorted(df['size_mb'].unique()))
    ax2.set_xlim(left=df['size_mb'].min())
    ax2.set_ylim(bottom=0)
    
    # Set Y-axis scale to 2 seconds per unit
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(2))
    
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.legend(title='Mode')

    plt.suptitle('Stripe Build + Encoding Metrics Comparison', fontsize=22, fontweight='bold', y=1.05)
    plt.tight_layout()
    
    output_file = 'stripe_encoding_metrics_one_fig.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nSuccessfully generated comparison plot: {output_file}")
    print(f"Modes compared: {', '.join(modes)}")

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'metrics.csv'
    plot_combined_metrics(csv_file)
