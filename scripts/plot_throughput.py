#!/usr/bin/env python3
"""
Script to plot Average Throughput vs File Size.
Range: 32MB to 1024MB.
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

def plot_throughput(csv_path):
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

    # Filter for size range 32MB to 1024MB
    df_filtered = df[(df['size_mb'] >= 32) & (df['size_mb'] <= 1024)].copy()
    
    if df_filtered.empty:
        print("No data points found in range 32MB to 1024MB")
        return

    # Sort by size
    df_filtered = df_filtered.sort_values(by=['size_mb', 'mode'])

    # Set aesthetic style
    sns.set_theme(style="whitegrid", context="talk")
    plt.figure(figsize=(12, 8))
    
    # Custom color palette mapping
    palette = {
        'SINGLE': '#3498db',
        'PARALLEL': '#2ecc71',
        'BLOCK_PARALLEL': '#e74c3c'
    }
    
    # Create the line plot
    ax = sns.lineplot(
        data=df_filtered,
        x='size_mb',
        y='avg_throughput',
        hue='mode',
        style='mode',
        markers=True,
        dashes=False,
        linewidth=3,
        markersize=10,
        palette=palette,
        errorbar='sd'
    )

    # Customize axes
    plt.title('Average Throughput vs File Size (32MB - 1024MB)', fontsize=18, fontweight='bold', pad=20)
    plt.xlabel('File Size (MB)', fontsize=14, fontweight='bold')
    plt.ylabel('Average Throughput (MB/s)', fontsize=14, fontweight='bold')
    
    # Log scale for X-axis (base 2)
    ax.set_xscale('log', base=2)
    ax.xaxis.set_major_formatter(ScalarFormatter())
    
    unique_sizes = sorted(df_filtered['size_mb'].unique())
    ax.set_xticks(unique_sizes)
    
    # Force axes to start at origin points for this range
    plt.xlim(left=32)
    plt.ylim(bottom=0)
    
    # Legend customization
    plt.legend(title='Execution Mode', frameon=True, shadow=True, borderpad=1)
    
    plt.tight_layout()
    
    output_plot = 'throughput_vs_size_32_1024.png'
    plt.savefig(output_plot, dpi=300)
    print(f"\nSuccessfully generated plot: {output_plot}")
    print(f"Data filtered for range: 32MB to 1024MB")

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'metrics.csv'
    plot_throughput(csv_file)
