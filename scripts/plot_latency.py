#!/usr/bin/env python3
"""
Script to plot Average Latency vs File Size.
Range: 16MB to 1024MB.
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

def plot_latency(csv_path):
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
    # Seaborn will show the mean and confidence interval for overlapping points
    ax = sns.lineplot(
        data=df_filtered,
        x='size_mb',
        y='avg_latency',
        hue='mode',
        style='mode',
        markers=True,
        dashes=False,
        linewidth=3,
        markersize=10,
        palette=palette,
        errorbar='sd' # Show standard deviation if multiple points exist
    )

    # Customize axes
    plt.title('Average Latency vs File Size (32MB - 1024MB)', fontsize=18, fontweight='bold', pad=20)
    plt.xlabel('File Size (MB)', fontsize=14, fontweight='bold')
    plt.ylabel('Average Latency (seconds)', fontsize=14, fontweight='bold')
    
    # Log scale for X-axis (base 2)
    ax.set_xscale('log', base=2)
    ax.xaxis.set_major_formatter(ScalarFormatter())
    
    unique_sizes = sorted(df_filtered['size_mb'].unique())
    ax.set_xticks(unique_sizes)
    
    # Force axes to start at origin points for this range, and truncate at 1s
    plt.xlim(left=32)
    plt.ylim(bottom=0, top=1)
    
    # Legend customization
    plt.legend(title='Execution Mode', frameon=True, shadow=True, borderpad=1)
    
    plt.tight_layout()
    
    output_plot = 'latency_vs_size_16_1024.png'
    plt.savefig(output_plot, dpi=300)
    print(f"\nSuccessfully generated plot: {output_plot}")
    print(f"Data filtered for range: 16MB to 1024MB")

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'metrics.csv'
    plot_latency(csv_file)
