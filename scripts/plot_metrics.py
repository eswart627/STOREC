#!/usr/bin/env python3
"""
Script to plot performance metrics from metrics.csv using Seaborn
Plots: Total Duration vs File Size for different modes
"""

import sys
import os

try:
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    from matplotlib.ticker import ScalarFormatter
except ImportError:
    print("Error: pandas, seaborn, and matplotlib are required to run this script.")
    print("Please install them using: pip install pandas seaborn matplotlib")
    sys.exit(1)

def plot_metrics(csv_path):
    """
    Read metrics from CSV and plot Total Duration vs File Size using Seaborn
    """
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        return

    try:
        # Load data using pandas
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if df.empty:
        print("No data found in CSV")
        return

    # Filter data to start from 32MB as requested
    df = df[df['size_mb'] >= 32]
    
    if df.empty:
        print("No data points >= 32MB found.")
        return

    # Sort data by size_mb for correct line plotting
    df = df.sort_values(by=['size_mb', 'mode'])

    # Set aesthetic style
    sns.set_theme(style="ticks", context="talk")

    # Increase figure height to make the y-axis (time scale) "wider" visually
    plt.figure(figsize=(12, 10))

    palette = {
        'SINGLE': '#4C72B0',
        'PARALLEL': '#55A868',
        'BLOCK_PARALLEL': '#C44E52'
    }

    ax = sns.lineplot(
        data=df,
        x='size_mb',
        y='total_duration',
        hue='mode',
        style='mode',
        markers=True,
        dashes=False,
        linewidth=2.5,
        markersize=8,
        alpha=0.9,
        palette=palette
    )

    # 🔥 Log scale for X (File Size)
    ax.set_xscale('log', base=2)

    # Show all unique sizes as ticks for better precision
    unique_sizes = sorted(df['size_mb'].unique())
    ax.set_xticks(unique_sizes)
    ax.set_xticklabels([int(x) if x >= 1 else x for x in unique_sizes])

    # Force origin to start at (32, 0)
    plt.xlim(left=32)
    plt.ylim(bottom=0)

    # Labels + title
    plt.title('Upload Duration vs File Size', fontsize=20, weight='bold', pad=20)
    plt.xlabel('File Size (MB)', fontsize=15, fontweight='bold')
    plt.ylabel('Duration (seconds)', fontsize=15, fontweight='bold')

    # Move legend OUTSIDE
    plt.legend(
        title='Execution Mode',
        bbox_to_anchor=(1.02, 1),
        loc='upper left',
        borderaxespad=0,
        frameon=True,
        shadow=True
    )

    # Cleaner grid
    sns.despine()
    plt.grid(True, which='both', linestyle='--', alpha=0.4)

    plt.tight_layout()
    plt.savefig("performance_comparison_clean.png", dpi=300, bbox_inches='tight')    
    print(f"\nSuccessfully generated plot: performance_comparison_clean.png")
    print(f"Origin set to (1, 0). Y-axis stretched for better visibility.")
    print(f"Data points plotted: {len(df)}")

if __name__ == "__main__":
    # Check if path provided as argument, otherwise use default
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'metrics.csv'
    
    if not os.path.exists(csv_file):
        print(f"Usage: python3 scripts/plot_metrics.py [csv_path]")
        print(f"Default path 'metrics.csv' not found.")
    else:
        plot_metrics(csv_file)
