#!/usr/bin/env python3

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_grouped_bars(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        return

    df = pd.read_csv(csv_path)

    if df.empty:
        print("No data found in CSV")
        return

    # Filter (Upto 32 MB)
    df = df[(df['size_mb'] >= 1) & (df['size_mb'] <= 32)]

    # Metrics
    df['total_time'] = df['total_stripe_build_time'] + df['total_encoding_time']
    df['avg_time'] = df['avg_stripe_build_time'] + df['avg_encoding_time_per_stripe']

    sizes = sorted(df['size_mb'].unique())
    modes = ['SINGLE', 'PARALLEL', 'BLOCK_PARALLEL']

    # Color palette
    colors = {
        'SINGLE': '#4C72B0',
        'PARALLEL': '#55A868',
        'BLOCK_PARALLEL': '#C44E52'
    }

    x = np.arange(len(sizes))  # group positions
    width = 0.25              # bar width

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # -------------------------
    # 🔵 TOTAL TIME BAR GRAPH
    # -------------------------
    ax1 = axes[0]

    for i, mode in enumerate(modes):
        values = []
        for size in sizes:
            val = df[(df['mode'] == mode) & (df['size_mb'] == size)]['total_time'].values
            values.append(val[0] if len(val) else 0)

        bars = ax1.bar(x + i*width, values, width,
                       label=mode,
                       color=colors[mode])

        # Value labels
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2,
                     height + 0.3,
                     f"{height:.1f}",
                     ha='center', fontsize=8)

    ax1.set_title("Total Time (Build + Encode)", fontsize=14, weight='bold')
    ax1.set_xlabel("File Size (MB)")
    ax1.set_ylabel("Time (seconds)")
    ax1.set_xticks(x + width)
    ax1.set_xticklabels(sizes)
    ax1.legend()
    ax1.grid(axis='y', linestyle='--', alpha=0.3)

    # 🔥 Zoom to make differences visible
    ax1.set_ylim(min(df['total_time']) - 2, max(df['total_time']) + 5)

    # -------------------------
    # 🟢 AVG TIME BAR GRAPH
    # -------------------------
    ax2 = axes[1]

    for i, mode in enumerate(modes):
        values = []
        for size in sizes:
            val = df[(df['mode'] == mode) & (df['size_mb'] == size)]['avg_time'].values
            values.append(val[0] if len(val) else 0)

        bars = ax2.bar(x + i*width, values, width,
                       label=mode,
                       color=colors[mode])

        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2,
                     height + 0.05,
                     f"{height:.2f}",
                     ha='center', fontsize=8)

    ax2.set_title("Average Time per Stripe", fontsize=14, weight='bold')
    ax2.set_xlabel("File Size (MB)")
    ax2.set_ylabel("Time (seconds)")
    ax2.set_xticks(x + width)
    ax2.set_xticklabels(sizes)
    ax2.legend()
    ax2.grid(axis='y', linestyle='--', alpha=0.3)

    # 🔥 tight zoom (THIS is what makes small diffs visible)
    ax2.set_ylim(min(df['avg_time']) - 0.5, max(df['avg_time']) + 1)

    plt.suptitle("Stripe Build + Encoding Comparison (Grouped Bars)",
                 fontsize=18, weight='bold')

    plt.tight_layout()
    plt.savefig("grouped_bar_metrics.png", dpi=300, bbox_inches='tight')

    print("Saved: grouped_bar_metrics.png")


if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "metrics.csv"
    plot_grouped_bars(csv_file)