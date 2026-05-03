#!/usr/bin/env python3

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_throughput_grouped(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        return

    df = pd.read_csv(csv_path)

    if df.empty:
        print("No data found in CSV")
        return

    # Sort data
    df = df.sort_values(by=['size', 'mode'])

    sizes = sorted(df['size'].unique())
    modes = ['SINGLE', 'PARALLEL', 'BLOCK_PARALLEL']

    colors = {
        'SINGLE': '#4C72B0',
        'PARALLEL': '#55A868',
        'BLOCK_PARALLEL': '#C44E52'
    }

    x = np.arange(len(sizes))
    width = 0.25

    plt.figure(figsize=(12, 7))

    for i, mode in enumerate(modes):
        values = []
        for size in sizes:
            val = df[(df['mode'] == mode) & (df['size'] == size)]['avg_throughput'].values
            values.append(val[0] if len(val) else 0)

        bars = plt.bar(x + i*width, values, width,
                       label=mode,
                       color=colors[mode])

        # value labels
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2,
                     height + 0.5,
                     f"{height:.1f}",
                     ha='center', fontsize=8)

    plt.title('Average Throughput vs File Size (Grouped)', fontsize=16, weight='bold')
    plt.xlabel('File Size (MB)', fontsize=13)
    plt.ylabel('Throughput (MB/s)', fontsize=13)

    plt.xticks(x + width, sizes)

    plt.legend(title='Execution Mode')

    # 🔥 zoom for better visibility
    plt.ylim(df['avg_throughput'].min() * 0.9,
             df['avg_throughput'].max() * 1.1)

    plt.grid(axis='y', linestyle='--', alpha=0.3)

    plt.tight_layout()
    plt.savefig("throughput_grouped_bar.png", dpi=300, bbox_inches='tight')

    print("Saved: throughput_grouped_bar.png")


if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "size_throughput.csv"
    plot_throughput_grouped(csv_file)