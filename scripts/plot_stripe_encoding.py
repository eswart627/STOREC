#!/usr/bin/env python3

import sys
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def plot_combined_metrics(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        return

    df = pd.read_csv(csv_path)

    if df.empty:
        print("No data found in CSV")
        return

    df = df[df['size_mb'] >= 1]

    # Derived metrics
    df['total_time'] = df['total_stripe_build_time'] + df['total_encoding_time']
    df['avg_time'] = df['avg_stripe_build_time'] + df['avg_encoding_time_per_stripe']

    df = df.sort_values(by=['size_mb', 'mode'])

    sns.set_theme(style="whitegrid", context="talk")

    fig, axes = plt.subplots(1, 2, figsize=(16, 8), sharey=True)

    palette = {
        'SINGLE': '#4C72B0',
        'PARALLEL': '#55A868',
        'BLOCK_PARALLEL': '#C44E52'
    }

    # -----------------------------
    # TOTAL TIME
    # -----------------------------
    ax1 = axes[0]

    sns.lineplot(
        data=df,
        x='size_mb',
        y='total_time',
        hue='mode',
        style='mode',
        markers=True,
        dashes=False,
        linewidth=2.5,
        markersize=6,
        palette=palette,
        ax=ax1
    )

    ax1.set_title('Total Time (Build + Encode)', fontsize=16, weight='bold')
    ax1.set_xlabel('File Size (MB)')
    ax1.set_ylabel('Time (seconds)')

    ax1.set_xscale('log', base=2)
    ax1.set_xticks(sorted(df['size_mb'].unique()))
    ax1.set_xlim(left=df['size_mb'].min())

    # -----------------------------
    # AVG TIME (same scale)
    # -----------------------------
    ax2 = axes[1]

    sns.lineplot(
        data=df,
        x='size_mb',
        y='avg_time',
        hue='mode',
        style='mode',
        markers=True,
        dashes=False,
        linewidth=2.5,
        markersize=6,
        palette=palette,
        ax=ax2
    )

    ax2.set_title('Average Time per Stripe', fontsize=16, weight='bold')
    ax2.set_xlabel('File Size (MB)')
    ax2.set_ylabel('Time (seconds)')

    ax2.set_xscale('log', base=2)
    ax2.set_xticks(sorted(df['size_mb'].unique()))
    ax2.set_xlim(left=df['size_mb'].min())

    # 🔥 KEY FIX: same Y scale for both
    ax1.set_ylim(0, 32)
    ax2.set_ylim(0, 32)

    # Legends
    ax1.legend(title='Mode')
    ax2.legend(title='Mode')

    plt.suptitle('Stripe Build + Encoding Metrics Comparison',
                 fontsize=20, weight='bold')

    plt.tight_layout()
    plt.savefig("stripe_encoding_consistent.png", dpi=300, bbox_inches='tight')

    print("Saved: stripe_encoding_consistent.png")


if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "metrics.csv"
    plot_combined_metrics(csv_file)