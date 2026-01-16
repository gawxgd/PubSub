#!/usr/bin/env python3
"""
Performance Test Results Plotter

Usage:
    python plot_results.py <report_directory>
    
Example:
    python plot_results.py perf_reports/throughput_100_20260115_143022

Requirements:
    pip install pandas matplotlib
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


def load_reports(report_dir: str):
    """Load all CSV files from the report directory."""
    report_path = Path(report_dir)
    
    summary_file = list(report_path.glob("*_summary.csv"))
    latencies_file = list(report_path.glob("*_latencies.csv"))
    timeline_file = list(report_path.glob("*_timeline.csv"))
    
    summary = pd.read_csv(summary_file[0]) if summary_file else None
    latencies = pd.read_csv(latencies_file[0]) if latencies_file else None
    timeline = pd.read_csv(timeline_file[0]) if timeline_file else None
    
    return summary, latencies, timeline


def plot_summary(summary: pd.DataFrame, output_dir: Path):
    """Plot summary metrics as a bar chart."""
    if summary is None or summary.empty:
        return
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    scenario = summary['scenario'].iloc[0]
    
    # Throughput comparison
    ax1 = axes[0]
    throughput_data = [summary['throughput_pub'].iloc[0], summary['throughput_recv'].iloc[0]]
    bars = ax1.bar(['Published', 'Received'], throughput_data, color=['#2ecc71', '#3498db'])
    ax1.set_ylabel('Messages/second')
    ax1.set_title('Throughput')
    for bar, val in zip(bars, throughput_data):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                f'{val:.1f}', ha='center', va='bottom')
    
    # Latency percentiles
    ax2 = axes[1]
    latency_labels = ['Min', 'P50', 'P75', 'P95', 'P99', 'Max']
    latency_values = [
        summary['lat_min_ms'].iloc[0],
        summary['lat_p50_ms'].iloc[0],
        summary['lat_p75_ms'].iloc[0],
        summary['lat_p95_ms'].iloc[0],
        summary['lat_p99_ms'].iloc[0],
        summary['lat_max_ms'].iloc[0]
    ]
    bars = ax2.bar(latency_labels, latency_values, color='#9b59b6')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Latency Percentiles')
    ax2.tick_params(axis='x', rotation=45)
    
    # Message loss
    ax3 = axes[2]
    published = summary['published'].iloc[0]
    received = summary['received'].iloc[0]
    loss = summary['loss'].iloc[0]
    ax3.pie([received, loss], labels=['Received', 'Lost'], 
            autopct='%1.2f%%', colors=['#2ecc71', '#e74c3c'],
            explode=(0, 0.1) if loss > 0 else (0, 0))
    ax3.set_title(f'Message Delivery\n({published} published)')
    
    plt.suptitle(f'Performance Summary: {scenario}', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'summary_chart.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Created: summary_chart.png")


def plot_latency_distribution(latencies: pd.DataFrame, output_dir: Path):
    """Plot latency distribution histogram."""
    if latencies is None or latencies.empty:
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    ax1 = axes[0]
    ax1.hist(latencies['latency_ms'], bins=50, color='#3498db', edgecolor='white', alpha=0.7)
    ax1.axvline(latencies['latency_ms'].median(), color='#e74c3c', linestyle='--', 
                label=f'Median: {latencies["latency_ms"].median():.2f}ms')
    ax1.axvline(latencies['latency_ms'].quantile(0.99), color='#f39c12', linestyle='--',
                label=f'P99: {latencies["latency_ms"].quantile(0.99):.2f}ms')
    ax1.set_xlabel('Latency (ms)')
    ax1.set_ylabel('Count')
    ax1.set_title('Latency Distribution')
    ax1.legend()
    
    # Latency over time (scatter)
    ax2 = axes[1]
    # Convert to relative time in seconds
    latencies['time_offset'] = (pd.to_datetime(latencies['published_utc']) - 
                                 pd.to_datetime(latencies['published_utc']).min()).dt.total_seconds()
    ax2.scatter(latencies['time_offset'], latencies['latency_ms'], 
                alpha=0.3, s=5, color='#3498db')
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Latency Over Time')
    
    # Add rolling average
    window = max(1, len(latencies) // 100)
    rolling_avg = latencies['latency_ms'].rolling(window=window, center=True).mean()
    ax2.plot(latencies['time_offset'], rolling_avg, color='#e74c3c', 
             linewidth=2, label=f'Rolling avg (window={window})')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(output_dir / 'latency_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Created: latency_distribution.png")


def plot_timeline(timeline: pd.DataFrame, output_dir: Path):
    """Plot throughput and latency over time."""
    if timeline is None or timeline.empty:
        return
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    
    # Throughput over time
    ax1 = axes[0]
    ax1.plot(timeline['second'], timeline['throughput_pub'], 
             label='Published', color='#2ecc71', linewidth=2)
    ax1.plot(timeline['second'], timeline['throughput_recv'], 
             label='Received', color='#3498db', linewidth=2)
    ax1.fill_between(timeline['second'], timeline['throughput_pub'], 
                     alpha=0.3, color='#2ecc71')
    ax1.fill_between(timeline['second'], timeline['throughput_recv'], 
                     alpha=0.3, color='#3498db')
    ax1.set_ylabel('Messages/second')
    ax1.set_title('Throughput Over Time')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # Latency over time
    ax2 = axes[1]
    ax2.plot(timeline['second'], timeline['avg_latency_ms'], 
             color='#9b59b6', linewidth=2)
    ax2.fill_between(timeline['second'], timeline['avg_latency_ms'], 
                     alpha=0.3, color='#9b59b6')
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Average Latency (ms)')
    ax2.set_title('Average Latency Over Time')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_dir / 'timeline_chart.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Created: timeline_chart.png")


def print_summary_table(summary: pd.DataFrame):
    """Print a formatted summary table to console."""
    if summary is None or summary.empty:
        return
    
    row = summary.iloc[0]
    
    print("\n" + "="*60)
    print(f"  PERFORMANCE SUMMARY: {row['scenario']}")
    print("="*60)
    print(f"  Duration:        {row['duration_sec']:.1f} seconds")
    print(f"  Published:       {row['published']:,} messages")
    print(f"  Received:        {row['received']:,} messages")
    print(f"  Loss:            {row['loss']:,} ({row['loss_pct']:.2f}%)")
    print("-"*60)
    print(f"  Throughput (pub): {row['throughput_pub']:.1f} msg/s")
    print(f"  Throughput (recv):{row['throughput_recv']:.1f} msg/s")
    print("-"*60)
    print(f"  Latency Min:     {row['lat_min_ms']:.2f} ms")
    print(f"  Latency P50:     {row['lat_p50_ms']:.2f} ms")
    print(f"  Latency P75:     {row['lat_p75_ms']:.2f} ms")
    print(f"  Latency P95:     {row['lat_p95_ms']:.2f} ms")
    print(f"  Latency P99:     {row['lat_p99_ms']:.2f} ms")
    print(f"  Latency Max:     {row['lat_max_ms']:.2f} ms")
    print("-"*60)
    print(f"  Memory Min:      {row['mem_min_mb']:.1f} MB")
    print(f"  Memory Max:      {row['mem_max_mb']:.1f} MB")
    print(f"  Memory Avg:      {row['mem_avg_mb']:.1f} MB")
    print("="*60 + "\n")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nAvailable report directories:")
        perf_reports = Path("perf_reports")
        if perf_reports.exists():
            for d in sorted(perf_reports.iterdir()):
                if d.is_dir():
                    print(f"  - {d}")
        else:
            print("  (no perf_reports directory found)")
        sys.exit(1)
    
    report_dir = sys.argv[1]
    
    if not os.path.exists(report_dir):
        print(f"Error: Directory not found: {report_dir}")
        sys.exit(1)
    
    print(f"Loading reports from: {report_dir}")
    summary, latencies, timeline = load_reports(report_dir)
    
    output_dir = Path(report_dir)
    
    print("\nGenerating charts...")
    
    if summary is not None:
        print_summary_table(summary)
        plot_summary(summary, output_dir)
    
    if latencies is not None and len(latencies) > 0:
        plot_latency_distribution(latencies, output_dir)
    
    if timeline is not None and len(timeline) > 0:
        plot_timeline(timeline, output_dir)
    
    print(f"\nCharts saved to: {output_dir}")


if __name__ == "__main__":
    main()
