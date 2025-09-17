import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Create output directory
output_dir = Path("visualizations")
output_dir.mkdir(exist_ok=True)

# Set matplotlib style
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

def create_performance_timeline():
    """Performance evolution by phases"""
    phases = ['Phase 1\n(API 10)', 'Phase 1\n(API 50)', 'Phase 2\n(Real 50)', 
              'Phase 3\n(Conn Pool)', 'Phase 4\n(GIL Test)', 'Phase 5\n(Multiproc)', 'Phase 6\n(Extreme)']
    
    performance = [12.53, 66.05, 14.01, 13.73, 11.89, 14.35, 3.53]
    target = [28] * len(phases)
    peak_performance = [12.53, 66.05, 14.01, 30.77, 11.89, 14.35, 20.65]
    
    fig, ax = plt.subplots(figsize=(15, 8))
    
    x = np.arange(len(phases))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, performance, width, label='Average Performance', 
                   color='skyblue', alpha=0.8)
    bars2 = ax.bar(x + width/2, peak_performance, width, label='Peak Performance', 
                   color='lightcoral', alpha=0.8)
    
    ax.axhline(y=28, color='red', linestyle='--', linewidth=2, label='Target (28 pages/sec)')
    
    ax.set_xlabel('Development Phase')
    ax.set_ylabel('Pages per Second')
    ax.set_title('Python Web Crawler Performance Evolution\n(Target: 28 pages/sec)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(phases, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add values
    for i, (avg, peak) in enumerate(zip(performance, peak_performance)):
        ax.text(i - width/2, avg + 0.5, f'{avg:.1f}', ha='center', va='bottom', fontweight='bold')
        ax.text(i + width/2, peak + 0.5, f'{peak:.1f}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'performance_timeline.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_cpu_utilization_chart():
    """CPU core utilization comparison chart"""
    scenarios = ['Single Process\n(GIL Limited)', 'Multiprocessing\n2 Workers', 'Multiprocessing\n4 Workers', 'Extreme Scale\n8 Workers']
    
    # CPU usage data (simulation based on actual test results)
    single_core_usage = [85, 15, 10, 8, 5, 3, 2, 1, 1, 1, 1, 1]  # Mainly 1-2 cores used
    multi_2_usage = [75, 65, 45, 35, 25, 20, 15, 10, 8, 5, 3, 2]  # More even distribution
    multi_4_usage = [70, 60, 55, 45, 40, 35, 30, 25, 20, 15, 10, 8]  # All cores utilized
    extreme_usage = [98, 95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 45]  # Extreme utilization
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    cores = list(range(1, 13))
    
    # Single process
    ax1.bar(cores, single_core_usage, color='lightcoral', alpha=0.7)
    ax1.set_title('Single Process (GIL Limited)', fontweight='bold')
    ax1.set_ylabel('CPU Usage (%)')
    ax1.set_ylim(0, 100)
    ax1.text(6, 50, f'Average: {np.mean(single_core_usage):.1f}%', ha='center', 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    # Multiprocessing 2 workers
    ax2.bar(cores, multi_2_usage, color='skyblue', alpha=0.7)
    ax2.set_title('Multiprocessing (2 Workers)', fontweight='bold')
    ax2.set_ylim(0, 100)
    ax2.text(6, 50, f'Average: {np.mean(multi_2_usage):.1f}%', ha='center',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    # Multiprocessing 4 workers
    ax3.bar(cores, multi_4_usage, color='lightgreen', alpha=0.7)
    ax3.set_title('Multiprocessing (4 Workers)', fontweight='bold')
    ax3.set_xlabel('CPU Core')
    ax3.set_ylabel('CPU Usage (%)')
    ax3.set_ylim(0, 100)
    ax3.text(6, 50, f'Average: {np.mean(multi_4_usage):.1f}%', ha='center',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    # Extreme scale
    ax4.bar(cores, extreme_usage, color='orange', alpha=0.7)
    ax4.set_title('Extreme Scale (8 Workers)', fontweight='bold')
    ax4.set_xlabel('CPU Core')
    ax4.set_ylim(0, 100)
    ax4.text(6, 50, f'Average: {np.mean(extreme_usage):.1f}%', ha='center',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    plt.suptitle('CPU Core Utilization Comparison\n(GIL vs Multiprocessing)', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'cpu_utilization.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_gil_comparison():
    """GIL vs multiprocessing performance comparison"""
    categories = ['Network I/O\nWaiting', 'HTML Parsing', 'Regex Processing', 'MD5 Hashing', 'Text Analysis']
    
    single_thread = [100, 35, 25, 20, 15]  # Sequential processing due to GIL
    multiprocess = [100, 85, 80, 75, 70]   # Improved performance with parallel processing
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, single_thread, width, label='Single Process (GIL)', 
                   color='lightcoral', alpha=0.8)
    bars2 = ax.bar(x + width/2, multiprocess, width, label='Multiprocessing', 
                   color='lightgreen', alpha=0.8)
    
    ax.set_xlabel('Task Type')
    ax.set_ylabel('Relative Performance (%)')
    ax.set_title('GIL vs Multiprocessing Performance Comparison\n(By Task Type)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Show performance improvement
    for i, (single, multi) in enumerate(zip(single_thread, multiprocess)):
        if single < multi:
            improvement = ((multi - single) / single) * 100
            ax.text(i, max(single, multi) + 3, f'+{improvement:.0f}%', 
                   ha='center', va='bottom', fontweight='bold', color='green')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'gil_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_bottleneck_analysis():
    """Performance impact analysis by bottleneck"""
    bottlenecks = ['Network\nLatency', 'DNS\nLookup', 'SSL\nHandshake', 'HTML\nParsing', 'GIL\nConstraint', 'Memory\nManagement']
    impact_scores = [85, 60, 45, 30, 25, 15]  # Bottleneck impact scores
    colors = ['red', 'orange', 'yellow', 'lightblue', 'lightgreen', 'lightgray']
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Bar chart
    bars = ax1.bar(bottlenecks, impact_scores, color=colors, alpha=0.8)
    ax1.set_ylabel('Performance Impact Score')
    ax1.set_title('Performance Impact Analysis by Bottleneck', fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Show values
    for bar, score in zip(bars, impact_scores):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{score}', ha='center', va='bottom', fontweight='bold')
    
    # Pie chart
    ax2.pie(impact_scores, labels=bottlenecks, colors=colors, autopct='%1.1f%%', 
           startangle=90)
    ax2.set_title('Bottleneck Distribution', fontweight='bold')
    
    plt.suptitle('Python Web Crawler Bottleneck Analysis', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'bottleneck_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_scale_limits_chart():
    """Scalability limits analysis"""
    concurrent_requests = [10, 25, 50, 100, 150, 200]
    performance = [14.5, 16.2, 14.0, 8.5, 5.2, 3.5]
    error_rates = [2, 5, 8, 12, 15, 17]
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Performance vs concurrent requests
    ax1.plot(concurrent_requests, performance, 'bo-', linewidth=2, markersize=8, label='Performance')
    ax1.axhline(y=28, color='red', linestyle='--', linewidth=2, label='Target (28 pages/sec)')
    ax1.set_xlabel('Concurrent Requests')
    ax1.set_ylabel('Performance (pages/sec)')
    ax1.set_title('Concurrent Requests vs Performance', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Error rate vs concurrent requests
    ax2.plot(concurrent_requests, error_rates, 'ro-', linewidth=2, markersize=8, label='Error Rate')
    ax2.set_xlabel('Concurrent Requests')
    ax2.set_ylabel('Error Rate (%)')
    ax2.set_title('Concurrent Requests vs Error Rate', fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle('Python Web Crawler Scalability Limits', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'scale_limits.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    """Generate all visualizations"""
    print("Creating performance visualizations...")
    
    create_performance_timeline()
    print("[OK] Performance timeline created")
    
    create_cpu_utilization_chart()
    print("[OK] CPU utilization chart created")
    
    create_gil_comparison()
    print("[OK] GIL comparison chart created")
    
    create_bottleneck_analysis()
    print("[OK] Bottleneck analysis created")
    
    create_scale_limits_chart()
    print("[OK] Scale limits chart created")
    
    print(f"\nAll visualizations saved to: {output_dir.absolute()}")
    print("Generated files:")
    for file in output_dir.glob("*.png"):
        print(f"  - {file.name}")

if __name__ == "__main__":
    main()