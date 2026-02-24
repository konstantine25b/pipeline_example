import argparse
import csv
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


DEFAULT_INPUT_CSV = Path("data/nbg_currencies_filled.csv")
DEFAULT_OUTPUT_DIR = Path("plots")


def parse_iso_date(iso_str):
    """Parse ISO date string to datetime object."""
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))


def plot_currencies(input_csv: Path, output_dir: Path, currencies: list = None):
    """
    Create plots for currency exchange rates over time.
    
    Args:
        input_csv: Path to the filled CSV file
        output_dir: Directory to save plots
        currencies: List of currency codes to plot (default: USD, EUR, GBP)
    """
    if currencies is None:
        currencies = ['USD', 'EUR', 'GBP']
    
    print(f"Reading data from {input_csv}...")
    
    # Read CSV into pandas DataFrame
    df = pd.read_csv(input_csv)
    
    # Parse dates
    df['calendar_date'] = pd.to_datetime(df['calendar_date'])
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    
    print(f"Loaded {len(df)} records")
    print(f"Date range: {df['calendar_date'].min().date()} to {df['calendar_date'].max().date()}")
    print(f"Currencies available: {sorted(df['code'].unique().tolist())}")
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Plot 1: Selected currencies over time
    plot_selected_currencies(df, currencies, output_dir)
    
    # Plot 2: All currencies (multi-panel)
    plot_all_currencies(df, output_dir)
    
    # Plot 3: Currency changes/volatility
    plot_currency_changes(df, currencies, output_dir)
    
    # Plot 4: Weekend forward-fill visualization
    plot_weekend_fill(df, currencies, output_dir)
    
    print(f"\n✓ All plots saved to {output_dir}/")


def plot_selected_currencies(df, currencies, output_dir):
    """Plot selected currencies on the same chart."""
    print(f"\nPlotting selected currencies: {currencies}")
    
    plt.figure(figsize=(14, 8))
    
    for currency in currencies:
        currency_data = df[df['code'] == currency].copy()
        if currency_data.empty:
            print(f"  ⚠ Warning: No data found for {currency}")
            continue
        
        # Sort by calendar date
        currency_data = currency_data.sort_values('calendar_date')
        
        # Identify forward-filled data (where calendar_date != as_of_date)
        currency_data['is_filled'] = currency_data['calendar_date'] != currency_data['as_of_date']
        
        # Plot original data
        original = currency_data[~currency_data['is_filled']]
        plt.plot(original['calendar_date'], original['rate'], 
                marker='o', label=f"{currency}", linewidth=2, markersize=6)
        
        # Plot filled data with different style
        filled = currency_data[currency_data['is_filled']]
        if not filled.empty:
            plt.plot(filled['calendar_date'], filled['rate'], 
                    linestyle='--', alpha=0.5, linewidth=1)
    
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Exchange Rate (GEL)', fontsize=12)
    plt.title('NBG Currency Exchange Rates Over Time\n(Dashed lines = forward-filled weekend data)', 
              fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.tight_layout()
    
    output_path = output_dir / 'selected_currencies.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def plot_all_currencies(df, output_dir):
    """Plot all currencies in a multi-panel grid."""
    print("\nPlotting all currencies...")
    
    currencies = sorted(df['code'].unique())
    n_currencies = len(currencies)
    
    # Calculate grid dimensions
    n_cols = 4
    n_rows = (n_currencies + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 4 * n_rows))
    axes = axes.flatten() if n_currencies > 1 else [axes]
    
    for idx, currency in enumerate(currencies):
        ax = axes[idx]
        currency_data = df[df['code'] == currency].sort_values('calendar_date')
        
        # Mark filled vs original
        currency_data['is_filled'] = currency_data['calendar_date'] != currency_data['as_of_date']
        
        # Plot
        original = currency_data[~currency_data['is_filled']]
        filled = currency_data[currency_data['is_filled']]
        
        ax.plot(original['calendar_date'], original['rate'], 
               marker='o', color='steelblue', markersize=4, linewidth=1.5)
        
        if not filled.empty:
            ax.plot(filled['calendar_date'], filled['rate'], 
                   linestyle='--', color='steelblue', alpha=0.5, linewidth=1)
        
        ax.set_title(f"{currency} - {currency_data['name'].iloc[0]}", fontsize=9, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45, labelsize=7)
        ax.tick_params(axis='y', labelsize=7)
    
    # Hide unused subplots
    for idx in range(n_currencies, len(axes)):
        axes[idx].axis('off')
    
    plt.suptitle('All NBG Currency Exchange Rates', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    
    output_path = output_dir / 'all_currencies.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def plot_currency_changes(df, currencies, output_dir):
    """Plot daily changes/differences for selected currencies."""
    print(f"\nPlotting currency changes for: {currencies}")
    
    fig, axes = plt.subplots(len(currencies), 1, figsize=(14, 4 * len(currencies)))
    if len(currencies) == 1:
        axes = [axes]
    
    for idx, currency in enumerate(currencies):
        ax = axes[idx]
        currency_data = df[df['code'] == currency].copy()
        
        if currency_data.empty:
            continue
        
        currency_data = currency_data.sort_values('calendar_date')
        currency_data['is_filled'] = currency_data['calendar_date'] != currency_data['as_of_date']
        
        # Only plot changes for non-filled data
        original = currency_data[~currency_data['is_filled']].copy()
        
        # Use the 'diff' field from NBG
        ax.bar(original['calendar_date'], original['diff'], 
              color=['green' if x >= 0 else 'red' for x in original['diff']],
              alpha=0.7, width=0.8)
        
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
        ax.set_title(f"{currency} - Daily Change (GEL)", fontsize=11, fontweight='bold')
        ax.set_ylabel('Change', fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
        ax.tick_params(axis='x', rotation=45, labelsize=9)
    
    axes[-1].set_xlabel('Date', fontsize=11)
    plt.suptitle('Currency Exchange Rate Changes', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    output_path = output_dir / 'currency_changes.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def plot_weekend_fill(df, currencies, output_dir):
    """Visualize weekend forward-fill with color coding."""
    print(f"\nPlotting weekend fill visualization for: {currencies}")
    
    fig, axes = plt.subplots(len(currencies), 1, figsize=(14, 4 * len(currencies)))
    if len(currencies) == 1:
        axes = [axes]
    
    for idx, currency in enumerate(currencies):
        ax = axes[idx]
        currency_data = df[df['code'] == currency].copy()
        
        if currency_data.empty:
            continue
        
        currency_data = currency_data.sort_values('calendar_date')
        currency_data['is_filled'] = currency_data['calendar_date'] != currency_data['as_of_date']
        
        # Separate original and filled
        original = currency_data[~currency_data['is_filled']]
        filled = currency_data[currency_data['is_filled']]
        
        # Plot with different colors
        ax.plot(original['calendar_date'], original['rate'], 
               marker='o', color='steelblue', label='Original NBG Data', 
               markersize=8, linewidth=2, linestyle='-')
        
        if not filled.empty:
            ax.plot(filled['calendar_date'], filled['rate'], 
                   marker='s', color='orange', label='Forward-filled (Weekend)', 
                   markersize=8, linewidth=2, linestyle='--', alpha=0.7)
        
        # Highlight weekends
        for date in currency_data['calendar_date']:
            if date.weekday() in [5, 6]:  # Saturday or Sunday
                ax.axvspan(date - pd.Timedelta(hours=12), 
                          date + pd.Timedelta(hours=12), 
                          alpha=0.1, color='gray')
        
        ax.set_title(f"{currency} - Weekend Forward-Fill Visualization", 
                    fontsize=11, fontweight='bold')
        ax.set_ylabel('Exchange Rate (GEL)', fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45, labelsize=9)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %a'))
    
    axes[-1].set_xlabel('Date', fontsize=11)
    plt.suptitle('Weekend Forward-Fill Pattern', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    output_path = output_dir / 'weekend_fill.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot NBG currency data from forward-filled CSV"
    )
    parser.add_argument(
        "--input-csv",
        type=str,
        default=str(DEFAULT_INPUT_CSV),
        help=f"Input CSV file with forward-filled data (default: {DEFAULT_INPUT_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory for plots (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--currencies",
        type=str,
        nargs='+',
        default=['USD', 'EUR', 'GBP'],
        help="Currency codes to plot in detail (default: USD EUR GBP)",
    )
    
    args = parser.parse_args()
    
    input_csv = Path(args.input_csv)
    output_dir = Path(args.output_dir)
    
    if not input_csv.exists():
        print(f"Error: Input file {input_csv} does not exist.")
        print("Run nbg_forward_fill.py first to generate the filled data.")
        return
    
    try:
        plot_currencies(input_csv, output_dir, args.currencies)
        print("\nDone! Open the plots folder to view the visualizations.")
    except Exception as e:
        print(f"\nError creating plots: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
