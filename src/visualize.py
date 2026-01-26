"""
Data Visualization module for TMDB Movie Analysis Pipeline.

This module creates visualizations for movie analysis:
- Revenue vs Budget trends
- ROI distribution by genre
- Popularity vs Rating
- Yearly box office trends
- Franchise vs Standalone comparison
"""

import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import OUTPUT_DIR
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger()


def setup_style():
    """
    Set up matplotlib style for clean, professional charts.
    """
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams['figure.figsize'] = (10, 6)
    plt.rcParams['font.size'] = 11
    plt.rcParams['axes.titlesize'] = 14
    plt.rcParams['axes.labelsize'] = 12
    plt.rcParams['legend.fontsize'] = 10
    plt.rcParams['figure.dpi'] = 100
    
    # Use a nice color palette
    sns.set_palette("husl")


def ensure_output_dir():
    """Ensure the output directory exists."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def plot_revenue_vs_budget(df: DataFrame, output_path: str = None) -> None:
    """
    Create a scatter plot of Revenue vs Budget with trend line.
    
    Args:
        df: PySpark DataFrame with budget_musd and revenue_musd columns
        output_path: Path to save the plot (optional)
    """
    logger.info("Creating Revenue vs Budget visualization...")
    
    try:
        # Convert to Pandas for plotting using collect() instead of toPandas()
        rows = df.select('title', 'budget_musd', 'revenue_musd').collect()
        plot_df = pd.DataFrame([row.asDict() for row in rows])
        plot_df = plot_df.dropna()
        
        if plot_df.empty:
            logger.warning("No data available for Revenue vs Budget plot")
            return
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Scatter plot
        scatter = ax.scatter(
            plot_df['budget_musd'], 
            plot_df['revenue_musd'],
            c='steelblue',
            alpha=0.7,
            s=100,
            edgecolors='white',
            linewidth=0.5
        )
        
        # Add trend line
        z = np.polyfit(plot_df['budget_musd'], plot_df['revenue_musd'], 1)
        p = np.poly1d(z)
        x_trend = np.linspace(plot_df['budget_musd'].min(), plot_df['budget_musd'].max(), 100)
        ax.plot(x_trend, p(x_trend), "r--", alpha=0.8, linewidth=2, label='Trend Line')
        
        # Add movie labels for notable points
        for idx, row in plot_df.nlargest(5, 'revenue_musd').iterrows():
            ax.annotate(
                row['title'][:15] + '...' if len(row['title']) > 15 else row['title'],
                (row['budget_musd'], row['revenue_musd']),
                textcoords="offset points",
                xytext=(5, 5),
                fontsize=8,
                alpha=0.8
            )
        
        ax.set_xlabel('Budget (Million USD)')
        ax.set_ylabel('Revenue (Million USD)')
        ax.set_title('Movie Revenue vs Budget')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved plot to {output_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error in plot_revenue_vs_budget: {str(e)}")


def plot_roi_by_genre(df: DataFrame, output_path: str = None) -> None:
    """
    Create a bar chart showing ROI distribution by genre.
    
    Args:
        df: PySpark DataFrame with genres, budget_musd, revenue_musd columns
        output_path: Path to save the plot (optional)
    """
    logger.info("Creating ROI by Genre visualization...")
    
    try:
        # Calculate ROI
        df_roi = df.withColumn(
            'roi', 
            F.when(F.col('budget_musd') > 0, 
                   F.col('revenue_musd') / F.col('budget_musd'))
        )
        
        # Explode genres (split by |)
        df_exploded = df_roi.withColumn(
            'genre', 
            F.explode(F.split(F.col('genres'), '\\|'))
        ).filter(F.col('genre').isNotNull())
        
        # Calculate mean ROI by genre
        genre_agg = df_exploded.groupBy('genre').agg(
            F.round(F.mean('roi'), 2).alias('mean_roi'),
            F.count('*').alias('movie_count')
        ).filter(F.col('movie_count') >= 1)
        
        # Convert to Pandas using collect() instead of toPandas()
        rows = genre_agg.collect()
        genre_roi = pd.DataFrame([row.asDict() for row in rows])
        
        if genre_roi.empty:
            logger.warning("No data available for ROI by Genre plot")
            return
        
        # Sort by ROI
        genre_roi = genre_roi.sort_values('mean_roi', ascending=False)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        colors = sns.color_palette("viridis", len(genre_roi))
        bars = ax.bar(genre_roi['genre'], genre_roi['mean_roi'], color=colors)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.1f}x',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)
        
        ax.set_xlabel('Genre')
        ax.set_ylabel('Average ROI (Revenue/Budget)')
        ax.set_title('Return on Investment by Movie Genre')
        ax.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='Break-even')
        ax.legend()
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved plot to {output_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error in plot_roi_by_genre: {str(e)}")
    
    plt.close()


def plot_popularity_vs_rating(df: DataFrame, output_path: str = None) -> None:
    """
    Create a scatter plot of Popularity vs Rating.
    
    Args:
        df: PySpark DataFrame with popularity and vote_average columns
        output_path: Path to save the plot (optional)
    """
    logger.info("Creating Popularity vs Rating visualization...")
    
    try:
        # Convert to Pandas for plotting using collect() instead of toPandas()
        rows = df.select('title', 'popularity', 'vote_average', 'vote_count').collect()
        plot_df = pd.DataFrame([row.asDict() for row in rows])
        plot_df = plot_df.dropna()
        
        if plot_df.empty:
            logger.warning("No data available for Popularity vs Rating plot")
            return
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Use vote count for point size
        sizes = plot_df['vote_count'] / plot_df['vote_count'].max() * 300 + 50
        
        scatter = ax.scatter(
            plot_df['vote_average'], 
            plot_df['popularity'],
            s=sizes,
            c=plot_df['vote_count'],
            cmap='plasma',
            alpha=0.7,
            edgecolors='white',
            linewidth=0.5
        )
        
        # Add colorbar
        cbar = plt.colorbar(scatter)
        cbar.set_label('Vote Count')
        
        # Add labels for notable movies
        for idx, row in plot_df.nlargest(5, 'popularity').iterrows():
            ax.annotate(
                row['title'][:15] + '...' if len(row['title']) > 15 else row['title'],
                (row['vote_average'], row['popularity']),
                textcoords="offset points",
                xytext=(5, 5),
                fontsize=8,
                alpha=0.8
            )
        
        ax.set_xlabel('Rating (Vote Average)')
        ax.set_ylabel('Popularity')
        ax.set_title('Movie Popularity vs Rating\n(Point size = Vote Count)')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved plot to {output_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error in plot_popularity_vs_rating: {str(e)}")


def plot_yearly_trends(df: DataFrame, output_path: str = None) -> None:
    """
    Create a line chart showing yearly box office trends.
    
    Args:
        df: PySpark DataFrame with release_date and revenue_musd columns
        output_path: Path to save the plot (optional)
    """
    logger.info("Creating Yearly Trends visualization...")
    
    try:
        # Extract year and calculate yearly stats
        df_year = df.withColumn('year', F.year('release_date'))
        
        yearly_agg = df_year.groupBy('year').agg(
            F.round(F.sum('revenue_musd'), 2).alias('total_revenue'),
            F.round(F.mean('revenue_musd'), 2).alias('avg_revenue'),
            F.count('*').alias('movie_count')
        ).filter(F.col('year').isNotNull()).orderBy('year')
        
        # Convert to Pandas using collect() instead of toPandas()
        rows = yearly_agg.collect()
        yearly_stats = pd.DataFrame([row.asDict() for row in rows])
        
        if yearly_stats.empty:
            logger.warning("No data available for Yearly Trends plot")
            return
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # Plot total revenue as bars
        color1 = 'steelblue'
        bars = ax1.bar(yearly_stats['year'].astype(str), yearly_stats['total_revenue'], 
                       color=color1, alpha=0.7, label='Total Revenue')
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Total Revenue (Million USD)', color=color1)
        ax1.tick_params(axis='y', labelcolor=color1)
        
        # Create second y-axis for average revenue
        ax2 = ax1.twinx()
        color2 = 'crimson'
        ax2.plot(yearly_stats['year'].astype(str), yearly_stats['avg_revenue'], 
                 color=color2, marker='o', linewidth=2, markersize=8, label='Avg Revenue')
        ax2.set_ylabel('Average Revenue (Million USD)', color=color2)
        ax2.tick_params(axis='y', labelcolor=color2)
        
        # Add movie count annotations
        for i, (idx, row) in enumerate(yearly_stats.iterrows()):
            ax1.annotate(f"n={int(row['movie_count'])}",
                         xy=(i, row['total_revenue']),
                         xytext=(0, 5),
                         textcoords="offset points",
                         ha='center', va='bottom', fontsize=8, color='gray')
        
        ax1.set_title('Yearly Box Office Performance')
        plt.xticks(rotation=45, ha='right')
        
        # Combined legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved plot to {output_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error in plot_yearly_trends: {str(e)}")


def plot_franchise_vs_standalone(comparison_data: Dict[str, Any], 
                                  output_path: str = None) -> None:
    """
    Create a comparison bar chart for franchise vs standalone movies.
    
    Args:
        comparison_data: Dictionary with 'franchise' and 'standalone' keys
        output_path: Path to save the plot (optional)
    """
    logger.info("Creating Franchise vs Standalone visualization...")
    
    metrics = ['mean_revenue_musd', 'mean_budget_musd', 'median_roi', 
               'mean_popularity', 'mean_rating']
    labels = ['Avg Revenue\n(M USD)', 'Avg Budget\n(M USD)', 'Median ROI', 
              'Avg Popularity', 'Avg Rating']
    
    franchise_values = [comparison_data['franchise'][m] for m in metrics]
    standalone_values = [comparison_data['standalone'][m] for m in metrics]
    
    x = range(len(metrics))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    bars1 = ax.bar([i - width/2 for i in x], franchise_values, width, 
                   label=f"Franchise (n={comparison_data['franchise']['count']})",
                   color='steelblue', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], standalone_values, width,
                   label=f"Standalone (n={comparison_data['standalone']['count']})",
                   color='coral', alpha=0.8)
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.1f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)
    
    ax.set_ylabel('Value')
    ax.set_title('Franchise vs Standalone Movie Performance')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"Saved plot to {output_path}")
    
    plt.close()


# =============================================================================
# Main Visualization Function
# =============================================================================

def save_all_visualizations(df: DataFrame, 
                            analysis_results: Dict[str, Any] = None) -> Dict[str, str]:
    """
    Generate and save all visualizations.
    
    This is the main entry point for the visualization module.
    
    Args:
        df: Cleaned DataFrame
        analysis_results: Optional dictionary with analysis results
        
    Returns:
        Dict: Paths to generated visualization files
    """
    # Import numpy here to avoid issues if not installed
    global np
    import numpy as np
    
    logger.info("=" * 50)
    logger.info("Generating visualizations...")
    logger.info("=" * 50)
    
    setup_style()
    ensure_output_dir()
    
    saved_files = {}
    
    # 1. Revenue vs Budget
    try:
        path = os.path.join(OUTPUT_DIR, 'revenue_vs_budget.png')
        plot_revenue_vs_budget(df, path)
        saved_files['revenue_vs_budget'] = path
    except Exception as e:
        logger.error(f"Error creating Revenue vs Budget plot: {e}")
    
    # 2. ROI by Genre
    try:
        path = os.path.join(OUTPUT_DIR, 'roi_by_genre.png')
        plot_roi_by_genre(df, path)
        saved_files['roi_by_genre'] = path
    except Exception as e:
        logger.error(f"Error creating ROI by Genre plot: {e}")
    
    # 3. Popularity vs Rating
    try:
        path = os.path.join(OUTPUT_DIR, 'popularity_vs_rating.png')
        plot_popularity_vs_rating(df, path)
        saved_files['popularity_vs_rating'] = path
    except Exception as e:
        logger.error(f"Error creating Popularity vs Rating plot: {e}")
    
    # 4. Yearly Trends
    try:
        path = os.path.join(OUTPUT_DIR, 'yearly_trends.png')
        plot_yearly_trends(df, path)
        saved_files['yearly_trends'] = path
    except Exception as e:
        logger.error(f"Error creating Yearly Trends plot: {e}")
    
    # 5. Franchise vs Standalone (if analysis results provided)
    if analysis_results and 'franchise_vs_standalone' in analysis_results:
        try:
            path = os.path.join(OUTPUT_DIR, 'franchise_vs_standalone.png')
            plot_franchise_vs_standalone(analysis_results['franchise_vs_standalone'], path)
            saved_files['franchise_vs_standalone'] = path
        except Exception as e:
            logger.error(f"Error creating Franchise vs Standalone plot: {e}")
    
    logger.info("=" * 50)
    logger.info(f"Saved {len(saved_files)} visualizations to '{OUTPUT_DIR}/'")
    logger.info("=" * 50)
    
    return saved_files
