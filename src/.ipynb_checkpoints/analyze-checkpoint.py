"""
Data Analysis module for TMDB Movie Analysis Pipeline.

This module implements KPI calculations and analysis functions:
- Movie ranking by various metrics
- Advanced filtering and search queries
- Franchise vs standalone comparisons
- Director and franchise performance analysis
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType
from typing import Dict, Any, List

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import MIN_BUDGET_FOR_ROI, MIN_VOTES_FOR_RATING
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger()


# =============================================================================
# Ranking Functions
# =============================================================================

def rank_movies(df: DataFrame, column: str, ascending: bool = False, 
                n: int = 10, filter_condition: str = None) -> DataFrame:
    """
    Rank movies by a specific column.
    
    This is a flexible ranking function that can be used for various metrics.
    
    Args:
        df: Input DataFrame
        column: Column to rank by
        ascending: If True, rank from lowest to highest
        n: Number of top/bottom movies to return
        filter_condition: Optional SQL filter condition
        
    Returns:
        DataFrame: Ranked movies
        
    Example:
        >>> top_revenue = rank_movies(df, 'revenue_musd', ascending=False, n=5)
    """
    # Apply filter if provided
    if filter_condition:
        df = df.filter(filter_condition)
    
    # Filter out nulls in the ranking column
    df = df.filter(F.col(column).isNotNull())
    
    # Sort and limit
    if ascending:
        result = df.orderBy(F.col(column).asc()).limit(n)
    else:
        result = df.orderBy(F.col(column).desc()).limit(n)
    
    return result


# =============================================================================
# Best/Worst Movie Rankings
# =============================================================================

def highest_revenue_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N movies by revenue."""
    logger.info(f"Finding top {n} highest revenue movies...")
    return rank_movies(
        df.select('id', 'title', 'release_date', 'revenue_musd', 'budget_musd'),
        'revenue_musd', ascending=False, n=n
    )


def highest_budget_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N movies by budget."""
    logger.info(f"Finding top {n} highest budget movies...")
    return rank_movies(
        df.select('id', 'title', 'release_date', 'budget_musd'),
        'budget_musd', ascending=False, n=n
    )


def highest_profit_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N movies by profit (revenue - budget)."""
    logger.info(f"Finding top {n} highest profit movies...")
    df_with_profit = df.withColumn(
        'profit_musd', 
        F.round(F.col('revenue_musd') - F.col('budget_musd'), 2)
    )
    return rank_movies(
        df_with_profit.select('id', 'title', 'release_date', 'revenue_musd', 
                              'budget_musd', 'profit_musd'),
        'profit_musd', ascending=False, n=n
    )


def lowest_profit_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """Get bottom N movies by profit (biggest losses)."""
    logger.info(f"Finding {n} lowest profit movies...")
    df_with_profit = df.withColumn(
        'profit_musd', 
        F.round(F.col('revenue_musd') - F.col('budget_musd'), 2)
    )
    return rank_movies(
        df_with_profit.select('id', 'title', 'release_date', 'revenue_musd', 
                              'budget_musd', 'profit_musd'),
        'profit_musd', ascending=True, n=n
    )


def highest_roi_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Get top N movies by ROI (Return on Investment).
    Only includes movies with budget >= MIN_BUDGET_FOR_ROI million.
    """
    logger.info(f"Finding top {n} highest ROI movies (budget >= {MIN_BUDGET_FOR_ROI}M)...")
    
    df_filtered = df.filter(F.col('budget_musd') >= MIN_BUDGET_FOR_ROI)
    df_with_roi = df_filtered.withColumn(
        'roi', 
        F.round(F.col('revenue_musd') / F.col('budget_musd'), 2)
    )
    return rank_movies(
        df_with_roi.select('id', 'title', 'release_date', 'revenue_musd', 
                           'budget_musd', 'roi'),
        'roi', ascending=False, n=n
    )


def lowest_roi_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Get bottom N movies by ROI.
    Only includes movies with budget >= MIN_BUDGET_FOR_ROI million.
    """
    logger.info(f"Finding {n} lowest ROI movies (budget >= {MIN_BUDGET_FOR_ROI}M)...")
    
    df_filtered = df.filter(F.col('budget_musd') >= MIN_BUDGET_FOR_ROI)
    df_with_roi = df_filtered.withColumn(
        'roi', 
        F.round(F.col('revenue_musd') / F.col('budget_musd'), 2)
    )
    return rank_movies(
        df_with_roi.select('id', 'title', 'release_date', 'revenue_musd', 
                           'budget_musd', 'roi'),
        'roi', ascending=True, n=n
    )


def most_voted_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N movies by vote count."""
    logger.info(f"Finding top {n} most voted movies...")
    return rank_movies(
        df.select('id', 'title', 'release_date', 'vote_count', 'vote_average'),
        'vote_count', ascending=False, n=n
    )


def highest_rated_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Get top N highest rated movies.
    Only includes movies with vote_count >= MIN_VOTES_FOR_RATING.
    """
    logger.info(f"Finding top {n} highest rated movies (votes >= {MIN_VOTES_FOR_RATING})...")
    
    df_filtered = df.filter(F.col('vote_count') >= MIN_VOTES_FOR_RATING)
    return rank_movies(
        df_filtered.select('id', 'title', 'release_date', 'vote_average', 'vote_count'),
        'vote_average', ascending=False, n=n
    )


def lowest_rated_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Get bottom N lowest rated movies.
    Only includes movies with vote_count >= MIN_VOTES_FOR_RATING.
    """
    logger.info(f"Finding {n} lowest rated movies (votes >= {MIN_VOTES_FOR_RATING})...")
    
    df_filtered = df.filter(F.col('vote_count') >= MIN_VOTES_FOR_RATING)
    return rank_movies(
        df_filtered.select('id', 'title', 'release_date', 'vote_average', 'vote_count'),
        'vote_average', ascending=True, n=n
    )


def most_popular_movies(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N movies by popularity."""
    logger.info(f"Finding top {n} most popular movies...")
    return rank_movies(
        df.select('id', 'title', 'release_date', 'popularity', 'vote_average'),
        'popularity', ascending=False, n=n
    )


# =============================================================================
# Advanced Search Functions
# =============================================================================

def search_scifi_action_bruce_willis(df: DataFrame) -> DataFrame:
    """
    Search 1: Find the best-rated Science Fiction Action movies 
    starring Bruce Willis, sorted by rating (highest to lowest).
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Matching movies sorted by rating
    """
    logger.info("Searching for Sci-Fi Action movies with Bruce Willis...")
    
    result = df.filter(
        (F.col('genres').contains('Science Fiction')) &
        (F.col('genres').contains('Action')) &
        (F.col('cast').contains('Bruce Willis'))
    ).select(
        'id', 'title', 'release_date', 'genres', 'cast', 
        'vote_average', 'vote_count'
    ).orderBy(F.col('vote_average').desc())
    
    count = result.count()
    logger.info(f"Found {count} matching movies")
    
    return result


def search_uma_tarantino(df: DataFrame) -> DataFrame:
    """
    Search 2: Find movies starring Uma Thurman directed by Quentin Tarantino,
    sorted by runtime (shortest to longest).
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Matching movies sorted by runtime
    """
    logger.info("Searching for Uma Thurman + Quentin Tarantino movies...")
    
    result = df.filter(
        (F.col('cast').contains('Uma Thurman')) &
        (F.col('director').contains('Quentin Tarantino'))
    ).select(
        'id', 'title', 'release_date', 'director', 'cast', 
        'runtime', 'vote_average'
    ).orderBy(F.col('runtime').asc())
    
    count = result.count()
    logger.info(f"Found {count} matching movies")
    
    return result


# =============================================================================
# Franchise vs Standalone Analysis
# =============================================================================

def franchise_vs_standalone_analysis(df: DataFrame) -> Dict[str, Any]:
    """
    Compare franchise movies vs standalone movies.
    
    Metrics compared:
    - Mean Revenue
    - Median ROI
    - Mean Budget
    - Mean Popularity
    - Mean Rating
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dict: Comparison results
    """
    logger.info("Analyzing franchise vs standalone movies...")
    
    # Add ROI column
    df = df.withColumn(
        'roi', 
        F.when(F.col('budget_musd') > 0, 
               F.col('revenue_musd') / F.col('budget_musd'))
    )
    
    # Split into franchise and standalone
    franchise_df = df.filter(F.col('belongs_to_collection').isNotNull())
    standalone_df = df.filter(F.col('belongs_to_collection').isNull())
    
    # Calculate metrics for franchises
    franchise_stats = franchise_df.agg(
        F.mean('revenue_musd').alias('mean_revenue'),
        F.expr('percentile_approx(roi, 0.5)').alias('median_roi'),
        F.mean('budget_musd').alias('mean_budget'),
        F.mean('popularity').alias('mean_popularity'),
        F.mean('vote_average').alias('mean_rating'),
        F.count('*').alias('count')
    ).collect()[0]
    
    # Calculate metrics for standalone
    standalone_stats = standalone_df.agg(
        F.mean('revenue_musd').alias('mean_revenue'),
        F.expr('percentile_approx(roi, 0.5)').alias('median_roi'),
        F.mean('budget_musd').alias('mean_budget'),
        F.mean('popularity').alias('mean_popularity'),
        F.mean('vote_average').alias('mean_rating'),
        F.count('*').alias('count')
    ).collect()[0]
    
    results = {
        'franchise': {
            'count': franchise_stats['count'],
            'mean_revenue_musd': round(franchise_stats['mean_revenue'] or 0, 2),
            'median_roi': round(franchise_stats['median_roi'] or 0, 2),
            'mean_budget_musd': round(franchise_stats['mean_budget'] or 0, 2),
            'mean_popularity': round(franchise_stats['mean_popularity'] or 0, 2),
            'mean_rating': round(franchise_stats['mean_rating'] or 0, 2)
        },
        'standalone': {
            'count': standalone_stats['count'],
            'mean_revenue_musd': round(standalone_stats['mean_revenue'] or 0, 2),
            'median_roi': round(standalone_stats['median_roi'] or 0, 2),
            'mean_budget_musd': round(standalone_stats['mean_budget'] or 0, 2),
            'mean_popularity': round(standalone_stats['mean_popularity'] or 0, 2),
            'mean_rating': round(standalone_stats['mean_rating'] or 0, 2)
        }
    }
    
    logger.info(f"Franchise movies: {results['franchise']['count']}, "
                f"Standalone movies: {results['standalone']['count']}")
    
    return results


def most_successful_franchises(df: DataFrame) -> DataFrame:
    """
    Find the most successful movie franchises.
    
    Metrics:
    - Total number of movies in franchise
    - Total & Mean Budget
    - Total & Mean Revenue
    - Mean Rating
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Franchise rankings
    """
    logger.info("Analyzing most successful franchises...")
    
    # Filter to franchise movies only
    franchise_df = df.filter(F.col('belongs_to_collection').isNotNull())
    
    # Group by collection and calculate metrics
    result = franchise_df.groupBy('belongs_to_collection').agg(
        F.count('*').alias('movie_count'),
        F.round(F.sum('budget_musd'), 2).alias('total_budget_musd'),
        F.round(F.mean('budget_musd'), 2).alias('mean_budget_musd'),
        F.round(F.sum('revenue_musd'), 2).alias('total_revenue_musd'),
        F.round(F.mean('revenue_musd'), 2).alias('mean_revenue_musd'),
        F.round(F.mean('vote_average'), 2).alias('mean_rating')
    ).orderBy(F.col('total_revenue_musd').desc())
    
    return result


def most_successful_directors(df: DataFrame) -> DataFrame:
    """
    Find the most successful directors.
    
    Metrics:
    - Total number of movies directed
    - Total Revenue
    - Mean Rating
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Director rankings
    """
    logger.info("Analyzing most successful directors...")
    
    # Filter to movies with directors
    director_df = df.filter(F.col('director').isNotNull())
    
    # Group by director and calculate metrics
    result = director_df.groupBy('director').agg(
        F.count('*').alias('movie_count'),
        F.round(F.sum('revenue_musd'), 2).alias('total_revenue_musd'),
        F.round(F.mean('vote_average'), 2).alias('mean_rating')
    ).orderBy(F.col('total_revenue_musd').desc())
    
    return result


# =============================================================================
# Main Analysis Function
# =============================================================================

def run_all_analysis(df: DataFrame) -> Dict[str, Any]:
    """
    Run all analysis functions and return results.
    
    This is the main entry point for the analysis module.
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        Dict: Dictionary containing all analysis results
    """
    logger.info("=" * 50)
    logger.info("Starting comprehensive movie analysis...")
    logger.info("=" * 50)
    
    results = {
        # Best/Worst rankings
        'highest_revenue': highest_revenue_movies(df, 5),
        'highest_budget': highest_budget_movies(df, 5),
        'highest_profit': highest_profit_movies(df, 5),
        'lowest_profit': lowest_profit_movies(df, 5),
        'highest_roi': highest_roi_movies(df, 5),
        'lowest_roi': lowest_roi_movies(df, 5),
        'most_voted': most_voted_movies(df, 5),
        'highest_rated': highest_rated_movies(df, 5),
        'lowest_rated': lowest_rated_movies(df, 5),
        'most_popular': most_popular_movies(df, 5),
        
        # Advanced searches
        'bruce_willis_scifi': search_scifi_action_bruce_willis(df),
        'uma_tarantino': search_uma_tarantino(df),
        
        # Comparisons
        'franchise_vs_standalone': franchise_vs_standalone_analysis(df),
        'successful_franchises': most_successful_franchises(df),
        'successful_directors': most_successful_directors(df)
    }
    
    logger.info("=" * 50)
    logger.info("Analysis complete!")
    logger.info("=" * 50)
    
    return results


def display_analysis_results(results: Dict[str, Any]) -> None:
    """
    Display analysis results in a formatted manner.
    
    Args:
        results: Dictionary of analysis results
    """
    print("\n" + "=" * 70)
    print("MOVIE ANALYSIS RESULTS")
    print("=" * 70)
    
    # Display DataFrame results
    df_results = [
        ('Top 5 Highest Revenue Movies', 'highest_revenue'),
        ('Top 5 Highest Budget Movies', 'highest_budget'),
        ('Top 5 Highest Profit Movies', 'highest_profit'),
        ('Top 5 Highest ROI Movies', 'highest_roi'),
        ('Top 5 Most Voted Movies', 'most_voted'),
        ('Top 5 Highest Rated Movies', 'highest_rated'),
        ('Top 5 Most Popular Movies', 'most_popular'),
    ]
    
    for title, key in df_results:
        if key in results and results[key].count() > 0:
            print(f"\n{title}:")
            print("-" * 50)
            results[key].show(truncate=False)
    
    # Display franchise vs standalone comparison
    if 'franchise_vs_standalone' in results:
        comparison = results['franchise_vs_standalone']
        print("\nFranchise vs Standalone Comparison:")
        print("-" * 50)
        print(f"{'Metric':<25} {'Franchise':<15} {'Standalone':<15}")
        print("-" * 55)
        for metric in ['mean_revenue_musd', 'median_roi', 'mean_budget_musd', 
                       'mean_popularity', 'mean_rating']:
            print(f"{metric:<25} {comparison['franchise'][metric]:<15} "
                  f"{comparison['standalone'][metric]:<15}")
    
    # Display successful franchises
    if 'successful_franchises' in results:
        print("\nMost Successful Franchises:")
        print("-" * 50)
        results['successful_franchises'].show(truncate=False)
    
    # Display successful directors
    if 'successful_directors' in results:
        print("\nMost Successful Directors:")
        print("-" * 50)
        results['successful_directors'].show(truncate=False)
