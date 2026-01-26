"""
TMDB Movie Analysis Pipeline - Main Orchestrator

This script orchestrates the complete ETL pipeline for movie data analysis:
1. Extract - Fetch movie data from TMDb API
2. Transform - Clean and prepare data using PySpark
3. Analyze - Run KPI analysis and rankings
4. Visualize - Generate charts and save results

Usage:
    python main.py
"""

import os
import sys
import subprocess

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Find the actual Python executable path
python_executable = subprocess.run(['which', 'python3'], capture_output=True, text=True).stdout.strip()
if not python_executable:
    python_executable = sys.executable

# Set PySpark environment variables to use the actual Python path
os.environ['PYSPARK_PYTHON'] = python_executable
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable

from pyspark.sql import SparkSession
from src.extract import fetch_all_movies
from src.transform import clean_and_transform, show_data_summary
from src.analyze import run_all_analysis, display_analysis_results
from src.visualize import save_all_visualizations
from utils.config import OUTPUT_DIR
from utils.logger import setup_logger


def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession for the pipeline.
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName("TMDB Movie Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.shuffle.partitions", "8") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def run_extraction():
    """
    Step 1: Extract movie data from TMDb API.
    
    Returns:
        list: List of raw movie dictionaries from API
    """
    logger.info("=" * 60)
    logger.info("STEP 1: DATA EXTRACTION")
    logger.info("=" * 60)
    
    raw_movies = fetch_all_movies()
    
    logger.info(f"Extracted {len(raw_movies)} movies from API")
    return raw_movies


def run_transformation(spark, raw_movies):
    """
    Step 2: Clean and transform the raw data.
    
    Args:
        spark: SparkSession instance
        raw_movies: List of raw movie dictionaries
        
    Returns:
        DataFrame: Cleaned and transformed DataFrame
    """
    logger.info("=" * 60)
    logger.info("STEP 2: DATA TRANSFORMATION")
    logger.info("=" * 60)
    
    df = clean_and_transform(spark, raw_movies)
    
    # Display summary for verification
    show_data_summary(df)
    
    return df


def run_analysis(df):
    """
    Step 3: Run analysis and generate KPIs.
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        dict: Dictionary containing all analysis results
    """
    logger.info("=" * 60)
    logger.info("STEP 3: DATA ANALYSIS")
    logger.info("=" * 60)
    
    results = run_all_analysis(df)
    
    # Display results in console
    display_analysis_results(results)
    
    return results


def run_visualization(df, analysis_results):
    """
    Step 4: Generate and save visualizations.
    
    Args:
        df: Cleaned DataFrame
        analysis_results: Dictionary of analysis results
        
    Returns:
        dict: Paths to saved visualization files
    """
    logger.info("=" * 60)
    logger.info("STEP 4: DATA VISUALIZATION")
    logger.info("=" * 60)
    
    saved_files = save_all_visualizations(df, analysis_results)
    
    logger.info("Saved visualizations:")
    for name, path in saved_files.items():
        logger.info(f"  - {name}: {path}")
    
    return saved_files


def main():
    """
    Main entry point - runs the complete ETL pipeline.
    """
    print("\n" + "=" * 60)
    print("   TMDB MOVIE ANALYSIS PIPELINE")
    print("=" * 60 + "\n")
    
    # Ensure output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark session...")
        spark = create_spark_session()
        logger.info("Spark session created successfully!")
        
        # Step 1: Extract data from API
        raw_movies = run_extraction()
        
        if not raw_movies:
            logger.error("No movies extracted. Exiting pipeline.")
            return
        
        # Step 2: Transform/clean the data
        df = run_transformation(spark, raw_movies)
        
        # Step 3: Analyze the data
        analysis_results = run_analysis(df)
        
        # Step 4: Create visualizations
        viz_files = run_visualization(df, analysis_results)
        
        # Final summary
        print("\n" + "=" * 60)
        print("   PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"\n Results saved to: {OUTPUT_DIR}/")
        print(f" Visualizations created: {len(viz_files)}")
        print(f" Movies analyzed: {df.count()}")
        print("\n")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        raise
        
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped.")



# Initialize logger
logger = setup_logger()


if __name__ == "__main__":
    main()
