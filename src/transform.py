"""
Data Transformation module for TMDB Movie Analysis Pipeline.

This module handles cleaning and transforming raw movie data:
- Extract values from JSON-like columns
- Handle data type conversions
- Clean missing and incorrect data
- Filter and validate rows
- Prepare data for analysis
"""

import json
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, ArrayType, DateType
)

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    COLUMNS_TO_DROP, FINAL_COLUMN_ORDER, MIN_NON_NULL_COLUMNS
)
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger()


# =============================================================================
# JSON Extraction Helper Functions
# =============================================================================

def extract_collection_name(collection_data: Any) -> Optional[str]:
    """
    Extract collection name from belongs_to_collection field.
    
    Args:
        collection_data: Dictionary or None
        
    Returns:
        str or None: Collection name if exists
    """
    if collection_data and isinstance(collection_data, dict):
        return collection_data.get('name')
    return None


def extract_names_pipe_separated(items: Any) -> Optional[str]:
    """
    Extract names from a list of dictionaries and join with '|'.
    
    Used for genres, production_companies, production_countries, spoken_languages.
    
    Args:
        items: List of dictionaries with 'name' key
        
    Returns:
        str or None: Pipe-separated names or None
        
    Example:
        >>> items = [{'name': 'Action'}, {'name': 'Sci-Fi'}]
        >>> extract_names_pipe_separated(items)
        'Action|Sci-Fi'
    """
    if items and isinstance(items, list):
        names = [item.get('name', '') for item in items if isinstance(item, dict)]
        names = [n for n in names if n]  # Filter empty strings
        if names:
            return '|'.join(names)
    return None


def extract_cast_info(credits: Any) -> tuple:
    """
    Extract cast names and cast size from credits.
    
    Args:
        credits: Dictionary with 'cast' list
        
    Returns:
        tuple: (cast_names_string, cast_size)
    """
    if credits and isinstance(credits, dict):
        cast = credits.get('cast', [])
        if cast:
            # Get top 10 cast members for readability
            cast_names = [member.get('name', '') for member in cast[:10] if member.get('name')]
            return ('|'.join(cast_names), len(cast))
    return (None, 0)


def extract_director_info(credits: Any) -> tuple:
    """
    Extract director name and crew size from credits.
    
    Args:
        credits: Dictionary with 'crew' list
        
    Returns:
        tuple: (director_name, crew_size)
    """
    if credits and isinstance(credits, dict):
        crew = credits.get('crew', [])
        crew_size = len(crew)
        
        # Find director(s)
        directors = [
            member.get('name') 
            for member in crew 
            if member.get('job') == 'Director' and member.get('name')
        ]
        
        director_name = '|'.join(directors) if directors else None
        return (director_name, crew_size)
    return (None, 0)


# =============================================================================
# Data Transformation Functions
# =============================================================================

def process_raw_movie_data(movies: List[Dict]) -> List[Dict]:
    """
    Process raw API response data before converting to DataFrame.
    
    Extracts nested JSON fields and prepares data for PySpark.
    
    Args:
        movies: List of raw movie dictionaries from API
        
    Returns:
        List[Dict]: Processed movie dictionaries
    """
    logger.info(f"Processing {len(movies)} raw movie records...")
    
    processed = []
    
    for movie in movies:
        try:
            # Extract credits info
            credits = movie.get('credits', {})
            cast_names, cast_size = extract_cast_info(credits)
            director, crew_size = extract_director_info(credits)
            
            # Build processed record
            record = {
                'id': movie.get('id'),
                'title': movie.get('title'),
                'tagline': movie.get('tagline'),
                'release_date': movie.get('release_date'),
                'status': movie.get('status'),
                'original_language': movie.get('original_language'),
                'budget': movie.get('budget'),
                'revenue': movie.get('revenue'),
                'runtime': movie.get('runtime'),
                'popularity': movie.get('popularity'),
                'vote_average': movie.get('vote_average'),
                'vote_count': movie.get('vote_count'),
                'overview': movie.get('overview'),
                'poster_path': movie.get('poster_path'),
                
                # Extract JSON-like fields
                'belongs_to_collection': extract_collection_name(movie.get('belongs_to_collection')),
                'genres': extract_names_pipe_separated(movie.get('genres')),
                'production_companies': extract_names_pipe_separated(movie.get('production_companies')),
                'production_countries': extract_names_pipe_separated(movie.get('production_countries')),
                'spoken_languages': extract_names_pipe_separated(movie.get('spoken_languages')),
                
                # Credits info
                'cast': cast_names,
                'cast_size': cast_size,
                'director': director,
                'crew_size': crew_size
            }
            
            processed.append(record)
            
        except Exception as e:
            logger.warning(f"Error processing movie {movie.get('id', 'unknown')}: {str(e)}")
            continue
    
    logger.info(f"Successfully processed {len(processed)} records")
    return processed


def create_spark_dataframe(spark: SparkSession, data: List[Dict]) -> DataFrame:
    """
    Create a PySpark DataFrame from processed movie data.
    
    Args:
        spark: SparkSession instance
        data: List of processed movie dictionaries
        
    Returns:
        DataFrame: PySpark DataFrame
    """
    logger.info("Creating PySpark DataFrame...")
    
    # Convert to DataFrame
    df = spark.createDataFrame(data)
    
    logger.info(f"Created DataFrame with {len(data)} rows and {len(df.columns)} columns")
    return df


def convert_datatypes(df: DataFrame) -> DataFrame:
    """
    Convert columns to appropriate data types.
    
    - budget, revenue, popularity -> numeric
    - release_date -> date
    - runtime, vote_count -> integer
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with correct data types
    """
    logger.info("Converting data types...")
    
    df = df.withColumn('budget', F.col('budget').cast('double'))
    df = df.withColumn('revenue', F.col('revenue').cast('double'))
    df = df.withColumn('popularity', F.col('popularity').cast('double'))
    df = df.withColumn('vote_average', F.col('vote_average').cast('double'))
    df = df.withColumn('vote_count', F.col('vote_count').cast('integer'))
    df = df.withColumn('runtime', F.col('runtime').cast('integer'))
    df = df.withColumn('cast_size', F.col('cast_size').cast('integer'))
    df = df.withColumn('crew_size', F.col('crew_size').cast('integer'))
    df = df.withColumn('release_date', F.to_date('release_date', 'yyyy-MM-dd'))
    
    return df


def handle_missing_values(df: DataFrame) -> DataFrame:
    """
    Handle missing and unrealistic values.
    
    - Replace 0 budget/revenue/runtime with null
    - Convert budget/revenue to millions USD
    - Replace placeholder text with null
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Cleaned DataFrame
    """
    logger.info("Handling missing values...")
    
    # Replace 0 values with null for budget, revenue, runtime
    df = df.withColumn('budget', 
        F.when(F.col('budget') == 0, None).otherwise(F.col('budget')))
    df = df.withColumn('revenue', 
        F.when(F.col('revenue') == 0, None).otherwise(F.col('revenue')))
    df = df.withColumn('runtime', 
        F.when(F.col('runtime') == 0, None).otherwise(F.col('runtime')))
    
    # Convert to millions USD
    df = df.withColumn('budget_musd', F.round(F.col('budget') / 1000000, 2))
    df = df.withColumn('revenue_musd', F.round(F.col('revenue') / 1000000, 2))
    
    # Handle empty/placeholder strings
    df = df.withColumn('overview',
        F.when(
            (F.col('overview').isNull()) | 
            (F.trim(F.col('overview')) == '') |
            (F.lower(F.col('overview')) == 'no data'),
            None
        ).otherwise(F.col('overview'))
    )
    
    df = df.withColumn('tagline',
        F.when(
            (F.col('tagline').isNull()) | 
            (F.trim(F.col('tagline')) == '') |
            (F.lower(F.col('tagline')) == 'no data'),
            None
        ).otherwise(F.col('tagline'))
    )
    
    # Drop original budget and revenue columns
    df = df.drop('budget', 'revenue')
    
    return df


def filter_valid_movies(df: DataFrame) -> DataFrame:
    """
    Filter to keep only valid movie rows.
    
    - Remove duplicates
    - Remove rows with null id or title
    - Keep only 'Released' movies
    - Keep rows with enough non-null values
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Filtered DataFrame
    """
    logger.info("Filtering valid movies...")
    
    # Remove duplicates
    df = df.dropDuplicates(['id'])
    
    # Remove rows with null id or title
    df = df.filter(
        (F.col('id').isNotNull()) & 
        (F.col('title').isNotNull()) &
        (F.trim(F.col('title')) != '')
    )
    
    # Keep only released movies
    df = df.filter(F.col('status') == 'Released')
    
    # Drop status column after filtering
    df = df.drop('status')
    
    logger.info("Filtering complete: removed duplicates and invalid entries")
    
    return df


def reorder_columns(df: DataFrame) -> DataFrame:
    """
    Reorder columns to match the required final order.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with reordered columns
    """
    logger.info("Reordering columns...")
    
    # Get available columns that exist in the DataFrame
    available_columns = df.columns
    ordered_columns = [col for col in FINAL_COLUMN_ORDER if col in available_columns]
    
    # Add any remaining columns not in the final order
    remaining_columns = [col for col in available_columns if col not in ordered_columns]
    all_columns = ordered_columns + remaining_columns
    
    return df.select(all_columns)


# =============================================================================
# Main Transformation Function
# =============================================================================

def clean_and_transform(spark: SparkSession, raw_movies: List[Dict]) -> DataFrame:
    """
    Main transformation function that orchestrates all cleaning steps.
    
    This is the primary entry point for the transformation module.
    
    Args:
        spark: SparkSession instance
        raw_movies: List of raw movie dictionaries from API
        
    Returns:
        DataFrame: Cleaned and transformed PySpark DataFrame
        
    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("test").getOrCreate()
        >>> df = clean_and_transform(spark, raw_movies)
        >>> df.show()
    """
    logger.info("=" * 50)
    logger.info("Starting data transformation pipeline...")
    logger.info("=" * 50)
    
    # Step 1: Process raw data (extract JSON fields)
    processed_data = process_raw_movie_data(raw_movies)
    
    # Step 2: Create PySpark DataFrame
    df = create_spark_dataframe(spark, processed_data)
    
    # Step 3: Convert data types
    df = convert_datatypes(df)
    
    # Step 4: Handle missing values and convert to millions
    df = handle_missing_values(df)
    
    # Step 5: Filter valid movies
    df = filter_valid_movies(df)
    
    # Step 6: Reorder columns
    df = reorder_columns(df)
    
    logger.info("=" * 50)
    logger.info("Transformation complete!")
    logger.info("=" * 50)
    
    return df


# =============================================================================
# Utility Functions
# =============================================================================

def show_data_summary(df: DataFrame) -> None:
    """
    Display a summary of the DataFrame for verification.
    
    Args:
        df: DataFrame to summarize
    """
    print("\n" + "=" * 60)
    print("DATA SUMMARY")
    print("=" * 60)
    logger.info("DataFrame created successfully")
    print(f"Total columns: {len(df.columns)}")
    print(f"\nColumns: {df.columns}")
    print("\nSchema:")
    df.printSchema()
    print("\nSample data (first 5 rows):")
    df.select('id', 'title', 'release_date', 'budget_musd', 'revenue_musd').show(5, truncate=False)

