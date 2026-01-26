"""
Configuration settings for TMDB Movie Analysis Pipeline.

This module contains all the configuration constants used across the pipeline,
including API settings, movie IDs, and retry/rate limit parameters.
"""

# =============================================================================
# TMDB API Configuration
# =============================================================================

API_KEY = '33c3009d4610d7c8a3484f3bc865055b'
BASE_URL = 'https://api.themoviedb.org/3'

# Movie IDs to fetch (as specified in the task)
# Note: ID 0 is invalid and will be skipped during extraction
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 
    24428, 168259, 99861, 284054, 12445, 181808, 330457, 
    351286, 109445, 321612, 260513
]

# =============================================================================
# Request Configuration
# =============================================================================

# Retry settings with exponential backoff
MAX_RETRIES = 3          # Maximum number of retry attempts
BASE_DELAY = 2           # Base delay in seconds for exponential backoff
MAX_DELAY = 30           # Maximum delay between retries

# Rate limiting (TMDb allows ~40 requests per second)
RATE_LIMIT = 40          # Requests per second
REQUEST_INTERVAL = 1.0 / RATE_LIMIT  # Interval between requests

# Timeout settings
REQUEST_TIMEOUT = 10     # Timeout in seconds for each API request

# =============================================================================
# Data Processing Configuration
# =============================================================================

# Columns to drop during cleaning
COLUMNS_TO_DROP = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

# JSON-like columns that need extraction
JSON_COLUMNS = [
    'belongs_to_collection', 
    'genres', 
    'production_countries', 
    'production_companies', 
    'spoken_languages'
]

# Final column order for the cleaned DataFrame
FINAL_COLUMN_ORDER = [
    'id', 'title', 'tagline', 'release_date', 'genres', 
    'belongs_to_collection', 'original_language', 'budget_musd', 
    'revenue_musd', 'production_companies', 'production_countries', 
    'vote_count', 'vote_average', 'popularity', 'runtime', 
    'overview', 'spoken_languages', 'poster_path', 
    'cast', 'cast_size', 'director', 'crew_size'
]

# Minimum non-null columns required for a row to be kept
MIN_NON_NULL_COLUMNS = 10

# Minimum budget for ROI calculations (in millions USD)
MIN_BUDGET_FOR_ROI = 10

# Minimum votes required for rating analysis
MIN_VOTES_FOR_RATING = 10

# =============================================================================
# Output Configuration
# =============================================================================

# Output directory for visualizations and logs
OUTPUT_DIR = 'output'
LOG_FILE = 'output/pipeline.log'
