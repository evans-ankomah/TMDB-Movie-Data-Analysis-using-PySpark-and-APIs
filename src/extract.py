"""
Data Extraction module for TMDB Movie Analysis Pipeline.

This module handles fetching movie data from the TMDb API with:
- Retry mechanism with exponential backoff
- Rate limiting to respect API limits
- Timeout handling for slow requests
- Comprehensive error logging
"""

import time
import requests
from typing import List, Dict, Optional, Any

# Import configuration and logger
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    API_KEY, BASE_URL, MOVIE_IDS,
    MAX_RETRIES, BASE_DELAY, MAX_DELAY,
    REQUEST_TIMEOUT, REQUEST_INTERVAL
)
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger()


def fetch_movie_details(movie_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch details for a single movie from TMDb API, including credits.
    
    Uses append_to_response to get movie details and credits in one request.
    Implements retry logic with exponential backoff for resilient fetching.
    
    Args:
        movie_id (int): The TMDb movie ID to fetch
        
    Returns:
        Optional[Dict]: Movie data dictionary if successful, None if failed
        
    Example:
        >>> movie = fetch_movie_details(299534)  # Avengers: Endgame
        >>> print(movie['title'])
        'Avengers: Endgame'
    """
    # Skip invalid movie IDs
    if movie_id <= 0:
        logger.warning(f"Skipping invalid movie ID: {movie_id}")
        return None
    
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {
        'api_key': API_KEY,
        'append_to_response': 'credits'  # Get cast and crew in same request
    }
    
    # Retry loop with exponential backoff
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.debug(f"Fetching movie ID {movie_id} (attempt {attempt + 1}/{MAX_RETRIES + 1})")
            
            response = requests.get(
                url, 
                params=params, 
                timeout=REQUEST_TIMEOUT
            )
            
            # Handle different HTTP status codes
            if response.status_code == 200:
                logger.info(f"Successfully fetched movie ID {movie_id}")
                return response.json()
            
            elif response.status_code == 404:
                logger.warning(f"Movie ID {movie_id} not found (404)")
                return None
            
            elif response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get('Retry-After', BASE_DELAY))
                logger.warning(f"Rate limited. Waiting {retry_after}s before retry...")
                time.sleep(retry_after)
                continue
            
            else:
                logger.error(f"HTTP {response.status_code} for movie ID {movie_id}")
                response.raise_for_status()
                
        except requests.exceptions.Timeout:
            delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
            logger.warning(f"Timeout for movie ID {movie_id}. Retrying in {delay}s...")
            time.sleep(delay)
            
        except requests.exceptions.RequestException as e:
            delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
            logger.error(f"Request error for movie ID {movie_id}: {str(e)}")
            
            if attempt < MAX_RETRIES:
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"All retries exhausted for movie ID {movie_id}")
                return None
    
    return None


def fetch_all_movies(movie_ids: List[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch details for multiple movies from TMDb API.
    
    Implements rate limiting and progress logging for batch fetching.
    
    Args:
        movie_ids (List[int], optional): List of movie IDs to fetch. 
                                         Defaults to MOVIE_IDS from config.
        
    Returns:
        List[Dict]: List of movie data dictionaries
        
    Example:
        >>> movies = fetch_all_movies([299534, 19995])
        >>> len(movies)
        2
    """
    if movie_ids is None:
        movie_ids = MOVIE_IDS
    
    logger.info(f"Starting extraction for {len(movie_ids)} movies...")
    logger.info("=" * 50)
    
    movies = []
    successful = 0
    failed = 0
    
    for i, movie_id in enumerate(movie_ids, 1):
        # Rate limiting - wait between requests
        if i > 1:
            time.sleep(REQUEST_INTERVAL)
        
        # Fetch movie details
        movie_data = fetch_movie_details(movie_id)
        
        if movie_data:
            movies.append(movie_data)
            successful += 1
            logger.info(f"[{i}/{len(movie_ids)}] Fetched: {movie_data.get('title', 'Unknown')}")
        else:
            failed += 1
            logger.warning(f"[{i}/{len(movie_ids)}] Failed to fetch movie ID: {movie_id}")
    
    # Log summary
    logger.info("=" * 50)
    logger.info(f"Extraction complete: {successful} successful, {failed} failed")
    logger.info(f"Total movies fetched: {len(movies)}")
    
    return movies


# =============================================================================
# Standalone execution for testing
# =============================================================================

if __name__ == "__main__":
    # Test with a small subset of movies
    test_ids = [299534, 19995, 140607]  # Endgame, Avatar, Force Awakens
    
    print("Testing TMDB API extraction...")
    print("-" * 40)
    
    movies = fetch_all_movies(test_ids)
    
    print("\nFetched movies:")
    for movie in movies:
        print(f"  - {movie.get('title')} ({movie.get('release_date', 'N/A')[:4]})")
