# Running TMDB PySpark Pipeline with Docker

This guide explains how to run the TMDB Movie Analysis Pipeline using Docker, which provides a stable Linux environment for PySpark.

## Prerequisites

- Docker Desktop installed and running
- All dependencies listed in `requirements.txt` (will be installed automatically in Docker)

## Quick Start

### Option 1: Using Docker Compose (Recommended)

```bash
# Build and run the pipeline
docker-compose up --build

# To run again without rebuilding
docker-compose up

# To clean up containers
docker-compose down
```

### Option 2: Using Docker Commands Directly

```bash
# Build the Docker image
docker build -t tmdb-pyspark-pipeline .

# Run the pipeline
docker run --rm \
  -v $(pwd):/app \
  -v $(pwd)/output:/app/output \
  tmdb-pyspark-pipeline
```

### Option 3: Running on WSL2 (If You Prefer Native Linux)

```bash
# Open WSL2 terminal
wsl

# Navigate to your project
cd /mnt/c/Users/EvansAnkomah/Downloads/py/TMBD\ with\ PySpark

# Install Python dependencies (if not already installed)
pip install -r requirements.txt

# Run the pipeline
python main.py
```

## What the Docker Setup Does

1. **Base Image**: Uses Python 3.11 slim on Linux
2. **Java Installation**: Installs OpenJDK 21 (required for PySpark)
3. **Dependencies**: Installs all Python packages from requirements.txt
4. **Environment Setup**: Automatically configures PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON
5. **Output Directory**: Creates and mounts an output volume for results

## Output Files

After running, you'll find:
- `output/pipeline.log` - Detailed execution log
- `output/*.csv` - Analysis results
- `output/*.png` - Visualization charts

## Troubleshooting

### Docker build fails
```bash
# Clean up and rebuild
docker-compose down -v
docker-compose up --build
```

### Permission issues with output files
The container runs as root by default. If you need to modify output permissions:
```bash
sudo chown -R $USER:$USER output/
```

### Check container logs
```bash
docker-compose logs -f tmdb-pipeline
```

## Performance Notes

- First run will take longer (Docker image building and PySpark initialization)
- Subsequent runs will be faster
- The pipeline processes 18 movies from the TMDB API
- Total execution time: ~2-3 minutes

## Environment Variables

All required environment variables are automatically configured in the Docker container:
- `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`
- `PYSPARK_PYTHON=/usr/local/bin/python`
- `PYSPARK_DRIVER_PYTHON=/usr/local/bin/python`

No Windows-specific configuration needed!
