# Use official Spark runtime as base - includes Java and Python
FROM apache/spark:3.4.0-python3

# Install additional dependencies 
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Find Python location
RUN which python3

# Verify Python works
RUN python3 --version

# Set Python path for Spark workers - use absolute path
ENV PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create output directory
RUN mkdir -p output

# Run the pipeline
CMD ["python3", "main.py"]
