# TMDB Movie Analysis Pipeline - Setup & Run Guide

A complete ETL pipeline for analyzing movie data from the TMDb API using PySpark. This guide walks you through installation, configuration, and execution from scratch.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation Steps](#installation-steps)
3. [Configuration](#configuration)
4. [Running the Pipeline](#running-the-pipeline)
5. [Project Structure](#project-structure)
6. [Understanding the Pipeline](#understanding-the-pipeline)
7. [Troubleshooting](#troubleshooting)
8. [Output & Results](#output--results)

---

## 📌 Prerequisites

Before starting, ensure you have:

- **Python 3.8 or higher** installed on your system
- **pip** (Python package manager)
- **Java 11 or higher** (required for PySpark)
- **A TMDb API key** (obtain for free at https://www.themoviedb.org/settings/api)
- **Minimum 4GB RAM** (recommended 8GB+ for Spark)
- **~2GB disk space** for dependencies and output files

---

## 🔧 Installation Steps

### Step 1: Navigate to Project Directory

```bash
cd "c:\Users\EvansAnkomah\Downloads\py\TMBD with PySpark"
```

### Step 2: Create a Python Virtual Environment (Optional but Recommended)

Creating a virtual environment keeps your dependencies isolated from the system Python:

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate

# On macOS/Linux:
source venv/bin/activate
```

### Step 3: Upgrade pip

```bash
python -m pip install --upgrade pip
```

### Step 4: Install Dependencies

Install all required packages from the requirements.txt file:

```bash
pip install -r requirements.txt
```

This will install:
- **pyspark** (>=3.4.0) - Distributed data processing
- **pandas** (>=2.0.0) - Data manipulation
- **matplotlib** (>=3.7.0) - Plotting and visualization
- **requests** (>=2.28.0) - HTTP requests for API calls
- **seaborn** (>=0.12.0) - Statistical data visualization

### Step 5: Verify Installation

Test that all packages are installed correctly:

```bash
python -c "import pyspark, pandas, matplotlib, requests, seaborn; print('✅ All packages installed successfully!')"
```

---

## ⚙️ Configuration

### Step 1: Set Up Your TMDb API Key

The project uses the TMDb API to fetch movie data. You need to configure your API key.

1. **Get an API Key:**
   - Visit https://www.themoviedb.org/settings/api
   - Create a free account if needed
   - Request an API key (choose "Developer" option)
   - Copy your API key

2. **Add API Key to Configuration:**

   Open `utils/config.py` and find the line with:
   ```python
   API_KEY = 'your_api_key_here'
   ```
   
   Replace it with your actual API key:
   ```python
   API_KEY = 'your_actual_api_key_33c3009d4610d7c8a3484f3bc865055b'
   ```

3. **Alternatively, Set as Environment Variable:**

   ```bash
   # On Windows (PowerShell):
   $env:TMDB_API_KEY='your_api_key'
   
   # On Windows (Command Prompt):
   set TMDB_API_KEY=your_api_key
   
   # On macOS/Linux:
   export TMDB_API_KEY=your_api_key
   ```

### Step 2: Verify Output Directory

The pipeline creates an `output/` directory for results. This should be automatically created, but you can verify:

```bash
# Check if output directory exists
dir output

# Or create it manually if needed:
mkdir output
```

---

## 🚀 Running the Pipeline

You have two options to run the pipeline:

### Option 1: Run from Command Line (Recommended for Production)

```bash
python main.py
```

This will execute the complete pipeline from start to finish:
- Extract movie data from TMDb API
- Clean and transform the data
- Analyze and generate KPIs
- Create visualizations
- Save all results to `output/` directory

### Option 2: Run the Jupyter Notebook (Interactive, Better for Exploration)

```bash
jupyter notebook tmdb_pipeline.ipynb
```

Or, if Jupyter is not installed:

```bash
pip install jupyter
jupyter notebook tmdb_pipeline.ipynb
```

Then, in the Jupyter interface:
1. Click **Kernel** → **Restart & Run All** to execute all cells
2. Or press **Ctrl+Shift+Enter** to run all cells

---

## 📁 Project Structure

```
TMBD with PySpark/
├── main.py                          # Main orchestrator script
├── tmdb_pipeline.ipynb             # Interactive Jupyter notebook
├── requirements.txt                # Python dependencies
├── SETUP_AND_RUN_GUIDE.md         # This file
├── task.md                         # Project requirements
├── read.md                         # Project guidelines
├── src/                            # Source code modules
│   ├── __init__.py
│   ├── extract.py                  # API data extraction
│   ├── transform.py                # Data cleaning & transformation
│   ├── analyze.py                  # KPI analysis & rankings
│   └── visualize.py                # Chart generation & visualization
├── utils/                          # Utility modules
│   ├── __init__.py
│   ├── config.py                   # Configuration & API key
│   └── logger.py                   # Logging setup
└── output/                         # Generated results (created after running)
    ├── analysis_results.json       # KPI analysis results
    ├── movies_data.csv             # Cleaned dataset
    └── visualizations/             # Generated charts
        ├── revenue_vs_budget.png
        ├── roi_by_genre.png
        ├── popularity_vs_rating.png
        ├── yearly_boxoffice.png
        └── franchise_comparison.png
```

---

## 🔍 Understanding the Pipeline

### Pipeline Architecture

The pipeline follows an **ETL (Extract, Transform, Load) pattern**:

```
┌─────────────────────────────────────────────────────────────┐
│                    TMDB PIPELINE                            │
├─────────────────────────────────────────────────────────────┤
│ 1. EXTRACT          │ Fetch data from TMDb API              │
│ 2. TRANSFORM        │ Clean & prepare data with PySpark     │
│ 3. ANALYZE          │ Generate KPIs & rankings              │
│ 4. VISUALIZE        │ Create charts & insights               │
│ 5. SAVE RESULTS     │ Export CSV, JSON, PNG files            │
└─────────────────────────────────────────────────────────────┘
```

### Step 1: Extract
- **File:** `src/extract.py`
- **What it does:** 
  - Fetches movie data from the TMDb API
  - Implements retry logic (up to 3 attempts)
  - Applies rate limiting (1-2 seconds between requests)
  - Handles API errors gracefully
- **Output:** List of raw movie dictionaries

### Step 2: Transform
- **File:** `src/transform.py`
- **What it does:**
  - Cleans raw JSON data from the API
  - Extracts and flattens nested columns (genres, companies, countries, languages)
  - Converts data types (budget/revenue to millions USD, dates to datetime)
  - Handles missing values and outliers
  - Filters for "Released" movies only
- **Output:** PySpark DataFrame with clean, structured data

### Step 3: Analyze
- **File:** `src/analyze.py`
- **What it does:**
  - Ranks movies by revenue, budget, profit, ROI, ratings
  - Analyzes franchise vs standalone performance
  - Evaluates director performance
  - Generates KPI summaries
  - Produces advanced search results
- **Output:** Dictionary of analysis results

### Step 4: Visualize
- **File:** `src/visualize.py`
- **What it does:**
  - Creates revenue vs budget scatter plots
  - Generates ROI by genre bar charts
  - Builds popularity vs rating distributions
  - Shows yearly box office trends
  - Compares franchise vs standalone performance
- **Output:** PNG chart files saved to `output/visualizations/`

---

## 🛠️ Troubleshooting

### Issue 1: "No module named 'pyspark'"

**Solution:** Install PySpark again:
```bash
pip install pyspark>=3.4.0
```

### Issue 2: "java.lang.NoClassDefFoundError"

**Solution:** Java is not installed or not in PATH. Install Java:
- Download from https://www.oracle.com/java/technologies/downloads/
- Add Java to your system PATH
- Restart your terminal

### Issue 3: API Key Error / 401 Unauthorized

**Solution:** 
1. Verify your API key in `utils/config.py`
2. Check that the API key is correct (no extra spaces)
3. Ensure your TMDb account has API access enabled
4. Test with a fresh API key if issues persist

### Issue 4: "Connection timeout" when fetching data

**Solution:**
- Check your internet connection
- Try again later (TMDb API might be down)
- Increase timeout in `src/extract.py` if needed

### Issue 5: Out of Memory Error

**Solution:** 
- Reduce Spark memory allocation in `main.py`:
  ```python
  .config("spark.driver.memory", "2g")  # Change from 4g to 2g
  ```
- Close other applications to free up RAM

### Issue 6: Port already in use (Spark driver port)

**Solution:**
```bash
# Stop any existing Spark processes
# Then restart the pipeline
python main.py
```

---

## 📊 Output & Results

After running the pipeline successfully, check the `output/` directory:

### Files Generated

1. **analysis_results.json**
   - All KPI rankings and analysis results
   - Open with any text editor or JSON viewer

2. **movies_data.csv**
   - Cleaned and transformed dataset
   - Can be imported into Excel, Pandas, or other tools

3. **visualizations/** folder
   - `revenue_vs_budget.png` - Scatter plot of budget vs revenue
   - `roi_by_genre.png` - ROI rankings by movie genre
   - `popularity_vs_rating.png` - Distribution of popularity vs ratings
   - `yearly_boxoffice.png` - Trend of box office revenue over years
   - `franchise_comparison.png` - Franchise vs standalone performance

### Checking Results

```bash
# List all output files
dir output /s

# View the CSV data (first 10 rows)
# On Windows PowerShell:
Get-Content output\movies_data.csv -Head 10

# View analysis JSON (formatted):
python -m json.tool output\analysis_results.json
```

---

## 📝 Expected Execution Time

- **Data Extraction:** 2-5 minutes (depends on API speed)
- **Data Transformation:** 10-30 seconds
- **Analysis:** 5-15 seconds
- **Visualization:** 10-20 seconds
- **Total:** 3-6 minutes for complete pipeline

---

## 🎯 Next Steps

After running the pipeline:

1. **Explore the Results**
   - Open `output/movies_data.csv` in Excel or Pandas
   - View PNG charts in any image viewer
   - Review `output/analysis_results.json` for detailed KPIs

2. **Customize the Analysis**
   - Edit `src/analyze.py` to add custom metrics
   - Modify `src/visualize.py` to create different charts
   - Update `utils/config.py` to change movie IDs or parameters

3. **Extend the Pipeline**
   - Add more data sources
   - Implement machine learning models
   - Export results to a database

---

## 📚 Additional Resources

- **PySpark Documentation:** https://spark.apache.org/docs/latest/api/python/
- **TMDb API Docs:** https://developers.themoviedb.org/3
- **Pandas Documentation:** https://pandas.pydata.org/docs/
- **Matplotlib Tutorial:** https://matplotlib.org/stable/users/index.html

---

## ✅ Quick Reference Checklist

Before running the pipeline, verify:

- [ ] Python 3.8+ installed
- [ ] Java 11+ installed
- [ ] Virtual environment activated (optional but recommended)
- [ ] All dependencies installed (`pip install -r requirements.txt`)
- [ ] TMDb API key added to `utils/config.py`
- [ ] At least 4GB free RAM
- [ ] At least 2GB free disk space
- [ ] Internet connection available

---

## 🤝 Support

If you encounter issues:

1. **Check the logs:** Errors are logged to `logs/` directory (if enabled)
2. **Review the code:** Check relevant files in `src/` directory
3. **Verify configuration:** Ensure `utils/config.py` is correct
4. **Run individually:** Test each step separately if the full pipeline fails

---

**Happy analyzing! 🎬📊**
