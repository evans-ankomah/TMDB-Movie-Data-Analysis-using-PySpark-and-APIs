# Setup PySpark for Windows
# This script configures all necessary environment variables for PySpark

# Get Python path
$python_path = (Get-Command python).Source
Write-Host "Python location: $python_path"

# Set Java Home
$java_home = 'C:\Program Files\Java\jdk-21'
if (Test-Path $java_home) {
    Write-Host "Java found at: $java_home"
} else {
    Write-Host "ERROR: Java not found at $java_home"
    exit 1
}

# Create Hadoop bin directory if it doesn't exist
$hadoop_path = "C:\hadoop\bin"
if (-not (Test-Path $hadoop_path)) {
    New-Item -ItemType Directory -Path $hadoop_path -Force | Out-Null
    Write-Host "Created Hadoop directory: $hadoop_path"
    
    # Download winutils.exe for Hadoop 3.2.1
    $url = "https://github.com/steveloughran/winutils/raw/master/hadoop-3.2.1/bin/winutils.exe"
    $output = "$hadoop_path\winutils.exe"
    
    Write-Host "Downloading winutils.exe from GitHub..."
    try {
        Invoke-WebRequest -Uri $url -OutFile $output -ErrorAction Stop
        Write-Host "Downloaded winutils.exe successfully"
    } catch {
        Write-Host "WARNING: Could not download winutils.exe automatically"
        Write-Host "You can manually download from: $url"
        Write-Host "And place it in: $hadoop_path"
    }
}

# Set environment variables (User level - persistent)
Write-Host "`nSetting environment variables..."
[Environment]::SetEnvironmentVariable('JAVA_HOME', $java_home, 'User')
[Environment]::SetEnvironmentVariable('PYSPARK_PYTHON', $python_path, 'User')
[Environment]::SetEnvironmentVariable('PYSPARK_DRIVER_PYTHON', $python_path, 'User')
[Environment]::SetEnvironmentVariable('HADOOP_HOME', 'C:\hadoop', 'User')

# Set for current session
$env:JAVA_HOME = $java_home
$env:PYSPARK_PYTHON = $python_path
$env:PYSPARK_DRIVER_PYTHON = $python_path
$env:HADOOP_HOME = 'C:\hadoop'

Write-Host "Environment variables set successfully!"
Write-Host "`nEnvironment Configuration:"
Write-Host "  JAVA_HOME: $env:JAVA_HOME"
Write-Host "  PYSPARK_PYTHON: $env:PYSPARK_PYTHON"
Write-Host "  PYSPARK_DRIVER_PYTHON: $env:PYSPARK_DRIVER_PYTHON"
Write-Host "  HADOOP_HOME: $env:HADOOP_HOME"
