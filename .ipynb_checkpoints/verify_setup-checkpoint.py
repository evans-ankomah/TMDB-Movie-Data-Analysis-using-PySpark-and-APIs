#!/usr/bin/env python
"""
Setup script to verify and configure PySpark environment for Windows.
This script checks all requirements and provides detailed diagnostics.
"""

import os
import sys
import subprocess
from pathlib import Path


def check_java():
    """Check if Java is properly installed and configured."""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        print("✓ Java is installed")
        return True
    except FileNotFoundError:
        print("✗ Java not found in PATH")
        return False


def check_python():
    """Check Python version."""
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"✓ Python version: {version}")
    return True


def check_required_packages():
    """Check if all required packages are installed."""
    required = ['pyspark', 'pandas', 'requests', 'matplotlib', 'seaborn']
    missing = []
    
    for package in required:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is NOT installed")
            missing.append(package)
    
    return len(missing) == 0


def check_environment_variables():
    """Check if required environment variables are set."""
    vars_to_check = {
        'JAVA_HOME': 'Java installation directory',
        'PYSPARK_PYTHON': 'Python executable for PySpark workers',
        'PYSPARK_DRIVER_PYTHON': 'Python executable for PySpark driver'
    }
    
    all_set = True
    for var, description in vars_to_check.items():
        value = os.environ.get(var)
        if value:
            print(f"✓ {var} = {value}")
        else:
            print(f"✗ {var} is not set ({description})")
            all_set = False
    
    return all_set


def main():
    """Run all checks."""
    print("\n" + "="*60)
    print("PYSPARK ENVIRONMENT VERIFICATION")
    print("="*60 + "\n")
    
    checks = [
        ("Java Installation", check_java),
        ("Python Version", check_python),
        ("Required Packages", check_required_packages),
        ("Environment Variables", check_environment_variables),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\nChecking {name}...")
        try:
            results.append((name, check_func()))
        except Exception as e:
            print(f"✗ Error during {name} check: {e}")
            results.append((name, False))
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"{name}: {status}")
    
    all_passed = all(result for _, result in results)
    
    if all_passed:
        print("\n✓ All checks passed! You can run: python main.py")
    else:
        print("\n✗ Some checks failed. Please install missing dependencies:")
        print("  pip install pyspark pandas requests matplotlib seaborn")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
