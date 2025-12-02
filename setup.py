"""
Setup script for SSIS to PySpark Converter.
"""
from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "SSIS to PySpark Converter - Convert SSIS packages to PySpark code for Databricks"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="ssis-to-pyspark-converter",
    version="1.0.0",
    author="Gurdain Singh Madan",
    author_email="support@example.com",
    description="Convert SSIS packages (.dtsx) to PySpark code (.py) for Databricks deployment",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/gsmadan/ssis-to-pyspark-converter",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ssis-to-pyspark=cli:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.txt", "*.md", "*.yml", "*.yaml"],
    },
    keywords="ssis, pyspark, databricks, etl, data-engineering, conversion, migration",
    project_urls={
        "Bug Reports": "https://github.com/your-org/ssis-to-pyspark-converter/issues",
        "Source": "https://github.com/your-org/ssis-to-pyspark-converter",
        "Documentation": "https://ssis-to-pyspark-converter.readthedocs.io/",
    },
)
