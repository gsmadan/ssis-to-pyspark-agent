# Databricks notebook source
# MAGIC %md
# MAGIC # SSIS to PySpark Converter - Databricks Execution
# MAGIC 
# MAGIC This notebook runs the SSIS to PySpark converter directly within Databricks, utilizing Databricks Model Serving for LLM-powered code refinement.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Installation

# COMMAND ----------

# Install dependencies
%pip install -r requirements.txt

# COMMAND ----------

# Restart Python kernel to load installed packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration
# MAGIC 
# MAGIC Set your API keys and configuration here.

# COMMAND ----------

import os

# Databricks Configuration
# Automatically detected in Databricks environment, but can be overridden
# os.environ["DATABRICKS_HOST"] = "https://your-workspace.cloud.databricks.com"
# os.environ["DATABRICKS_TOKEN"] = "dapi..."

# Model Serving Endpoint
# Replace with your actual endpoint name (e.g., databricks-dbrx-instruct, databricks-llama-2-70b-chat)
os.environ["DATABRICKS_ENDPOINT"] = "databricks-dbrx-instruct"

# Set default provider to Databricks
os.environ["DEFAULT_LLM_PROVIDER"] = "databricks"

# Output directory
# Using Unity Catalog Volumes (Recommended)
# Format: /Volumes/<catalog>/<schema>/<volume>/<path>
os.environ["OUTPUT_DIRECTORY"] = "/Volumes/main/default/ssis_migration/output"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Converter
# MAGIC 
# MAGIC Run the converter on your SSIS packages.

# COMMAND ----------

import sys
import logging
from ssis_to_pyspark_app import SSISToPySparkApp

# Configure logging to show in cell output
logging.basicConfig(level=logging.INFO, force=True)

# Initialize App
app = SSISToPySparkApp(
    databricks_mode=True,
    skip_validation=False  # Enable LLM validation
)

# Define input path (upload your .dtsx files to your Volume first)
# Example: /Volumes/main/default/ssis_migration/input/
input_path = "/Volumes/main/default/ssis_migration/input/"

# Run Conversion
if os.path.isdir(input_path):
    print(f"Converting all packages in: {input_path}")
    results = app.convert_folder(input_path)
    app.print_summary(results)
elif os.path.isfile(input_path):
    print(f"Converting single package: {input_path}")
    result = app.convert_single_package(input_path)
    app.print_summary([result])
else:
    print(f"Input path not found: {input_path}")
    print("Please ensure you have created the Volume and uploaded your files.")
    print("Expected path example: /Volumes/<catalog>/<schema>/<volume>/input/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. View Results
# MAGIC 
# MAGIC The generated code is saved to the output directory.

# COMMAND ----------

output_dir = os.environ["OUTPUT_DIRECTORY"]
# For Volumes, we can list directly with os or dbutils using the path
display(dbutils.fs.ls(output_dir))
