# SSIS to PySpark Converter - Complete Project Guide

**Last Updated:** October 20, 2025  
**Version:** 2.0 - Production Ready

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Quick Start](#quick-start)
3. [Architecture](#architecture)
4. [Building Executables](#building-executables)
5. [Usage Guide](#usage-guide)
6. [Parser Capabilities](#parser-capabilities)
7. [Mapping System](#mapping-system)
8. [Recent Fixes & Improvements](#recent-fixes--improvements)
9. [Testing & Deployment](#testing--deployment)
10. [Troubleshooting](#troubleshooting)

---

## 1. Project Overview

### Purpose
Converts Microsoft SSIS (SQL Server Integration Services) packages to PySpark code for Databricks.

### Key Features
- ✅ Parses `.dtsx` files (SSIS XML format)
- ✅ Maps SSIS components to PySpark equivalents
- ✅ Generates production-ready PySpark code
- ✅ Handles SQL tasks, data flows, transformations
- ✅ Preserves execution order and dependencies
- ✅ Standalone executable (no Python required for deployment)

### Technology Stack
- **Parser:** Enhanced XML processor for SSIS packages
- **Mapper:** Rule-based + LLM fallback for complex transformations
- **Output:** Clean, executable PySpark code
- **Deployment:** PyInstaller for standalone `.exe` files

---

## 2. Quick Start

### Installation (Development)
```bash
# Clone repository
git clone <repository-url>
cd ssis-to-pyspark-agent

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage
```bash
# Convert single package
python ssis_to_pyspark_app.py input/package.dtsx

# Convert entire folder
python ssis_to_pyspark_app.py input/

# Build executable
build_exe.bat  # Select option 2 for CLI
```

### Output Structure
```
output/
├── parsed_json/         # Intermediate JSON from parser
├── pyspark_code/        # Generated PySpark scripts
├── mapping_details/     # Mapping statistics and details
└── analysis/            # Conversion analysis reports
```

---

## 3. Architecture

### Component Overview

```
SSIS Package (.dtsx)
    ↓
Data Engineering Parser
    ↓
Structured JSON
    ↓
Enhanced JSON Mapper
    ↓
PySpark Code (.py)
```

### Key Components

#### 3.1 Parser (`parsing/data_engineering_parser.py`)
- Extracts SSIS package structure from XML
- Identifies SQL tasks, data flows, connections
- Captures execution order via precedence constraints
- Handles disabled tasks, containers, event handlers
- Supports AccessMode detection (SQL query vs table)

#### 3.2 Mapper (`mapping/enhanced_json_mapper.py`)
- Maps SSIS components to PySpark equivalents
- Generates connection configurations
- Creates ordered execution code
- Handles DataFrame flow tracking
- Implements topological sort for correct task ordering

#### 3.3 Expression Translator (`mapping/expression_translator.py`)
- Converts SSIS expressions to PySpark
- Handles date functions, string operations, conditionals
- LLM fallback for complex expressions

#### 3.4 Code Generator (`code_generation/llm_code_generator.py`)
- LLM-based fallback for unmapped components
- Uses Google Gemini API
- Generates context-aware PySpark code

---

## 4. Building Executables

### Build Process
```bash
# Run build script
build_exe.bat

# Options:
# 1. GUI Application (graphical interface)
# 2. CLI Application (command-line) - RECOMMENDED
# 3. Both
```

### Build Requirements
- Python 3.7+
- PyInstaller (auto-installed)
- ~100 MB disk space

### Output
- `dist/ssis_to_pyspark_converter.exe` (~25-30 MB)
- Completely standalone
- No Python installation required on target

### What's Included in EXE
- All Python code (parsing, mapping, generation)
- All dependencies (pydantic, typing-extensions, etc.)
- Configuration files
- Models and schemas
- All recent fixes and improvements

---

## 5. Usage Guide

### CLI Application

#### Single File Conversion
```bash
ssis_to_pyspark_converter.exe input/package.dtsx
```

#### Batch Conversion
```bash
ssis_to_pyspark_converter.exe input/folder/
```

#### Custom Output Directory
```bash
ssis_to_pyspark_converter.exe input/package.dtsx -o custom_output/
```

### GUI Application
- Double-click `ssis_to_pyspark_converter.exe`
- Select input file/folder via file browser
- Click "Convert"
- View results in output panel

### Generated Code Structure

```python
# Header and imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Spark initialization
spark = SparkSession.builder...

# Connection configurations
dwcommon_url = "jdbc:sqlserver://..."
dwcommon_user = "username"
# ... more connections

# Control Flow Execution (Correct Order)
# Step 1: SQL Task
spark.sql("""TRUNCATE TABLE...""")

# Step 2: Data Flow
source_df = spark.read.format("jdbc")...
transformed_df = source_df.withColumn(...)
transformed_df.write.format("jdbc")...

# Step 3: More SQL Tasks
# ...
```

---

## 6. Parser Capabilities

### Supported SSIS Components

#### SQL Tasks
- Execute SQL Task (DDL, DML, stored procedures)
- INSERT, UPDATE, DELETE, TRUNCATE, MERGE
- Dynamic SQL with variables
- Connection management

#### Data Flow Components

**Sources:**
- OLE DB Source (table and SQL query modes)
- Flat File Source
- Excel Source
- REST API / JSON Source
- Azure Blob Source

**Transformations:**
- Derived Column
- Lookup
- Conditional Split
- Aggregate
- Sort
- Merge Join
- Union All
- Row Count (RC_INSERT)
- Data Conversion
- Multicast

**Destinations:**
- OLE DB Destination
- Flat File Destination
- Azure Blob Destination

#### Control Flow
- Precedence Constraints (task dependencies)
- For Loop Container
- Foreach Loop Container
- Sequence Container
- Execute Package Task
- Script Task (basic support)

#### Advanced Features
- **AccessMode Detection:** Differentiates between table access (mode 1) and SQL query (mode 2)
- **Disabled Task Filtering:** Skips tasks with `DTS:Disabled="True"`
- **Execution Order:** Uses topological sort (Kahn's algorithm) for correct task ordering
- **Connection Linking:** Maps SSIS connection managers to PySpark JDBC connections
- **DataFrame Flow Tracking:** Maintains DataFrame variable names through transformations

---

## 7. Mapping System

### Mapping Categories

#### 7.1 SQL Tasks
- **Mapped:** TRUNCATE, INSERT, UPDATE, DELETE, EXECUTE (stored procs)
- **Output:** `spark.sql("""...""")` statements
- **Sanitization:** Converts SQL Server syntax to Spark SQL

#### 7.2 Data Sources
- **OLE DB:** Mapped to `spark.read.format("jdbc")`
- **File Sources:** Mapped to `spark.read.csv()`, `spark.read.excel()`
- **REST API:** Mapped to `requests` + `spark.createDataFrame()`

#### 7.3 Transformations
- **Rule-Based:** Predefined mappings for common transformations
- **LLM Fallback:** Uses Gemini for complex/unmapped transformations
- **DataFrame Tracking:** Maintains variable names through pipeline

#### 7.4 Destinations
- **OLE DB:** Mapped to `df.write.format("jdbc")`
- **File Destinations:** Mapped to `df.write.csv()`, etc.
- **Modes:** Supports append, overwrite, error

### Connection Management

SSIS connections are converted to PySpark JDBC configurations:

```python
# SSIS: Connection Manager "dwCOMMON"
# PySpark:
dwcommon_url = "jdbc:sqlserver://server:port;databaseName=db"
dwcommon_user = "username"
dwcommon_password = "password"
dwcommon_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
```

---

## 8. Recent Fixes & Improvements

### October 20, 2025 Updates

#### 8.1 SQL Query Syntax Fix
- **Issue:** SQL queries with newlines caused syntax errors
- **Fix:** Display only first line in comments, properly wrap in `.option("query", """...""")`
- **Impact:** Syntax validation now passes

#### 8.2 Connection Variables
- **Issue:** Hardcoded connection details
- **Fix:** 
  - Parser extracts connection info from component XML
  - Mapper preserves connection through `_map_sources()` and `_map_destinations()`
  - Code generation uses actual connection variables
- **Impact:** Both source and destination now use proper connection variables

#### 8.3 DataFrame Flow Tracking
- **Issue:** Destination using generic `df` variable instead of actual DataFrame
- **Fix:** Added `_get_last_df_in_flow()` method to track DataFrame variables
- **Impact:** Correct DataFrame variable used in destination writes

#### 8.4 Import Cleanup
- **Issue:** Circular import with `from datetime import datetime`
- **Fix:** Removed datetime import, use local import in header generation
- **Impact:** No more import conflicts

#### 8.5 Indentation Fix
- **Issue:** RC_INSERT transformation had excessive indentation (34 spaces)
- **Fix:** Updated to proper 4-space indentation
- **Impact:** Clean, readable code

#### 8.6 Disabled Task Filtering
- **Issue:** Disabled SSIS tasks included in execution
- **Fix:** Parser checks `DTS:Disabled` attribute and skips disabled tasks
- **Impact:** Only active tasks are converted

#### 8.7 Execution Order
- **Issue:** Tasks not executing in correct dependency order
- **Fix:** Implemented topological sort (Kahn's algorithm) using task IDs
- **Impact:** Tasks execute in correct order respecting dependencies

#### 8.8 AccessMode Support
- **Issue:** SQL queries treated as table names
- **Fix:** Parser extracts AccessMode property, distinguishes query mode (2) from table mode (1)
- **Impact:** SQL queries use `.option("query", ...)`, tables use `.option("dbtable", ...)`

---

## 9. Testing & Deployment

### Testing in Databricks

#### Prerequisites
- Databricks workspace with cluster
- JDBC drivers for SQL Server
- Source/destination databases accessible

#### Steps
1. Upload generated PySpark script to Databricks
2. Create notebook or job
3. Configure connection credentials (replace username/password)
4. Run and verify output

#### Validation Checklist
- [ ] Syntax validation passes
- [ ] Connections established successfully
- [ ] SQL tasks execute without errors
- [ ] Data flows complete (source → transform → destination)
- [ ] Row counts match SSIS execution
- [ ] Data quality matches SSIS output

### Deployment Options

#### Option 1: Direct Python Execution
```bash
python ssis_to_pyspark_app.py input/package.dtsx
```
- Requires Python environment
- Good for development/testing

#### Option 2: Standalone Executable
```bash
ssis_to_pyspark_converter.exe input/package.dtsx
```
- No Python required
- Perfect for client deployments
- Can be integrated into pipelines

#### Option 3: Azure Data Factory Integration
- Call executable via ADF pipeline
- Schedule conversions
- Trigger Databricks jobs with generated code

---

## 10. Troubleshooting

### Common Issues

#### Issue: "Import pyspark.sql could not be resolved"
- **Cause:** PySpark not installed in local IDE environment
- **Solution:** Ignore IDE warning (code is valid) OR `pip install pyspark`

#### Issue: "df is not defined"
- **Cause:** DataFrame flow tracking issue (fixed in latest version)
- **Solution:** Rebuild executable with latest code

#### Issue: Syntax validation fails
- **Cause:** SQL query formatting issue (fixed in latest version)
- **Solution:** Update to latest code and rebuild

#### Issue: Wrong execution order
- **Cause:** Task dependencies not properly resolved (fixed)
- **Solution:** Ensure using topological sort implementation

#### Issue: Connection variables not used
- **Cause:** Connection info not preserved in mapper (fixed)
- **Solution:** Update `_map_sources()` and `_map_destinations()` methods

### Debug Mode

Enable detailed logging:
```python
# In ssis_to_pyspark_app.py
logging.basicConfig(level=logging.DEBUG)
```

### Getting Help

1. Check `output/analysis/conversion_summary.json` for statistics
2. Review `output/mapping_details/*.json` for mapping details
3. Check generated code for TODOs and warnings
4. Enable DEBUG logging for detailed trace

---

## Appendix

### Project Structure
```
ssis-to-pyspark-agent/
├── parsing/                    # XML parsing
├── mapping/                    # Component mapping
├── code_generation/            # LLM fallback
├── input/                      # Input .dtsx files
├── output/                     # Generated outputs
├── docs/                       # Documentation
├── scripts/                    # Utility scripts
├── build_exe.bat              # Build script
├── ssis_to_pyspark_app.py     # CLI entry point
├── ssis_to_pyspark_gui.py     # GUI entry point
└── requirements.txt           # Python dependencies
```

### File Naming Convention
- Input: `<package_name>.dtsx`
- Parsed JSON: `<package_name>_data_engineering.json`
- PySpark code: `<package_name>_pyspark.py`
- Mapping details: `<package_name>_mapping.json`

### Version History
- **v2.0 (Oct 2025):** Production-ready with all major fixes
- **v1.5 (Oct 2025):** Enhanced parser and mapper
- **v1.0 (Earlier):** Initial release

---

**For more specific topics, see:**
- `BUILD_INSTRUCTIONS.md` - Detailed build guide
- `README_CLIENT.md` - Client-facing usage guide
- `README.md` - Technical documentation

**Project Status:** ✅ Production Ready


