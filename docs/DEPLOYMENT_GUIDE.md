# SSIS to PySpark Converter - Deployment Guide

## Overview

This guide explains how to build, deploy, and use the SSIS to PySpark Converter as a standalone .exe application in client environments.

---

## 📋 Table of Contents

1. [Building the .exe](#building-the-exe)
2. [Deploying to Client Environment](#deploying-to-client-environment)
3. [Using the Application](#using-the-application)
4. [Integration with ADF and Databricks](#integration-with-adf-and-databricks)
5. [Testing and Validation](#testing-and-validation)
6. [Troubleshooting](#troubleshooting)

---

## 🔨 Building the .exe

### Prerequisites

**Development Environment:**
- Python 3.8+ installed
- All dependencies installed: `pip install -r requirements.txt`
- PyInstaller 5.0+: `pip install pyinstaller`

### Build Steps

#### Option 1: Using the Build Script (Recommended)
```bash
# Run the automated build script
python build_exe.py
```

This will:
1. Clean previous build artifacts
2. Run PyInstaller with the spec file
3. Test the built .exe
4. Show exe location and size

#### Option 2: Manual PyInstaller Command
```bash
# Clean previous builds
rmdir /s /q build dist

# Build the exe
pyinstaller ssis_to_pyspark.spec --clean --noconfirm
```

### Build Output

**Location**: `dist/ssis_to_pyspark_converter.exe`

**Expected Size**: ~30-50 MB (includes all dependencies)

**What's Included**:
- All Python parsing and mapping code
- All required dependencies (lxml, pydantic, etc.)
- No external Python installation needed

---

## 🚀 Deploying to Client Environment

### Deployment Package

Create a deployment package with:
```
deployment_package/
├── ssis_to_pyspark_converter.exe    # The application
├── README_CLIENT.txt                 # Usage instructions
└── examples/                         # (Optional) Example input files
```

### Client Environment Requirements

**Minimal Requirements**:
- Windows 10/11 or Windows Server 2016+
- No Python installation needed
- No additional dependencies needed
- ~100 MB free disk space

**Recommended**:
- 4 GB RAM
- Modern CPU (for faster processing)
- Network access (if using LLM fallback - optional)

### Deployment Steps

1. **Copy the exe** to client machine
2. **Create output folder** (or let exe create it automatically)
3. **Place input .dtsx files** in an input folder
4. **Run the converter** (see usage below)

---

## 📖 Using the Application

### Basic Usage

#### Convert a Single Package
```cmd
ssis_to_pyspark_converter.exe input\package.dtsx
```

#### Convert All Packages in a Folder
```cmd
ssis_to_pyspark_converter.exe input\
```

#### Specify Custom Output Folder
```cmd
ssis_to_pyspark_converter.exe input\package.dtsx --output my_output\
```

#### Verbose Mode (Detailed Logging)
```cmd
ssis_to_pyspark_converter.exe input\package.dtsx --verbose
```

### Command Line Options

```
usage: ssis_to_pyspark_converter.exe [-h] [--output OUTPUT] [--verbose] 
                                     [--no-validation] input

positional arguments:
  input              Path to .dtsx file or folder containing .dtsx files

options:
  -h, --help         show this help message and exit
  --output OUTPUT    Output directory for generated files (default: output/)
  --verbose          Enable verbose logging
  --no-validation    Skip syntax validation
```

### Output Structure

After running, you'll find:
```
output/
├── parsed_json/              # Parsed SSIS package data (JSON)
├── pyspark_code/             # Generated PySpark code (.py files)
├── mapping_details/          # Mapping metadata (JSON)
└── analysis/                 # Conversion summary and reports
```

### Log Files

The application creates a log file: `ssis_converter.log`
- Contains detailed conversion process logs
- Useful for troubleshooting
- Includes timestamps and error messages

---

## 🔗 Integration with ADF and Databricks

### Typical Workflow

```
1. Client Environment (On-Premises)
   ├── SSIS Packages (.dtsx files)
   ├── Run: ssis_to_pyspark_converter.exe
   └── Output: PySpark code (.py files)
         ↓
2. Upload to Azure Data Factory
   ├── Store PySpark scripts in ADF repository
   └── Create ADF pipeline
         ↓
3. Execute in Databricks
   ├── ADF triggers Databricks notebook/job
   ├── Runs generated PySpark code
   └── Produces same results as SSIS
```

### ADF Pipeline Setup

**Step 1: Store PySpark Code**
- Upload generated `.py` files to Azure Blob Storage or ADF Git repo
- Organize by package name

**Step 2: Create ADF Pipeline**
```json
{
  "name": "Run_Converted_PySpark",
  "type": "DatabricksNotebook",
  "linkedServiceName": {
    "referenceName": "DatabricksLinkedService",
    "type": "LinkedServiceReference"
  },
  "typeProperties": {
    "notebookPath": "/path/to/generated_pyspark_code"
  }
}
```

**Step 3: Configure Databricks**
- Create Databricks cluster
- Install required libraries (if any)
- Upload generated PySpark notebooks
- Configure connection credentials

### Databricks Execution

**Option 1: As Notebook**
```python
# Upload generated .py file as Databricks notebook
# Update connection credentials
# Run the notebook
```

**Option 2: As Job**
```python
# Create Databricks job
# Point to generated .py file
# Schedule or trigger from ADF
```

---

## ✅ Testing and Validation

### Before Deployment Testing

#### 1. Test .exe Locally
```cmd
# Test single file
ssis_to_pyspark_converter.exe input\test_package.dtsx

# Check outputs
dir output\pyspark_code\
```

#### 2. Validate Generated PySpark Code
```python
# The exe automatically validates syntax
# Check the log for validation results
# Look for "[OK] Syntax validation: PASSED"
```

#### 3. Test in Local Spark Environment (If Available)
```bash
# Install PySpark locally
pip install pyspark

# Run generated code
spark-submit output/pyspark_code/test_package_pyspark.py
```

### SSIS vs Databricks Comparison Testing

#### Step 1: Capture SSIS Results
```sql
-- Run SSIS package
-- Export results to CSV/table
-- Document row counts, data samples
```

#### Step 2: Run Generated PySpark in Databricks
```python
# Run converted PySpark code
# Export results using same format
# Document row counts, data samples
```

#### Step 3: Compare Results
```python
# Compare:
# - Row counts
# - Column names and types
# - Data samples
# - Aggregation results
# - Transformation logic
```

**Expected**: Results should match (within acceptable tolerance for date/time functions)

---

## 🔧 Troubleshooting

### Common Issues

#### Issue 1: Exe Won't Run
**Symptoms**: Double-click does nothing or error message
**Solutions**:
- Run from command prompt to see error messages
- Check Windows antivirus (may block unsigned exe)
- Verify exe was built completely (check file size)

#### Issue 2: No Output Generated
**Symptoms**: Exe runs but no output files created
**Solutions**:
- Check `ssis_converter.log` for errors
- Verify input file is valid .dtsx
- Check disk space
- Verify write permissions on output folder

#### Issue 3: Syntax Validation Fails
**Symptoms**: Log shows syntax errors in generated code
**Solutions**:
- Review the generated PySpark file manually
- Check for complex expressions that need manual translation
- Look for TODOs in generated code
- Most common: Join conditions need to be specified

#### Issue 4: Connection Errors in Databricks
**Symptoms**: PySpark code runs but can't connect to data sources
**Solutions**:
- Update connection credentials in generated code
- Update JDBC URLs with correct servers
- Install required JDBC drivers in Databricks
- Configure Databricks secrets for credentials

---

## 📊 Performance Expectations

### Conversion Speed
| Package Size | Conversion Time |
|--------------|-----------------|
| Small (1-5 components) | < 1 second |
| Medium (5-20 components) | 1-3 seconds |
| Large (20+ components) | 3-10 seconds |

### Output Size
| Input | Output PySpark Code |
|-------|---------------------|
| 1 MB .dtsx | ~5-20 KB .py file |
| Typical | 50-200 lines of Python |

---

## 🎯 Client Deployment Checklist

### Pre-Deployment
- [ ] Build .exe using `build_exe.py`
- [ ] Test exe locally with sample packages
- [ ] Verify all test packages convert successfully
- [ ] Document any manual steps needed

### Deployment
- [ ] Copy .exe to client environment
- [ ] Create input and output folders
- [ ] Provide usage documentation
- [ ] Test with one sample package

### Post-Deployment
- [ ] Review generated PySpark code
- [ ] Update connection credentials
- [ ] Test in Databricks environment
- [ ] Compare results with SSIS
- [ ] Get client approval

### Production
- [ ] Integrate with ADF pipelines
- [ ] Schedule regular conversions (if needed)
- [ ] Monitor for errors
- [ ] Maintain conversion documentation

---

## 🔐 Security Considerations

### In Client Environment
- Exe runs locally (no network calls by default)
- No credentials stored in exe
- Generated code contains placeholder credentials
- Client must provide actual credentials

### For Databricks
- Use Databricks secrets for credentials
- Don't hardcode passwords in PySpark code
- Use managed identities when possible
- Encrypt connection strings

### Recommendations
```python
# Instead of hardcoding:
password = "actual_password"  # DON'T DO THIS

# Use Databricks secrets:
password = dbutils.secrets.get(scope="my_scope", key="db_password")
```

---

## 📚 Additional Resources

### Documentation
- Main README: `../README.md`
- Mapping Documentation: `docs/MAPPING_PROGRESS_SUMMARY.md`
- Parser Documentation: `docs/parsing/`
- Architecture: `docs/architecture/`

### Support
- Check log files: `ssis_converter.log`
- Review generated code comments (TODOs)
- Consult mapping details JSON for statistics

---

## 🎯 Success Criteria

### Conversion Success
- [x] 100% mapping rate
- [x] Valid Python syntax
- [x] All components mapped
- [x] Complete metadata

### Deployment Success
- [ ] Exe runs on client machine
- [ ] Converts all client SSIS packages
- [ ] Generated code runs in Databricks
- [ ] Results match SSIS output

### Production Success
- [ ] ADF pipeline integration complete
- [ ] Scheduled execution working
- [ ] Monitoring in place
- [ ] Client satisfied with results

---

## 💡 Best Practices

### For Conversion
1. Start with simple packages to validate
2. Review generated code before deploying
3. Test with sample data first
4. Document any manual changes needed

### For Databricks
1. Use appropriate cluster size
2. Install required JDBC drivers
3. Configure connection pooling
4. Implement error handling
5. Add logging and monitoring

### For Maintenance
1. Keep conversion log files
2. Document any issues found
3. Track SSIS vs Databricks differences
4. Update mappings as needed

---

## 📞 Quick Reference

### Build Command
```bash
python build_exe.py
```

### Run Converter
```cmd
ssis_to_pyspark_converter.exe input\package.dtsx
```

### Check Results
```cmd
dir output\pyspark_code\
type output\pyspark_code\package_pyspark.py
```

### Deploy to Databricks
1. Review generated code
2. Update credentials
3. Upload to Databricks
4. Test execution
5. Integrate with ADF

---

**Ready for production deployment!** 🚀

For questions or issues, consult the log file or main documentation.

