# Scripts Directory

This directory contains utility scripts for the SSIS to PySpark converter.

## Main Application

**For conversion, use the main application:**
```bash
python ssis_to_pyspark_app.py input/package.dtsx --output output/
```

## Utility Scripts

### `parse_all_packages.py`
Batch parse all SSIS packages in the input directory.

**Usage:**
```bash
python scripts/parse_all_packages.py
```

**Purpose:**
- Parses all `.dtsx` files in the `input/` directory
- Saves parsed JSON files to `output/parsed_json/`
- Useful for batch processing multiple packages

---

### `validate_pyspark_syntax.py`
Validate generated PySpark code for syntax correctness.

**Usage:**
```bash
# Validate all PySpark files in output directory
python scripts/validate_pyspark_syntax.py

# Validate specific file
python scripts/validate_pyspark_syntax.py output/pyspark_code/my_package.py
```

**Purpose:**
- Checks Python syntax validity of generated PySpark code
- Reports syntax errors and warnings
- Provides validation statistics

---

### `show_execution_flow.py`
Display execution flow for SSIS packages.

**Usage:**
```bash
python scripts/show_execution_flow.py input/package.dtsx
```

**Purpose:**
- Shows the execution order of tasks in SSIS packages
- Displays control flow and precedence constraints
- Useful for debugging and understanding package structure
- Shows both SSIS task IDs and generated PySpark execution order

---

## Notes

- All scripts require the project root to be in the Python path
- Main conversion should use `ssis_to_pyspark_app.py` at the project root
- These utility scripts are for specific tasks like batch processing or validation


