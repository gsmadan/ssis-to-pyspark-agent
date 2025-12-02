# SSIS to PySpark Converter - Deployment Guide

## Building the Executable

### Prerequisites
- Python 3.8 or higher
- Windows OS (for building .exe)
- Internet connection (for downloading dependencies)

### Build Steps

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Build the Executable**
   - **Option 1 - Batch Script (Recommended)**
     ```bash
     build_exe.bat
     ```
     Follow the prompts to select:
     - `1` for GUI Application
     - `2` for CLI Application
     - `3` for Both

   - **Option 2 - Python Script**
     ```bash
     python build_exe.py
     ```

3. **Find Your Executable**
   - Location: `dist/ssis_to_pyspark_converter.exe`
   - The .exe is a standalone file (no Python installation needed on target machine)

---

## Deploying to a Different Environment

### What You Need
1. The `.exe` file: `dist/ssis_to_pyspark_converter.exe`
2. API Key for LLM validation (optional but recommended):
   - **Gemini API Key** (default): Get from [Google AI Studio](https://makersuite.google.com/app/apikey)
   - **OpenAI API Key** (alternative): Get from [OpenAI Platform](https://platform.openai.com/api-keys)

### Deployment Steps

#### Option 1: Quick Deployment (Environment Variable)

1. **Copy the .exe file** to the target Windows machine
2. **Set API Key** (PowerShell):
   ```powershell
   $env:GEMINI_API_KEY="your_api_key_here"
   ```
   Or in Command Prompt:
   ```cmd
   set GEMINI_API_KEY=your_api_key_here
   ```
3. **Run the converter**:
   ```cmd
   ssis_to_pyspark_converter.exe input\package.dtsx --output output\
   ```

#### Option 2: Persistent Configuration (.env file)

1. **Copy the .exe file** to the target machine
2. **Create a `.env` file** in the same folder as the .exe:
   ```
   GEMINI_API_KEY=your_api_key_here
   ```
   Or for OpenAI:
   ```
   OPENAI_API_KEY=your_api_key_here
   ```
3. **Run the converter**:
   ```cmd
   ssis_to_pyspark_converter.exe input\package.dtsx --output output\
   ```

#### Option 3: System-Wide Configuration

**Windows (PowerShell as Administrator):**
```powershell
[System.Environment]::SetEnvironmentVariable("GEMINI_API_KEY", "your_api_key_here", "Machine")
```

**Windows (Command Prompt as Administrator):**
```cmd
setx GEMINI_API_KEY "your_api_key_here" /M
```

---

## Usage Examples

### Single Package Conversion
```cmd
ssis_to_pyspark_converter.exe "input\DW.Dim.Common.Regulatory.ClaimStatus.TL.dtsx" --output "output-ClaimStatus"
```

### Batch Folder Conversion
```cmd
ssis_to_pyspark_converter.exe "input\" --output "output"
```

### With Verbose Logging
```cmd
ssis_to_pyspark_converter.exe "input\package.dtsx" --output "output" --verbose
```

### Without LLM Validation (Rule-Based Only)
```cmd
REM This is not currently supported via CLI flag
REM But the code will fall back to rule-based if LLM fails
```

---

## Features Included in This Build

✅ **Databricks-Optimized Code Generation**
- Native Databricks PySpark code
- Delta Lake format support
- Unity Catalog compatibility

✅ **Enhanced Transformations**
- Merge Join with extracted join conditions
- Lookup transformations with SQL query extraction
- OLE DB Command with SQL statement extraction
- Derived Columns with expression translation
- Conditional Splits with condition mapping

✅ **LLM-Powered Validation**
- Gemini-based code refinement
- Syntax validation
- PySpark Python API compliance
- Clean, production-ready code

✅ **Comprehensive Mapping**
- 100% component mapping rate
- Detailed mapping reports
- Conversion analysis reports

---

## Output Structure

After conversion, you'll get:

```
output/
├── parsed_json/           # Parsed SSIS package JSON
├── pyspark_code/          # Generated PySpark Databricks code
├── mapping_details/       # Component mapping details
└── analysis/              # Conversion reports and statistics
```

---

## Troubleshooting

### Issue: "API Key not found"
**Solution:** 
- Set the `GEMINI_API_KEY` environment variable
- Or create a `.env` file with the key
- The converter will work without LLM validation, but code quality may be lower

### Issue: "Python DLL not found"
**Solution:** 
- Ensure you're running on Windows
- The .exe should be standalone - if this error occurs, rebuild with PyInstaller

### Issue: "Syntax errors in generated code"
**Solution:** 
- The latest build includes improved LLM validation
- Regenerate with the updated executable
- Check that API key is set for LLM validation

### Issue: "LLM validation timeout"
**Solution:** 
- This is normal for complex packages (may take 2-3 minutes)
- The converter will save rule-based code first
- LLM validation runs in background - wait for completion

---

## System Requirements

### For Building the .exe:
- Windows 10/11
- Python 3.8+
- 4GB RAM minimum
- Internet connection

### For Running the .exe:
- Windows 10/11 (64-bit)
- No Python installation required
- 2GB RAM minimum
- Internet connection (for LLM validation)

---

## Support

For issues or questions:
1. Check the conversion logs: `ssis_converter.log`
2. Review the analysis reports in `output/analysis/`
3. Verify API keys are set correctly
4. Ensure SSIS packages are valid `.dtsx` files

---

## Version History

**Latest (2025-10-28):**
- ✅ Improved LLM validator prompt
- ✅ Enhanced Merge Join extraction
- ✅ Fixed PySpark syntax issues (<=> operator)
- ✅ Clean code generation with minimal comments
- ✅ Better error handling and timeout management

