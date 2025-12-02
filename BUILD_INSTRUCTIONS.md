# Build Instructions - Updated Executable

## 🚀 Quick Build

Simply run:
```bash
build_exe.bat
```

Then select:
- **Option 2** for CLI only (recommended)
- **Option 3** for both GUI and CLI

## 📋 What's Included in the Updated Build

### ✅ Latest Fixes (October 20, 2025)
1. **SQL Query Syntax** - Properly formatted SQL queries in PySpark
2. **Connection Variables** - Source and destination use actual connection variables
3. **DataFrame Flow** - Correct DataFrame variable names throughout
4. **Indentation** - Proper code formatting
5. **Import Cleanup** - Removed circular import issues
6. **Disabled Tasks** - Filtered out disabled SSIS tasks
7. **Execution Order** - Topological sort for correct task ordering
8. **AccessMode Support** - SQL queries vs table access properly handled

### 📦 Build Output

After running `build_exe.bat`, you'll get:

```
dist/
└── ssis_to_pyspark_converter.exe   # Standalone executable (~25-30 MB)
```

## 🎯 Build Options

### Option 1: GUI Application
- Graphical user interface
- File browser for input selection
- Visual progress indicators
- Builds: `ssis_to_pyspark_converter.exe` (GUI version)

### Option 2: CLI Application (Recommended)
- Command-line interface
- Scriptable and automatable
- Better for production pipelines
- Builds: `ssis_to_pyspark_converter.exe` (CLI version)

### Option 3: Both
- Builds both GUI and CLI versions
- Different executables for different use cases

## 💻 Usage After Build

### CLI Usage:
```bash
# Single file
dist\ssis_to_pyspark_converter.exe input\DM.Dim.Regulatory.BrokerGroupings.L.dtsx

# Entire folder
dist\ssis_to_pyspark_converter.exe input\

# With output directory
dist\ssis_to_pyspark_converter.exe input\package.dtsx -o custom_output\
```

### GUI Usage:
```bash
# Just double-click the .exe file
dist\ssis_to_pyspark_converter.exe
```

## 📝 What Gets Built Into the EXE

- ✅ All Python code (parsing, mapping, code generation)
- ✅ All dependencies (no need for Python on target machine)
- ✅ Configuration files
- ✅ Models and schemas
- ✅ Expression translator
- ✅ All recent fixes and improvements

## 🔧 Build Requirements

- Python 3.7+ installed
- PyInstaller (auto-installed by batch file)
- ~100 MB free disk space for build

## ✨ Features in Updated Build

1. **Correct SQL Queries** - Using `.option("query", ...)` for SQL commands
2. **Connection Variables** - Using `dwcommon_url`, `dmregulatory_url`, etc.
3. **Proper Execution Order** - Tasks execute in correct dependency order
4. **Clean Code** - No import conflicts, proper indentation
5. **Active Writes** - Destination writes are uncommented and functional
6. **DataFrame Tracking** - Correct DataFrame variables throughout transformations

## 🎁 Deployment

The built `.exe` file is completely standalone:
- ✅ No Python installation needed
- ✅ No pip packages needed
- ✅ No configuration needed
- ✅ Just copy and run!

Perfect for:
- Client deployments
- Production environments
- Air-gapped systems
- Non-technical users

## 📊 Expected Build Time

- CLI build: ~2-3 minutes
- GUI build: ~3-4 minutes
- Both: ~5-6 minutes

## ✅ Success Indicators

After successful build, you should see:
```
[SUCCESS] Build completed successfully!

Built executables in dist\ folder:
ssis_to_pyspark_converter.exe - Size: XXXXXXX bytes

These .exe files can be deployed without Python!
```

## 🚨 Troubleshooting

If build fails:
1. Check Python installation: `python --version`
2. Ensure PyInstaller is installed: `pip install pyinstaller`
3. Clear build cache: Delete `build\` and `dist\` folders
4. Try running as administrator

---

**Ready to build? Run `build_exe.bat` and select option 1!** 🚀

