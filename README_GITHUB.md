# SSIS to PySpark Converter

🚀 **Automated conversion tool for migrating SSIS packages to PySpark code with Databricks integration**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Overview

This tool automates the conversion of SQL Server Integration Services (SSIS) packages to PySpark code optimized for Databricks. It uses a combination of rule-based mapping and AI-powered code refinement to ensure high-quality, production-ready PySpark code.

### Key Features

- ✅ **Automated SSIS Parsing**: Extracts control flow and data flow components from `.dtsx` files
- ✅ **Intelligent Mapping**: Rule-based conversion of SSIS components to PySpark equivalents
- ✅ **AI-Powered Refinement**: Optional LLM validation and code enhancement (OpenAI, Google Gemini, or Databricks)
- ✅ **Databricks Optimization**: Generated code is optimized for Databricks runtime
- ✅ **Schema Mapping**: Support for custom schema mapping from SSIS to Databricks catalogs
- ✅ **Comprehensive Reporting**: Detailed analysis and mapping statistics
- ✅ **Batch Processing**: Convert multiple packages at once

## 🏗️ Architecture

```
┌─────────────────┐
│  SSIS Package   │
│   (.dtsx)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  XML Parser     │ ──► Extracts connections, tasks, data flows
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  JSON Mapper    │ ──► Maps SSIS components to PySpark
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Code Generator │ ──► Generates PySpark code
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  LLM Validator  │ ──► Refines and validates (optional)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PySpark Code   │
│   (.py)         │
└─────────────────┘
```

## 📦 Installation

### Prerequisites

- Python 3.8 or higher
- Git (for cloning the repository)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/gsmadan/ssis-to-pyspark-converter.git
   cd ssis-to-pyspark-converter
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API keys (optional, for AI features)**
   
   Create a `.env` file in the project root:
   ```bash
   # Choose one of the following providers:
   
   # Option 1: OpenAI
   DEFAULT_LLM_PROVIDER=openai
   OPENAI_API_KEY=your-openai-api-key
   
   # Option 2: Google Gemini
   DEFAULT_LLM_PROVIDER=gemini
   GEMINI_API_KEY=your-gemini-api-key
   
   # Option 3: Databricks Model Serving
   DEFAULT_LLM_PROVIDER=databricks
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-databricks-token
   DATABRICKS_ENDPOINT=databricks-meta-llama-3-1-405b-instruct
   ```

## 🚀 Usage

### Basic Usage

**Convert a single SSIS package:**
```bash
python ssis_to_pyspark_app.py input/Sample_Package.dtsx
```

**Convert all packages in a folder:**
```bash
python ssis_to_pyspark_app.py input/
```

### Advanced Options

**Specify custom output directory:**
```bash
python ssis_to_pyspark_app.py input/package.dtsx --output custom_output/
```

**Use schema mapping:**
```bash
python ssis_to_pyspark_app.py input/package.dtsx --schema-mapping schema_mapping.json
```

**Skip AI validation (rule-based only):**
```bash
python ssis_to_pyspark_app.py input/package.dtsx --no-validation
```

**Enable verbose logging:**
```bash
python ssis_to_pyspark_app.py input/package.dtsx --verbose
```

### Schema Mapping

Create a `schema_mapping.json` file to map SSIS connections to Databricks schemas:

```json
{
  "connection_mappings": {
    "SourceDB": {
      "catalog": "dev_catalog",
      "schema": "bronze_layer",
      "description": "Source database connection"
    },
    "TargetDB": {
      "catalog": "dev_catalog",
      "schema": "silver_layer",
      "description": "Target database connection"
    }
  }
}
```

## 📊 Output Structure

After conversion, you'll find:

```
output/
├── parsed_json/          # Intermediate JSON representation
│   └── Package_data_engineering.json
├── pyspark_code/         # Generated PySpark code
│   └── Package_pyspark.py
├── mapping_details/      # Mapping statistics
│   └── Package_mapping.json
└── analysis/            # Conversion analysis report
    └── conversion_report.json
```

## 🎯 Supported SSIS Components

### Data Flow Components

| SSIS Component | PySpark Equivalent | Support Level |
|----------------|-------------------|---------------|
| OLE DB Source | `spark.table()` / `spark.sql()` | ✅ Full |
| OLE DB Destination | `df.write.saveAsTable()` | ✅ Full |
| Derived Column | `df.withColumn()` | ✅ Full |
| Conditional Split | `df.filter()` | ✅ Full |
| Lookup | `df.join()` | ✅ Full |
| Merge Join | `df.join()` | ✅ Full |
| Union All | `df.union()` | ✅ Full |
| Sort | `df.orderBy()` | ✅ Full |
| Aggregate | `df.groupBy().agg()` | ✅ Full |
| Data Conversion | `df.withColumn().cast()` | ✅ Full |
| Multicast | Multiple DataFrame assignments | ✅ Full |

### Control Flow Components

| SSIS Component | PySpark Equivalent | Support Level |
|----------------|-------------------|---------------|
| Execute SQL Task | `spark.sql()` | ✅ Full |
| Data Flow Task | PySpark DataFrame operations | ✅ Full |
| Sequence Container | Code blocks with comments | ✅ Full |
| For Loop Container | Python `for` loop | ⚠️ Partial |
| Foreach Loop Container | Python `for` loop | ⚠️ Partial |

## 🧪 Running on Databricks

### Option 1: Upload as Notebook

1. Convert your SSIS package locally
2. Upload the generated `.py` file to Databricks workspace
3. Run as a Databricks notebook

### Option 2: Run as Job

1. Package the converter as a wheel or zip
2. Create a Databricks job
3. Attach the package and run

### Option 3: Use Databricks Volumes

```python
# In a Databricks notebook
import sys
sys.path.append("/Volumes/catalog/schema/ssis_converter/")

from ssis_to_pyspark_app import SSISToPySparkApp

app = SSISToPySparkApp(databricks_mode=True)
result = app.convert_single_package("/Volumes/catalog/schema/input/package.dtsx")
```

## 📈 Performance

- **Parsing**: ~2-5 seconds per package
- **Mapping**: ~1-3 seconds per package
- **AI Validation**: ~10-30 seconds per package (if enabled)

## 🛠️ Development

### Project Structure

```
ssis-to-pyspark-agent/
├── parsing/                    # SSIS XML parsing
│   └── data_engineering_parser.py
├── mapping/                    # Component mapping logic
│   ├── enhanced_json_mapper.py
│   ├── schema_mapper.py
│   └── expression_translator.py
├── code_generation/           # Code generation and validation
│   ├── llm_code_validator.py
│   └── databricks_client.py
├── ssis_to_pyspark_app.py    # Main application
├── config.py                  # Configuration management
├── models.py                  # Data models
└── requirements.txt           # Dependencies
```

### Running Tests

```bash
pytest testing/
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built for enterprise SSIS to Databricks migrations
- Supports OpenAI GPT-4, Google Gemini, and Databricks Foundation Models
- Optimized for Databricks Unity Catalog

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**⭐ If you find this tool useful, please consider giving it a star!**
