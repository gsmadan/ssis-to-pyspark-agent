# SSIS to PySpark Converter

A comprehensive system for converting SQL Server Integration Services (SSIS) packages (.dtsx files) to PySpark code (.py files) for deployment in Databricks.

## 🚀 Features

- **Complete SSIS Package Parsing**: Extracts components, control flow, data flows, variables, and connection managers
- **Intelligent Mapping**: Translates SSIS tasks to equivalent PySpark functions
- **LLM-Powered Code Generation**: Uses OpenAI GPT-4 or Anthropic Claude for intelligent code generation
- **Template-Based Generation**: Jinja2 templates for structured code generation
- **Comprehensive Validation**: Syntax, logic, and best practices validation
- **Databricks Ready**: Generated code optimized for Databricks deployment
- **CLI Interface**: Easy-to-use command-line interface
- **Batch Processing**: Convert multiple packages at once

## 📋 System Architecture

The system follows a modular architecture with four main layers:

1. **Parsing Layer**: Extracts SSIS components from .dtsx files
2. **Mapping Layer**: Translates SSIS tasks to PySpark functions
3. **Code Generation Layer**: Generates PySpark code using LLM and templates
4. **Validation Layer**: Validates syntax, logic, and best practices

## 🛠️ Installation

### Prerequisites

- Python 3.8 or higher
- OpenAI API key or Anthropic API key

### Install from Source

```bash
git clone https://github.com/your-org/ssis-to-pyspark-converter.git
cd ssis-to-pyspark-converter
pip install -e .
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## ⚙️ Configuration

1. Copy the example configuration file:
```bash
cp env_example.txt .env
```

2. Edit `.env` with your settings:
```bash
# LLM Configuration
OPENAI_API_KEY=your_openai_api_key_here
DEFAULT_LLM_PROVIDER=openai
DEFAULT_MODEL=gpt-4

# System Settings
OUTPUT_DIRECTORY=./output
LOG_LEVEL=INFO
```

## 🚀 Quick Start

### Command Line Usage

#### Convert a Single Package
```bash
ssis-to-pyspark convert input_package.dtsx --output converted_package.py
```

#### Convert Multiple Packages
```bash
ssis-to-pyspark batch ./ssis_packages/ --output-dir ./converted_packages/
```

#### Validate a Package
```bash
ssis-to-pyspark validate input_package.dtsx
```

#### Test the Converter
```bash
ssis-to-pyspark test
```

### Python API Usage

```python
from ssis_to_pyspark_converter import SSISToPySparkConverter

# Initialize converter
converter = SSISToPySparkConverter(
    use_llm=True,
    use_templates=True,
    generation_strategy="hybrid"
)

# Convert a package
result = converter.convert_package(
    input_file="input_package.dtsx",
    output_file="output_package.py",
    validation_level="comprehensive"
)

# Check results
if result.success:
    print(f"Conversion successful: {result.output_file}")
    print(f"Coverage score: {result.validation_result.coverage_score}")
else:
    print(f"Conversion failed: {result.error_message}")
```

## 📊 Supported SSIS Components

### Control Flow Tasks
- ✅ Data Flow Task
- ✅ Execute SQL Task
- ✅ Script Task
- ✅ File System Task
- ✅ FTP Task
- ✅ Web Service Task
- ✅ Expression Task
- ✅ Loop Containers (For Loop, Foreach Loop)
- ✅ Sequence Container

### Data Sources
- ✅ SQL Server
- ✅ Oracle
- ✅ MySQL
- ✅ PostgreSQL
- ✅ CSV Files
- ✅ Excel Files
- ✅ Flat Files
- ✅ XML Files
- ✅ JSON Files

### Data Transformations
- ✅ Lookup Transformation
- ✅ Merge Join Transformation
- ✅ Conditional Split Transformation
- ✅ Derived Column Transformation
- ✅ Data Conversion Transformation
- ✅ Sort Transformation
- ✅ Aggregate Transformation
- ✅ Union All Transformation
- ✅ Multicast Transformation
- ✅ Row Count Transformation

## 🔧 Code Generation Strategies

### 1. LLM-Only Strategy
Uses only Large Language Models for code generation:
```python
converter = SSISToPySparkConverter(generation_strategy="llm")
```

### 2. Template-Only Strategy
Uses only Jinja2 templates for code generation:
```python
converter = SSISToPySparkConverter(generation_strategy="template")
```

### 3. Hybrid Strategy (Recommended)
Combines LLM and templates for optimal results:
```python
converter = SSISToPySparkConverter(generation_strategy="hybrid")
```

## 📝 Generated Code Example

The converter generates comprehensive PySpark code:

```python
# Generated PySpark Script from SSIS Package: SamplePackage
# Version: 1.0
# Generated on: 2024-01-15T10:30:00

# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("SamplePackage_Conversion") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main execution function."""
    try:
        logger.info("Starting SSIS to PySpark conversion execution")
        
        # Variables
        test_variable = "TestValue"
        
        # Connection configurations
        test_connection_config = {
            "url": "jdbc:sqlserver://server:port;database=db",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        
        # Execute data flows
        execute_data_flow_1()
        
        # Execute control flow tasks
        execute_task_1()
        
        logger.info("Execution completed successfully")
        
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        raise
    finally:
        cleanup_resources()

def execute_data_flow_1():
    """Execute data flow 1."""
    try:
        logger.info("Executing data flow 1")
        
        # Data sources
        source_df = load_data_source_1()
        
        # Transformations
        transformed_df = apply_transformation_1()
        
        # Destinations
        save_data_destination_1()
        
    except Exception as e:
        logger.error(f"Data flow 1 failed: {e}")
        raise

def cleanup_resources():
    """Clean up resources."""
    try:
        logger.info("Resources cleaned up")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    main()
```

## 🔍 Validation Features

### Syntax Validation
- Python syntax checking
- PySpark-specific syntax validation
- Import statement validation

### Logic Validation
- Data flow consistency checking
- Control flow execution order validation
- Variable usage validation
- Connection manager validation

### Best Practices Validation
- Performance optimization suggestions
- Error handling recommendations
- Logging best practices
- Security considerations
- Databricks-specific optimizations

## 📈 Performance Considerations

The generated code includes performance optimizations:

- **Adaptive Query Execution**: Enabled by default
- **Partition Management**: Automatic coalescing and repartitioning
- **Caching Strategies**: Recommendations for DataFrame caching
- **Broadcast Joins**: Automatic detection for small tables
- **Resource Management**: Proper cleanup and resource management

## 🚀 Databricks Deployment

### Prerequisites
1. Databricks workspace access
2. PySpark 3.x runtime
3. Required libraries (specified in generated code)

### Deployment Steps
1. Convert your SSIS package using the converter
2. Review and test the generated code
3. Update connection strings and credentials
4. Deploy to Databricks workspace
5. Configure job scheduling and monitoring

### Databricks-Specific Features
- Delta Lake integration recommendations
- Cluster optimization settings
- Notebook-specific optimizations
- Job configuration templates

## 🧪 Testing

Run the test suite:
```bash
pytest tests/
```

Run specific tests:
```bash
pytest tests/test_parser.py
pytest tests/test_mapper.py
pytest tests/test_code_generator.py
pytest tests/test_validator.py
```

## 📚 API Reference

### SSISToPySparkConverter

Main converter class for orchestrating the conversion process.

#### Methods

- `convert_package(input_file, output_file=None, validation_level="comprehensive")`: Convert a single SSIS package
- `convert_package_from_string(xml_content, package_name, output_file=None, validation_level="comprehensive")`: Convert from XML string
- `validate_package(input_file)`: Validate an SSIS package
- `batch_convert(input_directory, output_directory=None, validation_level="comprehensive")`: Convert multiple packages
- `get_conversion_summary(conversion_result)`: Get human-readable summary

### ValidationAgent

Comprehensive validation agent for generated code.

#### Methods

- `validate_generated_code(generated_code, ssis_package, validation_level="comprehensive")`: Validate generated code
- `validate_syntax_only(generated_code)`: Syntax-only validation
- `validate_logic_only(generated_code, ssis_package)`: Logic-only validation
- `validate_best_practices_only(generated_code)`: Best practices-only validation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Documentation**: [Read the Docs](https://ssis-to-pyspark-converter.readthedocs.io/)
- **Issues**: [GitHub Issues](https://github.com/your-org/ssis-to-pyspark-converter/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/ssis-to-pyspark-converter/discussions)

## 🙏 Acknowledgments

- OpenAI for GPT-4 API
- Anthropic for Claude API
- Apache Spark community
- Databricks team
- SSIS community

## 📊 Roadmap

- [ ] Support for more SSIS components
- [ ] Enhanced error handling
- [ ] Performance benchmarking
- [ ] Integration with CI/CD pipelines
- [ ] Web-based interface
- [ ] Real-time conversion monitoring
