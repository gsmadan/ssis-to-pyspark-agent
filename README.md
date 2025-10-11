# SSIS to PySpark Converter Agent

A comprehensive system for converting SQL Server Integration Services (SSIS) packages (.dtsx files) to PySpark code (.py files) for deployment in Databricks.

## 🚀 Features

- **Complete SSIS Package Parsing**: Extracts components, control flow, data flows, variables, and connection managers
- **Intelligent Mapping**: Translates SSIS tasks to equivalent PySpark functions
- **LLM-Powered Code Generation**: Uses OpenAI GPT-4 or Gemini 2.5 flash for intelligent code generation
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


## 📊 Roadmap

- [ ] Support for more SSIS components
- [ ] Enhanced error handling
- [ ] Performance benchmarking
- [ ] Integration with CI/CD pipelines
- [ ] Web-based interface
- [ ] Real-time conversion monitoring
