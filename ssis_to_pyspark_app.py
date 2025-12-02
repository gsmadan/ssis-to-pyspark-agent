#!/usr/bin/env python3
"""
SSIS to PySpark Converter - Main Application
Standalone executable for converting SSIS packages to PySpark code.

This application can be packaged as a .exe file for deployment in client environments.
"""

import sys
import os
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging

# Add current directory to path for bundled exe
if getattr(sys, 'frozen', False):
    # Running as compiled exe
    application_path = sys._MEIPASS
else:
    # Running as script
    application_path = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, application_path)

from parsing.data_engineering_parser import DataEngineeringParser
from mapping.enhanced_json_mapper import EnhancedJSONMapper
from mapping.schema_mapper import SchemaMapper
from code_generation.llm_code_validator import LLMCodeValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ssis_converter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SSISToPySparkApp:
    """Main application class for SSIS to PySpark conversion."""
    
    def __init__(self, databricks_mode: bool = True, schema_mapping_file: Optional[str] = None, skip_validation: bool = False):
        self.parser = DataEngineeringParser()
        
        # Initialize schema mapper if mapping file provided
        self.schema_mapper = SchemaMapper(schema_mapping_file) if schema_mapping_file else None
        
        self.mapper = EnhancedJSONMapper(
            use_llm_fallback=False, 
            databricks_mode=databricks_mode,
            schema_mapper=self.schema_mapper
        )  # LLM fallback disabled - using rule-based mappings only
        self.validator = LLMCodeValidator()  # Initialize LLM validator for output refinement
        self.skip_validation = skip_validation  # Store skip validation flag
        self.output_base = Path('output')
        
        # Create output directories
        self._ensure_output_directories()
    
    def _ensure_output_directories(self):
        """Ensure all output directories exist."""
        (self.output_base / 'parsed_json').mkdir(parents=True, exist_ok=True)
        (self.output_base / 'pyspark_code').mkdir(parents=True, exist_ok=True)
        (self.output_base / 'mapping_details').mkdir(parents=True, exist_ok=True)
        (self.output_base / 'analysis').mkdir(parents=True, exist_ok=True)
    
    def convert_single_package(self, dtsx_path: str) -> dict:
        """
        Convert a single SSIS package to PySpark.
        
        Args:
            dtsx_path: Path to the .dtsx file
            
        Returns:
            Dictionary with conversion results
        """
        dtsx_file = Path(dtsx_path)
        
        if not dtsx_file.exists():
            raise FileNotFoundError(f"SSIS package not found: {dtsx_path}")
        
        logger.info(f"Converting: {dtsx_file.name}")
        
        # Step 1: Parse SSIS package
        logger.info("Step 1/4: Parsing SSIS package...")
        parsed_data = self.parser.parse_dtsx_file(str(dtsx_file))
        package_name = parsed_data.get('package_name', 'Unknown')
        
        logger.info(f"  [OK] Package: {package_name}")
        logger.info(f"       - Connections: {len(parsed_data.get('connections', {}))}")
        logger.info(f"       - SQL Tasks: {len(parsed_data.get('sql_tasks', []))}")
        logger.info(f"       - Data Flows: {len(parsed_data.get('data_flows', []))}")
        
        # Save parsed JSON
        json_filename = dtsx_file.stem + "_data_engineering.json"
        json_path = self.output_base / 'parsed_json' / json_filename
        
        with open(json_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)
        logger.info(f"  [SAVED] Parsed JSON: {json_path}")
        
        # Step 2: Map to PySpark
        logger.info("Step 2/4: Mapping to PySpark...")
        mapping_result = self.mapper.map_json_package(str(json_path))
        
        stats = mapping_result['statistics']
        logger.info(f"  [OK] Mapping Rate: {stats['overall_mapping_rate']}%")
        logger.info(f"       - Sources: {stats['mapped_sources']}/{stats['total_sources']}")
        logger.info(f"       - Transformations: {stats['mapped_transformations']}/{stats['total_transformations']}")
        logger.info(f"       - Destinations: {stats['mapped_destinations']}/{stats['total_destinations']}")
        
        # Save rule-based code first (before LLM validation)
        pyspark_filename = dtsx_file.stem + "_pyspark.py"
        pyspark_path = self.output_base / 'pyspark_code' / pyspark_filename
        
        # Save intermediate rule-based code
        with open(pyspark_path, 'w') as f:
            f.write(mapping_result['pyspark_code'])
        logger.info(f"  [SAVED] Rule-based PySpark Code: {pyspark_path}")
        
        # Step 3: Validate and refine code with LLM (only when client available and heuristics pass)
        use_llm_validation = (
            not self.skip_validation and  # Check skip_validation flag first
            bool(getattr(self.validator, 'client', None)) and
            self._should_use_llm_validation(parsed_data, stats)
        )
        
        if use_llm_validation:
            logger.info("Step 3/4: Validating and refining PySpark code with LLM...")
            logger.info(f"  [INFO] Package complexity detected - using LLM validation")
            try:
                refined_code = self.validator.validate_and_refine_code(
                    pyspark_code=mapping_result['pyspark_code'],
                    package_name=package_name,
                    parsed_json=parsed_data,
                    mapping_details=mapping_result,
                    databricks_mode=True
                )
                logger.info("  [OK] LLM validation and refinement: COMPLETED")
                
                # Save refined code (overwrite rule-based code)
                with open(pyspark_path, 'w') as f:
                    f.write(refined_code)
                logger.info(f"  [SAVED] LLM-refined PySpark Code: {pyspark_path}")
            except Exception as e:
                logger.warning(f"  [WARNING] LLM validation failed: {e}")
                logger.warning("  [FALLBACK] Using original rule-based code (already saved)")
                refined_code = mapping_result['pyspark_code']
        else:
            if self.skip_validation:
                logger.info("Step 3/4: Skipping LLM validation - --no-validation flag set")
            else:
                logger.info("Step 3/4: Skipping LLM validation - simple package detected")
                logger.info(f"  [INFO] Package is simple (transformations: {stats.get('total_transformations', 0)}, "
                           f"complex logic: {self._has_complex_logic(parsed_data)})")
            refined_code = mapping_result['pyspark_code']
        
        # Save mapping details
        mapping_filename = dtsx_file.stem + "_mapping.json"
        mapping_path = self.output_base / 'mapping_details' / mapping_filename
        
        result_copy = mapping_result.copy()
        result_copy.pop('pyspark_code', None)
        
        with open(mapping_path, 'w') as f:
            json.dump(result_copy, f, indent=2)
        logger.info(f"  [SAVED] Mapping Details: {mapping_path}")
        
        # Step 4: Validate syntax and analyze final code
        logger.info("Step 4/4: Validating Python syntax and analyzing final code...")
        import ast
        import re
        
        with open(pyspark_path, 'r') as f:
            code = f.read()
        
        syntax_valid = False
        syntax_errors = []
        code_analysis = self._analyze_final_code(code, stats)
        
        try:
            ast.parse(code)
            syntax_valid = True
            logger.info("  [OK] Syntax validation: PASSED")
        except SyntaxError as e:
            syntax_errors.append({
                'line': e.lineno,
                'message': e.msg,
                'text': e.text
            })
            logger.warning(f"  [WARNING] Syntax validation: FAILED at line {e.lineno}")
        
        # Return results with code analysis
        return {
            'success': True,
            'package_name': package_name,
            'package_file': dtsx_file.name,
            'parsed_json': str(json_path),
            'pyspark_code': str(pyspark_path),
            'mapping_details': str(mapping_path),
            'mapping_rate': stats['overall_mapping_rate'],
            'syntax_valid': syntax_valid,
            'syntax_errors': syntax_errors,
            'statistics': stats,
            'code_analysis': code_analysis,
            'llm_validated': refined_code != mapping_result['pyspark_code']
        }
    
    def convert_folder(self, folder_path: str) -> list:
        """
        Convert all SSIS packages in a folder.
        
        Args:
            folder_path: Path to folder containing .dtsx files
            
        Returns:
            List of conversion results
        """
        folder = Path(folder_path)
        
        if not folder.exists() or not folder.is_dir():
            raise NotADirectoryError(f"Folder not found: {folder_path}")
        
        # Find all .dtsx files
        dtsx_files = list(folder.glob('*.dtsx'))
        
        if not dtsx_files:
            logger.warning(f"No .dtsx files found in: {folder_path}")
            return []
        
        logger.info(f"Found {len(dtsx_files)} SSIS package(s) in: {folder_path}")
        logger.info("="*60)
        
        results = []
        
        for i, dtsx_file in enumerate(dtsx_files, 1):
            logger.info(f"\n[{i}/{len(dtsx_files)}] Processing: {dtsx_file.name}")
            logger.info("-"*60)
            
            try:
                result = self.convert_single_package(str(dtsx_file))
                results.append(result)
                logger.info(f"[SUCCESS] {dtsx_file.name}")
            except Exception as e:
                logger.error(f"[FAILED] {dtsx_file.name} - {e}")
                results.append({
                    'success': False,
                    'package_file': dtsx_file.name,
                    'error': str(e)
                })
        
        return results
    
    def _should_use_llm_validation(self, parsed_data: dict, stats: dict) -> bool:
        """
        Determine if LLM validation should be used.
        Enabled when complex logic exists or significant transformations (>3) are present.
        """
        if not self.validator or not self.validator.client:
            return False

        if self._has_complex_logic(parsed_data):
            return True

        return self._count_significant_transformations(parsed_data) > 3

    def _count_significant_transformations(self, parsed_data: dict) -> int:
        """Count transformations excluding pure row-count components."""
        skip_prefixes = ("RC ", "TD ")
        skip_types = {"ROW_COUNT", "RC_INSERT", "RC_UPDATE", "RC_DELETE", "RC_INTERMEDIATE", "TRASH_DESTINATION"}

        count = 0
        for data_flow in parsed_data.get('data_flows', []):
            for transformations in data_flow.get('transformations', []):
                name = transformations.get('name', '')
                transf_type = transformations.get('type', '').upper()

                if name.startswith(skip_prefixes):
                    continue
                if transf_type in skip_types:
                    continue
                count += 1

        return count
    
    def _has_complex_logic(self, parsed_data: dict) -> bool:
        """
        Check if package contains complex logic.
        
        Args:
            parsed_data: Parsed SSIS package data
            
        Returns:
            True if complex logic is detected, False otherwise
        """
        # Check for derived columns, lookups, merges/joins, OLE DB commands
        for data_flow in parsed_data.get('data_flows', []):
            for transformation in data_flow.get('transformations', []):
                transf_type = transformation.get('type', '').upper()
                
                if transf_type in {'DERIVED_COLUMN', 'LOOKUP', 'MERGE', 'MERGE_JOIN', 'OLEDB_COMMAND', 'SORT'}:
                    return True
                
                # Check conditional splits
                elif transf_type == 'CONDITIONAL_SPLIT':
                    conditions = transformation.get('conditions', [])
                    if len(conditions) > 2:
                        return True
                
                # Check for merges/joins
                elif transf_type in ['MERGE', 'MERGE_JOIN']:
                    if transformation.get('join_conditions') or transformation.get('join_keys'):
                        return True
        
        return False
    
    def _analyze_final_code(self, code: str, stats: dict) -> dict:
        """Analyze the final PySpark code after LLM validation."""
        import re
        
        # Count TODOs
        todos = re.findall(r'# TODO[^\n]*', code)
        todo_count = len(todos)
        
        # Count transformations
        transformations = re.findall(r'# Transformation: ([^\n]+)', code)
        transformation_count = len(transformations)
        
        # Check for LLM-generated patterns (indicates LLM was used)
        has_llm_patterns = bool(re.search(r'(lit\(|when\(|otherwise\(|col\(["\']|withColumn\(|filter\(|join\(|groupBy\()', code))
        
        # Count code lines (excluding empty and comment-only lines)
        code_lines = [line for line in code.split('\n') if line.strip() and not line.strip().startswith('#')]
        effective_lines = len(code_lines)
        total_lines = len(code.split('\n'))
        
        # Identify transformation types present
        trans_types = {}
        for trans_line in transformations:
            if '(' in trans_line and ')' in trans_line:
                # Extract type from "Name (TYPE)"
                match = re.search(r'\(([^)]+)\)', trans_line)
                if match:
                    trans_type = match.group(1)
                    trans_types[trans_type] = trans_types.get(trans_type, 0) + 1
        
        completion_rate = 100
        if transformation_count > 0:
            completion_ratio = 1 - (todo_count / transformation_count)
            completion_rate = round(max(0.0, min(1.0, completion_ratio)) * 100, 2)

        return {
            'total_lines': total_lines,
            'effective_lines': effective_lines,
            'todo_count': todo_count,
            'transformation_count': transformation_count,
            'transformation_types': trans_types,
            'has_llm_patterns': has_llm_patterns,
            'completion_rate': completion_rate
        }
    
    def _count_llm_improvements(self, code: str) -> dict:
        """Count transformations that have LLM-generated improvements."""
        import re
        llm_indicators = {
            'derived_columns': len(re.findall(r'withColumn\([^)]*when\(|withColumn\([^)]*col\(', code)),
            'conditional_logic': len(re.findall(r'filter\([^)]*col\(.*(==|!=|>|<)', code)),
            'null_handling': len(re.findall(r'(isNotNull|isNull|coalesce)\(', code)),
            'complex_expressions': len(re.findall(r'(when\(.*otherwise\(|lit\(.*\).*cast\()', code))
        }
        return llm_indicators
    
    def _generate_unified_analysis(self, results: list) -> dict:
        """Generate comprehensive analysis report based on final output."""
        import re
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]
        
        # Overall statistics
        total_packages = len(results)
        successful_count = len(successful)
        failed_count = len(failed)
        
        # Aggregate parsing statistics
        total_parsed = {
            'connections': 0,
            'sql_tasks': 0,
            'sources': 0,
            'transformations': 0,
            'destinations': 0
        }
        
        # Aggregate mapping statistics
        total_mapped = {
            'connections': 0,
            'sql_tasks': 0,
            'sources': 0,
            'transformations': 0,
            'destinations': 0
        }
        
        # LLM statistics
        llm_improved_packages = 0
        llm_improvements = {
            'derived_columns': 0,
            'conditional_logic': 0,
            'null_handling': 0,
            'complex_expressions': 0
        }
        
        # Calculate aggregate metrics
        avg_mapping_rate = 0
        syntax_valid_count = 0
        
        # Package details with full breakdown
        packages = []
        for result in successful:
            stats = result.get('statistics', {})
            code_analysis = result.get('code_analysis', {})
            
            # Accumulate parsing stats
            total_parsed['connections'] += stats.get('total_connections', 0)
            total_parsed['sql_tasks'] += stats.get('total_sql_tasks', 0)
            total_parsed['sources'] += stats.get('total_sources', 0)
            total_parsed['transformations'] += stats.get('total_transformations', 0)
            total_parsed['destinations'] += stats.get('total_destinations', 0)
            
            # Accumulate mapping stats
            total_mapped['connections'] += stats.get('mapped_connections', 0)
            total_mapped['sql_tasks'] += stats.get('mapped_sql_tasks', 0)
            total_mapped['sources'] += stats.get('mapped_sources', 0)
            total_mapped['transformations'] += stats.get('mapped_transformations', 0)
            total_mapped['destinations'] += stats.get('mapped_destinations', 0)
            
            # LLM improvements
            if result.get('llm_validated', False):
                llm_improved_packages += 1
                # Count LLM improvements in code
                pyspark_path = Path(result.get('pyspark_code', ''))
                if pyspark_path.exists():
                    with open(pyspark_path, 'r') as f:
                        code = f.read()
                    improvements = self._count_llm_improvements(code)
                    for key in llm_improvements:
                        llm_improvements[key] += improvements[key]
            
            avg_mapping_rate += result.get('mapping_rate', 0)
            if result.get('syntax_valid'):
                syntax_valid_count += 1
            
            # Detailed package breakdown
            trans_types = code_analysis.get('transformation_types', {})
            
            # Read code for LLM improvements if LLM validated
            llm_improvements_dict = {}
            if result.get('llm_validated'):
                pyspark_path = Path(result.get('pyspark_code', ''))
                if pyspark_path.exists():
                    with open(pyspark_path, 'r') as f:
                        code_content = f.read()
                    llm_improvements_dict = self._count_llm_improvements(code_content)
            
            # Format syntax errors for report (only include if syntax is invalid)
            syntax_errors_summary = None
            syntax_valid = result.get('syntax_valid', False)
            if not syntax_valid:
                syntax_errors = result.get('syntax_errors', [])
                if syntax_errors:
                    # Create short summary of errors: "Line X: message; Line Y: message"
                    error_summaries = [f"Line {err.get('line', '?')}: {err.get('message', 'Unknown error')}" 
                                     for err in syntax_errors]
                    syntax_errors_summary = '; '.join(error_summaries)
            
            packages.append({
                'name': result.get('package_name', 'Unknown'),
                'file': result.get('package_file', ''),
                'status': 'success',
                'syntax_valid': syntax_valid,
                'syntax_errors': syntax_errors_summary,
                'llm_validated': result.get('llm_validated', False),
                'code_size': f"{code_analysis.get('effective_lines', 0)} lines",
                'pyspark_file': str(result.get('pyspark_code', '')).replace('\\', '/'),
                'parsing': {
                    'connections': stats.get('total_connections', 0),
                    'sql_tasks': stats.get('total_sql_tasks', 0),
                    'sources': stats.get('total_sources', 0),
                    'transformations': stats.get('total_transformations', 0),
                    'destinations': stats.get('total_destinations', 0)
                },
                'mapping': {
                    'connections': f"{stats.get('mapped_connections', 0)}/{stats.get('total_connections', 0)}",
                    'sql_tasks': f"{stats.get('mapped_sql_tasks', 0)}/{stats.get('total_sql_tasks', 0)}",
                    'sources': f"{stats.get('mapped_sources', 0)}/{stats.get('total_sources', 0)}",
                    'transformations': f"{stats.get('mapped_transformations', 0)}/{stats.get('total_transformations', 0)}",
                    'destinations': f"{stats.get('mapped_destinations', 0)}/{stats.get('total_destinations', 0)}",
                    'overall_rate': round(result.get('mapping_rate', 0), 1)
                },
                'transformations_implemented': code_analysis.get('transformation_count', 0),
                'transformation_types': {k: v for k, v in trans_types.items()},
                'llm_improvements': llm_improvements_dict
            })
        
        if successful:
            avg_mapping_rate /= len(successful)
        
        for result in failed:
            packages.append({
                'name': result.get('package_name', 'Unknown'),
                'file': result.get('package_file', ''),
                'status': 'failed',
                'error': result.get('error', 'Unknown error')
            })
        
        return {
            'timestamp': datetime.now().isoformat(),
            'process_overview': {
                'total_packages': total_packages,
                'successful': successful_count,
                'failed': failed_count,
                'success_rate': round((successful_count / total_packages * 100) if total_packages > 0 else 0, 1),
                'syntax_valid': syntax_valid_count,
                'packages_llm_validated': llm_improved_packages
            },
            'parsing_statistics': {
                'connections': total_parsed['connections'],
                'sql_tasks': total_parsed['sql_tasks'],
                'sources': total_parsed['sources'],
                'transformations': total_parsed['transformations'],
                'destinations': total_parsed['destinations'],
                'total_components': sum(total_parsed.values())
            },
            'mapping_statistics': {
                'rule_based': {
                    'connections': f"{total_mapped['connections']}/{total_parsed['connections']}",
                    'sql_tasks': f"{total_mapped['sql_tasks']}/{total_parsed['sql_tasks']}",
                    'sources': f"{total_mapped['sources']}/{total_parsed['sources']}",
                    'transformations': f"{total_mapped['transformations']}/{total_parsed['transformations']}",
                    'destinations': f"{total_mapped['destinations']}/{total_parsed['destinations']}"
                },
                'mapping_rates': {
                    'connections': round((total_mapped['connections'] / total_parsed['connections'] * 100) if total_parsed['connections'] > 0 else 0, 1),
                    'sql_tasks': round((total_mapped['sql_tasks'] / total_parsed['sql_tasks'] * 100) if total_parsed['sql_tasks'] > 0 else 0, 1),
                    'sources': round((total_mapped['sources'] / total_parsed['sources'] * 100) if total_parsed['sources'] > 0 else 0, 1),
                    'transformations': round((total_mapped['transformations'] / total_parsed['transformations'] * 100) if total_parsed['transformations'] > 0 else 0, 1),
                    'destinations': round((total_mapped['destinations'] / total_parsed['destinations'] * 100) if total_parsed['destinations'] > 0 else 0, 1),
                    'overall': round(avg_mapping_rate, 1)
                }
            },
            'llm_refinement': {
                'packages_refined': llm_improved_packages,
                'improvements_made': {
                    'derived_columns_enhanced': llm_improvements['derived_columns'],
                    'conditional_logic_added': llm_improvements['conditional_logic'],
                    'null_handling_improved': llm_improvements['null_handling'],
                    'complex_expressions_implemented': llm_improvements['complex_expressions'],
                    'total_improvements': sum(llm_improvements.values())
                }
            },
            'packages': packages
        }
    
    def print_summary(self, results: list):
        """Print conversion summary."""
        if not results:
            logger.info("\nNo files processed.")
            return
        
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]
        
        logger.info("\n" + "="*60)
        logger.info("CONVERSION SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Packages: {len(results)}")
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")
        
        if successful:
            avg_mapping = sum(r['mapping_rate'] for r in successful) / len(successful)
            syntax_valid = sum(1 for r in successful if r['syntax_valid'])
            logger.info(f"\nAverage Mapping Rate: {avg_mapping:.1f}%")
            logger.info(f"Syntax Valid: {syntax_valid}/{len(successful)}")
        
        logger.info("\nGenerated Files:")
        logger.info(f"  Parsed JSON: output/parsed_json/")
        logger.info(f"  PySpark Code: output/pyspark_code/")
        logger.info(f"  Mapping Details: output/mapping_details/")
        logger.info("="*60)


def main():
    """Main entry point for the application."""
    # Banner
    print("\n" + "="*60)
    print("SSIS to PySpark Converter")
    print("Production-Ready Conversion Tool")
    print("="*60 + "\n")
    
    # Check and setup API keys
    try:
        from config import ensure_api_keys, detect_available_provider
        settings = ensure_api_keys()
        
        # Show current configuration
        provider = detect_available_provider()
        if provider != "none":
            print(f"✅ Using {provider.upper()} for AI features")
        else:
            print("⚠️  No AI provider configured - using rule-based conversion only")
            
    except Exception as e:
        print(f"⚠️  API key setup failed: {e}")
        print("   Continuing with rule-based conversion only...")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Convert SSIS packages to PySpark code',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Convert a single package
  ssis_to_pyspark_app.exe input/package.dtsx
  
  # Convert all packages in a folder
  ssis_to_pyspark_app.exe input/
  
  # Specify custom output folder
  ssis_to_pyspark_app.exe input/package.dtsx --output custom_output/
        '''
    )
    
    parser.add_argument(
        'input',
        help='Path to .dtsx file or folder containing .dtsx files'
    )
    
    parser.add_argument(
        '--output',
        default='output',
        help='Output directory for generated files (default: output/)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--no-validation',
        action='store_true',
        help='Skip syntax validation'
    )
    
    parser.add_argument(
        '--schema-mapping',
        type=str,
        default=None,
        help='Path to schema mapping JSON file (maps SSIS connections to Databricks schemas)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize application
        app = SSISToPySparkApp(
            databricks_mode=True,  # Enable Databricks optimization by default
            schema_mapping_file=args.schema_mapping,
            skip_validation=args.no_validation  # Pass the no-validation flag
        )
        
        # Log schema mapping status
        if app.schema_mapper and app.schema_mapper.is_active():
            logger.info(f"Schema mapping active: {len(app.schema_mapper.connection_mappings)} connection(s) mapped")
        elif args.schema_mapping:
            logger.warning(f"Schema mapping file provided but not loaded: {args.schema_mapping}")
        
        # Override output directory if specified
        if args.output != 'output':
            app.output_base = Path(args.output)
            app._ensure_output_directories()
        
        # Determine if input is file or folder
        input_path = Path(args.input)
        
        if input_path.is_file():
            # Single file conversion
            logger.info(f"Mode: Single file conversion")
            result = app.convert_single_package(str(input_path))
            results = [result]
        
        elif input_path.is_dir():
            # Folder conversion
            logger.info(f"Mode: Batch folder conversion")
            results = app.convert_folder(str(input_path))
        
        else:
            logger.error(f"[ERROR] Invalid input: {args.input}")
            logger.error("  Input must be a .dtsx file or folder containing .dtsx files")
            return 1
        
        # Print summary
        if results:
            app.print_summary(results)
            
            # Generate unified analysis report (post LLM validation)
            analysis_file = app.output_base / 'analysis' / 'conversion_report.json'
            analysis_data = app._generate_unified_analysis(results)
            
            with open(analysis_file, 'w') as f:
                json.dump(analysis_data, f, indent=2)
            
            logger.info(f"\n[SAVED] Analysis report: {analysis_file}")
        
        # Exit code
        failed_count = sum(1 for r in results if not r.get('success', False))
        if failed_count > 0:
            logger.warning(f"\n[WARNING] {failed_count} package(s) failed to convert")
            return 1
        else:
            logger.info("\n[SUCCESS] All conversions completed successfully!")
            return 0
        
    except KeyboardInterrupt:
        logger.info("\n\n[INTERRUPTED] Conversion interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"\n[ERROR] Application error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
