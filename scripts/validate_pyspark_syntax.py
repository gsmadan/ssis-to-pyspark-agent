"""
Validate PySpark generated code for Python syntax correctness.
"""
import ast
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PySparkSyntaxValidator:
    """Validates generated PySpark code for syntax correctness."""
    
    def __init__(self):
        self.validation_results = []
    
    def validate_file(self, file_path: str) -> dict:
        """
        Validate a single PySpark file for syntax correctness.
        
        Args:
            file_path: Path to the PySpark file
            
        Returns:
            Dictionary with validation results
        """
        result = {
            'file': os.path.basename(file_path),
            'syntax_valid': False,
            'errors': [],
            'warnings': [],
            'line_count': 0,
            'import_count': 0,
            'function_count': 0
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            result['line_count'] = len(code.split('\n'))
            
            # Parse the Python code using AST
            try:
                tree = ast.parse(code, filename=file_path)
                result['syntax_valid'] = True
                
                # Analyze the AST
                self._analyze_ast(tree, result)
                
                logger.info(f"✅ {result['file']}: Syntax valid")
                
            except SyntaxError as e:
                result['syntax_valid'] = False
                result['errors'].append({
                    'type': 'SyntaxError',
                    'message': str(e.msg),
                    'line': e.lineno,
                    'offset': e.offset,
                    'text': e.text
                })
                logger.error(f"❌ {result['file']}: Syntax Error at line {e.lineno}: {e.msg}")
            
            except Exception as e:
                result['syntax_valid'] = False
                result['errors'].append({
                    'type': type(e).__name__,
                    'message': str(e)
                })
                logger.error(f"❌ {result['file']}: {type(e).__name__}: {e}")
            
            # Additional validation checks
            self._check_pyspark_specific_issues(code, result)
            
        except Exception as e:
            result['errors'].append({
                'type': 'FileError',
                'message': f"Failed to read file: {e}"
            })
            logger.error(f"❌ {result['file']}: Failed to read - {e}")
        
        return result
    
    def _analyze_ast(self, tree: ast.AST, result: dict):
        """Analyze the AST to extract code statistics."""
        for node in ast.walk(tree):
            # Count imports
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                result['import_count'] += 1
            
            # Count function definitions
            if isinstance(node, ast.FunctionDef):
                result['function_count'] += 1
    
    def _check_pyspark_specific_issues(self, code: str, result: dict):
        """Check for common PySpark-specific issues."""
        lines = code.split('\n')
        
        for i, line in enumerate(lines, 1):
            # Check for common issues
            
            # TODO markers (warnings, not errors)
            if 'TODO' in line:
                result['warnings'].append({
                    'type': 'TODO',
                    'message': 'Manual implementation needed',
                    'line': i,
                    'text': line.strip()
                })
            
            # Check for placeholder variables
            if 'connection_url' in line and '=' not in line:
                result['warnings'].append({
                    'type': 'PlaceholderVariable',
                    'message': 'Using placeholder variable - needs actual value',
                    'line': i,
                    'text': line.strip()
                })
            
            # Check for commented-out code in write operations
            if line.strip().startswith('#') and '.write' in line:
                result['warnings'].append({
                    'type': 'CommentedCode',
                    'message': 'Write operation is commented out',
                    'line': i,
                    'text': line.strip()
                })
    
    def validate_directory(self, directory: str) -> list:
        """
        Validate all PySpark files in a directory.
        
        Args:
            directory: Path to directory containing PySpark files
            
        Returns:
            List of validation results
        """
        results = []
        
        if not os.path.exists(directory):
            logger.error(f"Directory not found: {directory}")
            return results
        
        py_files = [f for f in os.listdir(directory) if f.endswith('.py')]
        
        logger.info(f"\nValidating {len(py_files)} PySpark files in {directory}\n")
        
        for py_file in py_files:
            file_path = os.path.join(directory, py_file)
            result = self.validate_file(file_path)
            results.append(result)
        
        self.validation_results = results
        return results
    
    def print_summary(self):
        """Print validation summary."""
        if not self.validation_results:
            logger.info("No files validated.")
            return
        
        total = len(self.validation_results)
        valid = sum(1 for r in self.validation_results if r['syntax_valid'])
        invalid = total - valid
        
        total_warnings = sum(len(r['warnings']) for r in self.validation_results)
        total_errors = sum(len(r['errors']) for r in self.validation_results)
        
        logger.info("\n" + "="*80)
        logger.info("SYNTAX VALIDATION SUMMARY")
        logger.info("="*80)
        logger.info(f"Total files: {total}")
        logger.info(f"✅ Valid syntax: {valid}")
        logger.info(f"❌ Invalid syntax: {invalid}")
        logger.info(f"⚠️  Total warnings: {total_warnings}")
        logger.info(f"🔴 Total errors: {total_errors}")
        logger.info("="*80 + "\n")
        
        # Print detailed results
        for result in self.validation_results:
            if not result['syntax_valid'] or result['errors']:
                logger.info(f"\n{result['file']}:")
                for error in result['errors']:
                    logger.info(f"  ❌ {error['type']}: {error['message']}")
                    if 'line' in error:
                        logger.info(f"     Line {error['line']}: {error.get('text', '')}")
        
        # Print warning summary
        if total_warnings > 0:
            logger.info("\n⚠️  Warning Summary:")
            warning_types = {}
            for result in self.validation_results:
                for warning in result['warnings']:
                    w_type = warning['type']
                    warning_types[w_type] = warning_types.get(w_type, 0) + 1
            
            for w_type, count in warning_types.items():
                logger.info(f"  - {w_type}: {count} occurrences")
        
        # Calculate success rate
        success_rate = (valid / total * 100) if total > 0 else 0
        logger.info(f"\n✨ Syntax Validation Success Rate: {success_rate:.1f}%\n")
        
        return {
            'total_files': total,
            'valid_files': valid,
            'invalid_files': invalid,
            'total_warnings': total_warnings,
            'total_errors': total_errors,
            'success_rate': success_rate
        }


def main():
    """Main function."""
    validator = PySparkSyntaxValidator()
    
    # Validate all PySpark files in output/pyspark_code/
    results = validator.validate_directory('output/pyspark_code')
    
    # Print summary
    summary = validator.print_summary()
    
    # Save detailed results to JSON
    import json
    output_file = 'output/analysis/syntax_validation_results.json'
    with open(output_file, 'w') as f:
        json.dump({
            'summary': summary,
            'details': results
        }, f, indent=2)
    
    logger.info(f"Detailed results saved to: {output_file}")
    
    return summary


if __name__ == '__main__':
    main()


