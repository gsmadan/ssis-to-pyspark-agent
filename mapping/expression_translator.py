"""
SSIS Expression Translator - Converts SSIS expressions to PySpark expressions.
"""
import re
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class SSISExpressionTranslator:
    """Translates SSIS expressions to PySpark expressions."""
    
    def __init__(self):
        # Initialize translation rules
        self._initialize_function_mappings()
        self._initialize_operator_mappings()
    
    def _initialize_function_mappings(self):
        """Initialize SSIS function to PySpark function mappings."""
        self.function_mappings = {
            # Date/Time functions
            'GETDATE': 'current_timestamp',
            'GETUTCDATE': 'current_timestamp',  # Note: SSIS GETUTCDATE() needs timezone handling
            'DATEADD': 'date_add',  # Simplified, needs parameter translation
            'DATEDIFF': 'datediff',
            'YEAR': 'year',
            'MONTH': 'month',
            'DAY': 'dayofmonth',
            
            # String functions
            'LEN': 'length',
            'SUBSTRING': 'substring',
            'UPPER': 'upper',
            'LOWER': 'lower',
            'TRIM': 'trim',
            'LTRIM': 'ltrim',
            'RTRIM': 'rtrim',
            'REPLACE': 'regexp_replace',
            'FINDSTRING': 'locate',  # SSIS FINDSTRING needs parameter order swap
            
            # Conversion functions
            'CAST': 'cast',
            'CONVERT': 'cast',
            
            # Null handling
            'ISNULL': 'coalesce',
            'REPLACENULL': 'coalesce',
            
            # Mathematical functions
            'ABS': 'abs',
            'CEILING': 'ceil',
            'FLOOR': 'floor',
            'ROUND': 'round',
            'SQRT': 'sqrt',
            'POWER': 'pow',
            
            # Conditional
            'IIF': 'when',  # SSIS IIF(condition, true_val, false_val) -> when(condition, true_val).otherwise(false_val)
        }
    
    def _initialize_operator_mappings(self):
        """Initialize SSIS operator to PySpark operator mappings."""
        self.operator_mappings = {
            '&&': '&',  # PySpark uses & for AND
            '||': '|',  # PySpark uses | for OR
            # Note: Keep != as is, don't translate '!' to '~' when part of '!='
        }
    
    def translate_expression(self, ssis_expr: str, context: Dict[str, Any] = None) -> str:
        """
        Translate SSIS expression to PySpark expression.
        
        Args:
            ssis_expr: SSIS expression string
            context: Additional context (column names, variables, etc.)
            
        Returns:
            PySpark expression string
        """
        if not ssis_expr or ssis_expr.strip() == '':
            return 'lit(None)'
        
        try:
            # Clean the expression
            expr = ssis_expr.strip()
            
            # Translate functions FIRST (before column references)
            expr = self._translate_functions(expr)
            
            # Handle column references with #{...}
            expr = self._translate_column_references(expr)
            
            # Translate literals
            expr = self._translate_literals(expr)
            
            # Handle ternary operator (? :)
            expr = self._translate_ternary(expr)
            
            # Translate operators LAST
            expr = self._translate_operators(expr)
            
            return expr
            
        except Exception as e:
            logger.warning(f"Failed to translate expression '{ssis_expr}': {e}")
            return f'lit("{ssis_expr}")  # TODO: Manual translation needed'
    
    def _translate_column_references(self, expr: str) -> str:
        """Translate SSIS column references to PySpark col() references."""
        # Pattern: #{Package\Task\Component.Outputs[Output].Columns[ColumnName]}
        # Or simpler: [ColumnName]
        
        # Replace #{...Columns[ColumnName]} with col("ColumnName")
        pattern = r'#\{[^\}]*\.Columns\[([^\]]+)\]\}'
        expr = re.sub(pattern, r'col("\1")', expr)
        
        # Replace [Component].ColumnName with col("ColumnName")
        pattern = r'\[([^\]]+)\]\.([A-Za-z_]\w*)'
        expr = re.sub(pattern, r'col("\2")', expr)
        
        # Replace simple [ColumnName] with col("ColumnName")
        pattern = r'\[([A-Za-z_]\w*)\]'
        expr = re.sub(pattern, r'col("\1")', expr)
        
        return expr
    
    def _translate_functions(self, expr: str) -> str:
        """Translate SSIS functions to PySpark functions."""
        # GETDATE() -> current_timestamp()
        for ssis_func, pyspark_func in self.function_mappings.items():
            # Match function with parentheses (case-insensitive)
            pattern = rf'\[?{ssis_func}\]?\s*\(\s*\)'
            if re.search(pattern, expr, re.IGNORECASE):
                expr = re.sub(pattern, f'{pyspark_func}()', expr, flags=re.IGNORECASE)
        
        return expr
    
    def _translate_operators(self, expr: str) -> str:
        """Translate SSIS operators to PySpark operators."""
        for ssis_op, pyspark_op in self.operator_mappings.items():
            expr = expr.replace(ssis_op, pyspark_op)
        
        return expr
    
    def _translate_literals(self, expr: str) -> str:
        """Translate SSIS literals to PySpark literals."""
        # NULL -> lit(None)
        expr = re.sub(r'\bNULL\b', 'lit(None)', expr, flags=re.IGNORECASE)
        
        # TRUE/FALSE -> lit(True)/lit(False)
        expr = re.sub(r'\bTRUE\b', 'lit(True)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bFALSE\b', 'lit(False)', expr, flags=re.IGNORECASE)
        
        return expr
    
    def _translate_ternary(self, expr: str) -> str:
        """Translate SSIS ternary operator (? :) to PySpark when().otherwise()."""
        # Pattern: condition ? true_value : false_value
        # Convert to: when(condition, true_value).otherwise(false_value)
        
        ternary_pattern = r'(.+?)\s*\?\s*(.+?)\s*:\s*(.+)'
        match = re.match(ternary_pattern, expr)
        
        if match:
            condition = match.group(1).strip()
            true_val = match.group(2).strip()
            false_val = match.group(3).strip()
            
            return f'when({condition}, {true_val}).otherwise({false_val})'
        
        return expr
    
    def translate_derived_column(self, logic: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Translate derived column logic to PySpark withColumn operations.
        
        Args:
            logic: Derived column logic dictionary
            
        Returns:
            List of column additions {name, expression}
        """
        columns = []
        
        expressions = logic.get('expressions', '')
        if not expressions:
            return columns
        
        # Parse expressions (could be comma-separated or newline-separated)
        expr_list = self._parse_expression_list(expressions)
        
        for expr in expr_list:
            # Try to extract column name and expression
            # Pattern: ColumnName = Expression
            match = re.match(r'([A-Za-z_]\w*)\s*=\s*(.+)', expr.strip())
            if match:
                col_name = match.group(1)
                col_expr = match.group(2)
                pyspark_expr = self.translate_expression(col_expr)
                columns.append({
                    'name': col_name,
                    'expression': pyspark_expr
                })
            else:
                # No column name, just expression - generate name
                pyspark_expr = self.translate_expression(expr)
                columns.append({
                    'name': 'derived_column',
                    'expression': pyspark_expr
                })
        
        return columns
    
    def translate_conditional_split(self, logic: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Translate conditional split logic to PySpark filter operations.
        
        Args:
            logic: Conditional split logic dictionary
            
        Returns:
            List of conditions {name, filter_expression}
        """
        conditions = []
        
        condition_str = logic.get('conditions', '')
        if not condition_str:
            return conditions
        
        # Parse multiple conditions (SSIS can have multiple outputs)
        # Format can be: "OUTPUT1: condition1 | OUTPUT2: condition2" or just conditions
        condition_list = []
        output_names = []
        
        # Check if pipe-delimited format with output names
        if '|' in condition_str:
            parts = [e.strip() for e in condition_str.split('|') if e.strip()]
            for part in parts:
                if ':' in part:
                    output_name, condition = part.split(':', 1)
                    output_names.append(output_name.strip())
                    condition_list.append(condition.strip())
                else:
                    output_names.append(f'condition_{len(output_names) + 1}')
                    condition_list.append(part.strip())
        else:
            # No pipe-delimited format, parse as before
            condition_list = self._parse_expression_list(condition_str)
            output_names = [f'condition_{i+1}' for i in range(len(condition_list))]
        
        for i, cond in enumerate(condition_list):
            pyspark_cond = self.translate_expression(cond)
            output_name = output_names[i] if i < len(output_names) else f'condition_{i+1}'
            conditions.append({
                'name': output_name,
                'filter_expression': pyspark_cond
            })
        
        return conditions
    
    def translate_lookup_query(self, logic: Dict[str, Any]) -> Dict[str, str]:
        """
        Translate lookup logic to PySpark join operation.
        
        Args:
            logic: Lookup logic dictionary
            
        Returns:
            Dictionary with join details
        """
        lookup_query = logic.get('lookup_query', '')
        no_match_behavior = logic.get('no_match_behavior', '1')  # '1' = Ignore (Left join), '0' = Error (Inner join)
        
        # Determine join type based on no match behavior
        # SSIS: 0 = Fail (inner), 1 = Ignore/Reroute (left)
        join_type = 'left' if str(no_match_behavior) == '1' else 'inner'
        
        return {
            'lookup_query': lookup_query,
            'join_type': join_type,
            'note': 'Join condition needs to be specified based on lookup column mappings'
        }
    
    def _parse_expression_list(self, expr_str: str) -> List[str]:
        """Parse a string containing multiple expressions."""
        # Try cobalt-delimited format first (e.g., "OUTPUT1: condition1 | OUTPUT2: condition2")
        if '|' in expr_str:
            parts = [e.strip() for e in expr_str.split('|') if e.strip()]
            # Extract just the condition part after the colon
            expressions = []
            for part in parts:
                if ':' in part:
                    condition = part.split(':', 1)[1].strip()
                    expressions.append(condition)
                else:
                    expressions.append(part)
            if expressions:
                return expressions
        
        # Try newline-separated
        if '\n' in expr_str:
            return [e.strip() for e in expr_str.split('\n') if e.strip()]
        
        # Try comma remote-separated
        if ',' in expr_str:
            return [e.strip() for e in expr_str.split(',') if e.strip()]
        
        # Single expression
        return [expr_str.strip()] if expr_str.strip() else []
    
    def generate_withcolumn_code(self, df_name: str, columns: List[Dict[str, str]]) -> str:
        """Generate chained withColumn code."""
        if not columns:
            return f'{df_name}  # No columns to add'
        
        code_lines = []
        for i, col_info in enumerate(columns):
            col_name = col_info['name']
            col_expr = col_info['expression']
            
            if i == 0:
                code_lines.append(f'{df_name}.withColumn("{col_name}", {col_expr})')
            else:
                code_lines.append(f'    .withColumn("{col_name}", {col_expr})')
        
        return ' \\\n'.join(code_lines)
    
    def generate_filter_code(self, df_name: str, conditions: List[Dict[str, str]]) -> Dict[str, str]:
        """Generate filter code for conditional split."""
        output_dfs = {}
        
        for cond_info in conditions:
            cond_name = cond_info['name']
            filter_expr = cond_info['filter_expression']
            output_df_name = f'{df_name}_{cond_name}'
            
            output_dfs[cond_name] = {
                'df_name': output_df_name,
                'code': f'{output_df_name} = {df_name}.filter({filter_expr})'
            }
        
        return output_dfs


# Singleton instance
_translator = None

def get_translator() -> SSISExpressionTranslator:
    """Get the singleton expression translator instance."""
    global _translator
    if _translator is None:
        _translator = SSISExpressionTranslator()
    return _translator

