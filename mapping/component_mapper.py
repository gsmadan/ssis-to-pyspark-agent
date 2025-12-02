"""
Component mapping utilities for SSIS to PySpark conversion.
Hybrid approach: Rule-based for common components, LLM fallback for unmapped ones.
"""
from typing import Dict, List, Any, Optional
import logging
from models import SSISComponent, PySparkMapping, SSISTaskType, DataSourceType, TransformationType

logger = logging.getLogger(__name__)


class ComponentMapper:
    """Maps individual SSIS components to PySpark equivalents using rules + LLM fallback."""
    
    def __init__(self, use_llm_fallback: bool = True):
        self.use_llm_fallback = use_llm_fallback
        self.llm_generator = None
        
        # Initialize mapping rules
        self.task_mappings = self._initialize_task_mappings()
        self.data_source_mappings = self._initialize_data_source_mappings()
        self.transformation_mappings = self._initialize_transformation_mappings()
        
        # Initialize LLM generator if fallback is enabled
        if self.use_llm_fallback:
            try:
                from code_generation.llm_code_generator import LLMCodeGenerator
                self.llm_generator = LLMCodeGenerator()
                logger.info("LLM fallback enabled for unmapped components")
            except Exception as e:
                logger.warning(f"Failed to initialize LLM generator: {e}. Will use generic fallback.")
                self.use_llm_fallback = False
    
    def map_component(self, component: SSISComponent) -> PySparkMapping:
        """
        Map an SSIS component to PySpark equivalent.
        Uses rule-based mapping first, LLM fallback for unmapped components.
        
        Args:
            component: SSIS component to map
            
        Returns:
            PySpark mapping object
        """
        try:
            # Try rule-based mapping first (no API call!)
            if isinstance(component.type, SSISTaskType):
                return self._map_task(component)
            elif isinstance(component.type, DataSourceType):
                return self._map_data_source(component)
            elif isinstance(component.type, TransformationType):
                return self._map_transformation(component)
            else:
                # Unknown component type - use LLM fallback
                logger.info(f"Unknown component type: {component.type} - using LLM fallback")
                return self._create_llm_fallback_mapping(component)
                
        except Exception as e:
            logger.error(f"Error mapping component {component.id}: {e}")
            return self._create_llm_fallback_mapping(component)
    
    def _map_task(self, component: SSISComponent) -> PySparkMapping:
        """Map SSIS task to PySpark equivalent using rules."""
        task_type = component.type
        
        # Rule-based mappings for common tasks
        if task_type == SSISTaskType.DATA_FLOW_TASK:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe_operations",
                parameters={"description": "Data flow operations mapped to DataFrame transformations"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession", "from pyspark.sql.functions import *"]
            )
        
        elif task_type == SSISTaskType.EXECUTE_SQL_TASK:
            sql_statement = component.properties.get('SqlStatementSource', '')
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="spark.sql",
                parameters={"sql_statement": sql_statement, "description": "Execute SQL using Spark SQL"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        elif task_type == SSISTaskType.SCRIPT_TASK:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="custom_function",
                parameters={"description": "Script task converted to PySpark UDF"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql.functions import udf"]
            )
        
        elif task_type == SSISTaskType.FILE_SYSTEM_TASK:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="file_operations",
                parameters={"description": "File system operations"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        else:
            # Unmapped task - use LLM fallback
            logger.info(f"Unmapped task type: {task_type} - using LLM fallback")
            return self._create_llm_fallback_mapping(component)
    
    def _map_data_source(self, component: SSISComponent) -> PySparkMapping:
        """Map data source to PySpark equivalent using rules."""
        source_type = component.type
        
        # Rule-based mappings for common data sources
        if source_type == DataSourceType.SQL_SERVER:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="spark.read.format('jdbc')",
                parameters={"description": "Read from SQL Server using JDBC"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        elif source_type == DataSourceType.CSV:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="spark.read.csv",
                parameters={"description": "Read CSV file"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        elif source_type == DataSourceType.JSON:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="spark.read.json",
                parameters={"description": "Read JSON file"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        elif source_type == DataSourceType.EXCEL:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="spark.read.format('com.crealytics.spark.excel')",
                parameters={"description": "Read Excel file"},
                dependencies=["pyspark.sql", "spark-excel"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        else:
            # Unmapped source - use LLM fallback
            logger.info(f"Unmapped data source: {source_type} - using LLM fallback")
            return self._create_llm_fallback_mapping(component)
    
    def _map_transformation(self, component: SSISComponent) -> PySparkMapping:
        """Map transformation to PySpark equivalent using rules."""
        transformation_type = component.type
        
        # Rule-based mappings for common transformations
        if transformation_type == TransformationType.LOOKUP:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe.join",
                parameters={"description": "Lookup using DataFrame join"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql.functions import *"]
            )
        
        elif transformation_type == TransformationType.DERIVED_COLUMN:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe.withColumn",
                parameters={"description": "Derived column using withColumn"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql.functions import *"]
            )
        
        elif transformation_type == TransformationType.CONDITIONAL_SPLIT:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe.filter",
                parameters={"description": "Conditional split using filter"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql.functions import *"]
            )
        
        elif transformation_type == TransformationType.AGGREGATE:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe.groupBy",
                parameters={"description": "Aggregate using groupBy and agg"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql.functions import *"]
            )
        
        elif transformation_type == TransformationType.SORT:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe.orderBy",
                parameters={"description": "Sort using orderBy"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql.functions import *"]
            )
        
        elif transformation_type == TransformationType.UNION_ALL:
            return PySparkMapping(
                ssis_component=component,
                pyspark_function="dataframe.union",
                parameters={"description": "Union all using union"},
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
        
        else:
            # Unmapped transformation - use LLM fallback
            logger.info(f"Unmapped transformation: {transformation_type} - using LLM fallback")
            return self._create_llm_fallback_mapping(component)
    
    def _create_llm_fallback_mapping(self, component: SSISComponent) -> PySparkMapping:
        """Create LLM-powered fallback mapping for unmapped components only."""
        if not self.use_llm_fallback or not self.llm_generator:
            return self._create_generic_fallback_mapping(component)
            
        try:
            logger.info(f"⚡ Using LLM fallback for unmapped component: {component.id} ({component.type})")
            
            # Create a temporary mapping to pass to LLM
            temp_mapping = PySparkMapping(
                ssis_component=component,
                pyspark_function="llm_generated_function",
                parameters={
                    "description": f"LLM-generated mapping for {component.type}",
                    "component_properties": component.properties,
                    "component_expressions": component.expressions
                },
                dependencies=["pyspark.sql"],
                imports=["from pyspark.sql import SparkSession"]
            )
            
            # Generate PySpark code using LLM (only for this unmapped component)
            generated_code = self.llm_generator.generate_code_for_component(temp_mapping)
            
            # Extract PySpark function name from generated code
            pyspark_function = self._extract_function_name_from_code(generated_code)
            
            # Extract imports and dependencies from generated code
            imports = self._extract_imports_from_code(generated_code)
            dependencies = self._extract_dependencies_from_code(generated_code)
            
            return PySparkMapping(
                ssis_component=component,
                pyspark_function=pyspark_function,
                parameters={
                    "description": f"LLM-generated mapping for {component.type}",
                    "generated_code": generated_code,
                    "component_properties": component.properties,
                    "component_expressions": component.expressions,
                    "llm_generated": True,
                    "confidence": "high"
                },
                dependencies=dependencies,
                imports=imports
            )
            
        except Exception as e:
            logger.error(f"LLM fallback failed for component {component.id}: {e}")
            return self._create_generic_fallback_mapping(component)
    
    def _create_generic_fallback_mapping(self, component: SSISComponent) -> PySparkMapping:
        """Create generic fallback mapping when LLM is not available."""
        return PySparkMapping(
            ssis_component=component,
            pyspark_function="generic_operation",
            parameters={
                "description": f"Generic operation for {component.type}",
                "warning": "This component used generic fallback - manual review recommended"
            },
            dependencies=["pyspark.sql"],
            imports=["from pyspark.sql import SparkSession"]
        )
    
    def _extract_function_name_from_code(self, code: str) -> str:
        """Extract function name from generated PySpark code."""
        lines = code.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('def '):
                func_name = line.split('(')[0].replace('def ', '').strip()
                return func_name
        return "llm_generated_function"
    
    def _extract_imports_from_code(self, code: str) -> List[str]:
        """Extract import statements from generated PySpark code."""
        imports = []
        lines = code.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('import ') or line.startswith('from '):
                imports.append(line)
        
        if not imports:
            imports = [
                "from pyspark.sql import SparkSession",
                "from pyspark.sql.functions import *",
                "from pyspark.sql.types import *"
            ]
        
        return imports
    
    def _extract_dependencies_from_code(self, code: str) -> List[str]:
        """Extract dependencies from generated PySpark code."""
        dependencies = set()
        dependencies.add("pyspark")
        
        code_lower = code.lower()
        if 'pandas' in code_lower:
            dependencies.add('pandas')
        if 'numpy' in code_lower:
            dependencies.add('numpy')
        if 'requests' in code_lower:
            dependencies.add('requests')
        if 'openpyxl' in code_lower:
            dependencies.add('openpyxl')
        if 'xmltodict' in code_lower:
            dependencies.add('xmltodict')
        
        return list(dependencies)
    
    def _initialize_task_mappings(self) -> Dict[str, str]:
        """Initialize task type mappings."""
        return {
            'DataFlowTask': 'dataframe_operations',
            'ExecuteSQLTask': 'spark.sql',
            'ScriptTask': 'custom_function',
            'FileSystemTask': 'file_operations',
            'FTPTask': 'ftp_operations',
            'WebServiceTask': 'web_service_call',
            'ExpressionTask': 'expression_evaluation',
            'ForLoopContainer': 'iteration',
            'ForeachLoopContainer': 'iteration',
            'SequenceContainer': 'sequential_operations'
        }
    
    def _initialize_data_source_mappings(self) -> Dict[str, str]:
        """Initialize data source mappings."""
        return {
            'SqlServer': 'spark.read.format("jdbc")',
            'CSV': 'spark.read.csv',
            'Excel': 'spark.read.format("com.crealytics.spark.excel")',
            'JSON': 'spark.read.json',
            'XML': 'xml_reader',
            'FlatFile': 'spark.read.text',
            'Oracle': 'spark.read.format("jdbc")',
            'MySQL': 'spark.read.format("jdbc")',
            'PostgreSQL': 'spark.read.format("jdbc")'
        }
    
    def _initialize_transformation_mappings(self) -> Dict[str, str]:
        """Initialize transformation mappings."""
        return {
            'Lookup': 'dataframe.join',
            'MergeJoin': 'dataframe.join',
            'ConditionalSplit': 'dataframe.filter',
            'DerivedColumn': 'dataframe.withColumn',
            'DataConversion': 'dataframe.cast',
            'Sort': 'dataframe.orderBy',
            'Aggregate': 'dataframe.groupBy',
            'UnionAll': 'dataframe.union',
            'Multicast': 'dataframe_multicast',
            'RowCount': 'dataframe.count'
        }