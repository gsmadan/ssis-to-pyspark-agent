"""
Data flow mapping utilities for SSIS to PySpark conversion.
"""
from typing import Dict, List, Any, Optional
import logging
from models import DataFlow, SSISComponent, PySparkMapping
from .component_mapper import ComponentMapper

logger = logging.getLogger(__name__)


class DataFlowMapper:
    """Maps SSIS data flows to PySpark DataFrame operations."""
    
    def __init__(self, use_llm_fallback: bool = True):
        self.component_mapper = ComponentMapper(use_llm_fallback=use_llm_fallback)
    
    def map_data_flow(self, data_flow: DataFlow) -> Dict[str, Any]:
        """
        Map an SSIS data flow to PySpark DataFrame operations.
        
        Args:
            data_flow: SSIS data flow to map
            
        Returns:
            Dictionary containing PySpark mapping for the data flow
        """
        try:
            logger.info(f"Mapping data flow: {data_flow.name}")
            
            # Map sources
            source_mappings = self._map_sources(data_flow.sources)
            
            # Map transformations
            transformation_mappings = self._map_transformations(data_flow.transformations)
            
            # Map destinations
            destination_mappings = self._map_destinations(data_flow.destinations)
            
            # Map paths (data flow connections)
            path_mappings = self._map_paths(data_flow.paths)
            
            # Generate PySpark code structure
            pyspark_structure = self._generate_pyspark_structure(
                source_mappings,
                transformation_mappings,
                destination_mappings,
                path_mappings
            )
            
            return {
                'data_flow_id': data_flow.id,
                'data_flow_name': data_flow.name,
                'sources': source_mappings,
                'transformations': transformation_mappings,
                'destinations': destination_mappings,
                'paths': path_mappings,
                'pyspark_structure': pyspark_structure,
                'imports': self._collect_imports(source_mappings, transformation_mappings, destination_mappings),
                'dependencies': self._collect_dependencies(source_mappings, transformation_mappings, destination_mappings)
            }
            
        except Exception as e:
            logger.error(f"Error mapping data flow {data_flow.name}: {e}")
            raise
    
    def _map_sources(self, sources: List[SSISComponent]) -> List[PySparkMapping]:
        """Map data flow sources to PySpark readers."""
        source_mappings = []
        
        for source in sources:
            try:
                mapping = self.component_mapper.map_component(source)
                source_mappings.append(mapping)
                logger.debug(f"Mapped source: {source.id}")
            except Exception as e:
                logger.error(f"Error mapping source {source.id}: {e}")
                # Add fallback mapping
                source_mappings.append(self._create_fallback_source_mapping(source))
        
        return source_mappings
    
    def _map_transformations(self, transformations: List[SSISComponent]) -> List[PySparkMapping]:
        """Map data flow transformations to PySpark DataFrame operations."""
        transformation_mappings = []
        
        for transformation in transformations:
            try:
                mapping = self.component_mapper.map_component(transformation)
                transformation_mappings.append(mapping)
                logger.debug(f"Mapped transformation: {transformation.id}")
            except Exception as e:
                logger.error(f"Error mapping transformation {transformation.id}: {e}")
                # Add fallback mapping
                transformation_mappings.append(self._create_fallback_transformation_mapping(transformation))
        
        return transformation_mappings
    
    def _map_destinations(self, destinations: List[SSISComponent]) -> List[PySparkMapping]:
        """Map data flow destinations to PySpark writers."""
        destination_mappings = []
        
        for destination in destinations:
            try:
                mapping = self.component_mapper.map_component(destination)
                destination_mappings.append(mapping)
                logger.debug(f"Mapped destination: {destination.id}")
            except Exception as e:
                logger.error(f"Error mapping destination {destination.id}: {e}")
                # Add fallback mapping
                destination_mappings.append(self._create_fallback_destination_mapping(destination))
        
        return destination_mappings
    
    def _map_paths(self, paths: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Map data flow paths to PySpark DataFrame operations."""
        path_mappings = []
        
        for path in paths:
            try:
                path_mapping = {
                    'source_id': path.get('source', ''),
                    'destination_id': path.get('destination', ''),
                    'pyspark_operation': 'dataframe_transformation',
                    'description': f"Data flow from {path.get('source', '')} to {path.get('destination', '')}"
                }
                path_mappings.append(path_mapping)
                logger.debug(f"Mapped path: {path.get('source', '')} -> {path.get('destination', '')}")
            except Exception as e:
                logger.error(f"Error mapping path: {e}")
        
        return path_mappings
    
    def _generate_pyspark_structure(self, 
                                   source_mappings: List[PySparkMapping],
                                   transformation_mappings: List[PySparkMapping],
                                   destination_mappings: List[PySparkMapping],
                                   path_mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate PySpark code structure for the data flow."""
        
        structure = {
            'initialization': self._generate_initialization_code(),
            'data_sources': self._generate_data_source_code(source_mappings),
            'transformations': self._generate_transformation_code(transformation_mappings),
            'data_sinks': self._generate_data_sink_code(destination_mappings),
            'execution_flow': self._generate_execution_flow(path_mappings)
        }
        
        return structure
    
    def _generate_initialization_code(self) -> str:
        """Generate PySpark initialization code."""
        return """
# Initialize Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \\
    .appName("SSIS_DataFlow_Conversion") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")
"""
    
    def _generate_data_source_code(self, source_mappings: List[PySparkMapping]) -> str:
        """Generate PySpark code for data sources."""
        code_lines = ["# Data Sources"]
        
        for i, source in enumerate(source_mappings):
            source_id = source.ssis_component.id
            source_name = source.ssis_component.name
            
            if source.pyspark_function == "spark.read.format('jdbc')":
                code_lines.append(f"""
# Source {i+1}: {source_name} ({source_id})
{source_id.lower()}_df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:sqlserver://server:port;database=db") \\
    .option("dbtable", "table_name") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .load()
""")
            elif source.pyspark_function == "spark.read.csv":
                file_path = source.parameters.get('file_path', 'path/to/file.csv')
                code_lines.append(f"""
# Source {i+1}: {source_name} ({source_id})
{source_id.lower()}_df = spark.read \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .csv("{file_path}")
""")
            elif source.pyspark_function == "spark.read.json":
                file_path = source.parameters.get('file_path', 'path/to/file.json')
                code_lines.append(f"""
# Source {i+1}: {source_name} ({source_id})
{source_id.lower()}_df = spark.read.json("{file_path}")
""")
            else:
                code_lines.append(f"""
# Source {i+1}: {source_name} ({source_id})
# TODO: Implement custom source reader
{source_id.lower()}_df = None  # Placeholder
""")
        
        return "\n".join(code_lines)
    
    def _generate_transformation_code(self, transformation_mappings: List[PySparkMapping]) -> str:
        """Generate PySpark code for transformations."""
        code_lines = ["# Data Transformations"]
        
        for i, transformation in enumerate(transformation_mappings):
            trans_id = transformation.ssis_component.id
            trans_name = transformation.ssis_component.name
            
            if transformation.pyspark_function == "dataframe.join":
                code_lines.append(f"""
# Transformation {i+1}: {trans_name} ({trans_id})
# TODO: Implement join logic
{trans_id.lower()}_df = None  # Placeholder for join operation
""")
            elif transformation.pyspark_function == "dataframe.filter":
                code_lines.append(f"""
# Transformation {i+1}: {trans_name} ({trans_id})
# TODO: Implement filter logic
{trans_id.lower()}_df = None  # Placeholder for filter operation
""")
            elif transformation.pyspark_function == "dataframe.withColumn":
                code_lines.append(f"""
# Transformation {i+1}: {trans_name} ({trans_id})
# TODO: Implement derived column logic
{trans_id.lower()}_df = None  # Placeholder for withColumn operation
""")
            elif transformation.pyspark_function == "dataframe.groupBy":
                code_lines.append(f"""
# Transformation {i+1}: {trans_name} ({trans_id})
# TODO: Implement aggregation logic
{trans_id.lower()}_df = None  # Placeholder for groupBy operation
""")
            else:
                code_lines.append(f"""
# Transformation {i+1}: {trans_name} ({trans_id})
# TODO: Implement transformation logic
{trans_id.lower()}_df = None  # Placeholder
""")
        
        return "\n".join(code_lines)
    
    def _generate_data_sink_code(self, destination_mappings: List[PySparkMapping]) -> str:
        """Generate PySpark code for data destinations."""
        code_lines = ["# Data Destinations"]
        
        for i, destination in enumerate(destination_mappings):
            dest_id = destination.ssis_component.id
            dest_name = destination.ssis_component.name
            
            code_lines.append(f"""
# Destination {i+1}: {dest_name} ({dest_id})
# TODO: Implement destination writer
# Example: df.write.mode("overwrite").parquet("output_path")
""")
        
        return "\n".join(code_lines)
    
    def _generate_execution_flow(self, path_mappings: List[Dict[str, Any]]) -> str:
        """Generate PySpark execution flow code."""
        code_lines = ["# Execution Flow"]
        
        if path_mappings:
            code_lines.append("# Data flow execution order:")
            for i, path in enumerate(path_mappings):
                source_id = path.get('source_id', '')
                dest_id = path.get('destination_id', '')
                code_lines.append(f"# {i+1}. {source_id} -> {dest_id}")
        
        code_lines.append("""
# Execute the data flow
# TODO: Implement actual execution logic based on the mapped components
""")
        
        return "\n".join(code_lines)
    
    def _collect_imports(self, 
                         source_mappings: List[PySparkMapping],
                         transformation_mappings: List[PySparkMapping],
                         destination_mappings: List[PySparkMapping]) -> List[str]:
        """Collect all required imports."""
        imports = set()
        
        # Add base imports
        imports.add("from pyspark.sql import SparkSession")
        imports.add("from pyspark.sql.functions import *")
        imports.add("from pyspark.sql.types import *")
        
        # Collect imports from mappings
        for mapping in source_mappings + transformation_mappings + destination_mappings:
            imports.update(mapping.imports)
        
        return list(imports)
    
    def _collect_dependencies(self, 
                             source_mappings: List[PySparkMapping],
                             transformation_mappings: List[PySparkMapping],
                             destination_mappings: List[PySparkMapping]) -> List[str]:
        """Collect all required dependencies."""
        dependencies = set()
        
        # Add base dependencies
        dependencies.add("pyspark")
        
        # Collect dependencies from mappings
        for mapping in source_mappings + transformation_mappings + destination_mappings:
            dependencies.update(mapping.dependencies)
        
        return list(dependencies)
    
    def _create_fallback_source_mapping(self, source: SSISComponent) -> PySparkMapping:
        """Create fallback mapping for source."""
        return PySparkMapping(
            ssis_component=source,
            pyspark_function="fallback_source",
            parameters={
                "description": f"Fallback source mapping for {source.id}",
                "warning": "This source could not be properly mapped"
            },
            dependencies=["pyspark.sql"],
            imports=["from pyspark.sql import SparkSession"]
        )
    
    def _create_fallback_transformation_mapping(self, transformation: SSISComponent) -> PySparkMapping:
        """Create fallback mapping for transformation."""
        return PySparkMapping(
            ssis_component=transformation,
            pyspark_function="fallback_transformation",
            parameters={
                "description": f"Fallback transformation mapping for {transformation.id}",
                "warning": "This transformation could not be properly mapped"
            },
            dependencies=["pyspark.sql"],
            imports=["from pyspark.sql.functions import *"]
        )
    
    def _create_fallback_destination_mapping(self, destination: SSISComponent) -> PySparkMapping:
        """Create fallback mapping for destination."""
        return PySparkMapping(
            ssis_component=destination,
            pyspark_function="fallback_destination",
            parameters={
                "description": f"Fallback destination mapping for {destination.id}",
                "warning": "This destination could not be properly mapped"
            },
            dependencies=["pyspark.sql"],
            imports=["from pyspark.sql import SparkSession"]
        )
