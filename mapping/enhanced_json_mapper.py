"""
Enhanced JSON-based mapper for converting parsed SSIS JSON to PySpark code.
Works directly with the data_engineering_parser JSON output.
"""
from typing import Dict, Any, List, Optional, Tuple
import logging
import json
import re
from .expression_translator import get_translator

logger = logging.getLogger(__name__)


class EnhancedJSONMapper:
    """
    Maps parsed SSIS JSON (from data_engineering_parser) to PySpark code.
    Extensive rule-based mappings with LLM fallback for unmapped components.
    """
    
    def __init__(self, use_llm_fallback: bool = True, databricks_mode: bool = True, schema_mapper=None):
        self.use_llm_fallback = use_llm_fallback
        self.databricks_mode = databricks_mode  # NEW: Enable Databricks optimization
        self.schema_mapper = schema_mapper  # Schema mapper for SSIS to Databricks schema mapping
        self.llm_generator = None
        
        # Initialize expression translator
        self.expression_translator = get_translator()
        
        # DataFrame flow tracking
        self.df_flow = {}  # Maps component_id to DataFrame variable name
        self.conditional_split_outputs = {}  # Maps component_id -> {output_name: df_name}
        self.df_counter = 0
        
        # Initialize comprehensive mapping rules
        self._initialize_mappings()
        
        # Initialize LLM generator if fallback is enabled
        if self.use_llm_fallback:
            try:
                from code_generation.llm_code_generator import LLMCodeGenerator
                self.llm_generator = LLMCodeGenerator()
                logger.info("LLM fallback enabled for unmapped components")
            except Exception as e:
                logger.warning(f"Failed to initialize LLM generator: {e}. Will use rule-based only.")
                self.use_llm_fallback = False
    
    def _initialize_mappings(self):
        """Initialize all mapping rules."""
        
        # Connection type mappings
        self.connection_mappings = {
            'SQL_DATABASE': {
                'reader': 'spark.read.format("jdbc")',
                'writer': 'df.write.format("jdbc")',
                'options_template': {
                    'url': 'jdbc:sqlserver://{server}:{port};databaseName={database}',
                    'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                    'user': 'username',
                    'password': 'password'
                }
            },
            'EXCEL': {
                'reader': 'spark.read.format("com.crealytics.spark.excel")',
                'writer': 'df.write.format("com.crealytics.spark.excel")',
                'options_template': {
                    'dataAddress': "'Sheet1'!A1",
                    'header': 'true',
                    'inferSchema': 'true'
                }
            },
            'CSV': {
                'reader': 'spark.read.csv',
                'writer': 'df.write.csv',
                'options_template': {
                    'header': 'true',
                    'inferSchema': 'true',
                    'sep': ','
                }
            },
            'JSON': {
                'reader': 'spark.read.json',
                'writer': 'df.write.json',
                'options_template': {}
            },
            'REST_API': {
                'reader': 'custom_api_reader',
                'requires_custom': True
            }
        }
        
        # Source type mappings
        self.source_mappings = {
            'SQL_TABLE': {
                'pyspark_function': 'spark.read.format("jdbc")' if not self.databricks_mode else 'spark.table',
                'requires_connection': not self.databricks_mode,
                'code_template': '''
# Read from SQL Table: {table_name}
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", "{jdbc_url}") \\
    .option("dbtable", "{table_name}") \\
    .option("user", "{username}") \\
    .option("password", "{password}") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .load()
''' if not self.databricks_mode else '''
# Read from Databricks table: {table_name}
{df_name} = spark.table("{table_name}") \\
    .select({columns})
'''
            },
            'SQL_QUERY': {
                'pyspark_function': 'spark.read.format("jdbc")' if not self.databricks_mode else 'spark.sql',
                'requires_connection': not self.databricks_mode,
                'code_template': '''
# Read from SQL Query
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", "{jdbc_url}") \\
    .option("query", """
{sql_query}
""") \\
    .option("user", "{username}") \\
    .option("password", "{password}") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .load()
''' if not self.databricks_mode else '''
# Read from Databricks using SQL
{df_name} = spark.sql("""
    {sql_query}
""")
'''
            },
            'REST_API_JSON': {
                'pyspark_function': 'requests + spark.createDataFrame',
                'requires_custom': True,
                'code_template': '''
# Read from REST API: {api_url}
import requests
import json

response = requests.get("{api_url}")
data = response.json()

# Extract data based on JSON path: {json_path}
if isinstance(data, list):
    {df_name} = spark.createDataFrame(data)
else:
    # Navigate to data location
    data_list = data{json_path_accessor}
    {df_name} = spark.createDataFrame(data_list)
'''
            },
            'EXCEL': {
                'pyspark_function': 'spark.read.format("com.crealytics.spark.excel")',
                'code_template': '''
# Read from Excel: {file_path}
{df_name} = spark.read \\
    .format("com.crealytics.spark.excel") \\
    .option("dataAddress", "{sheet_name}!A1") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("{file_path}")
'''
            },
            'CSV': {
                'pyspark_function': 'spark.read.csv',
                'code_template': '''
# Read from CSV: {file_path}
{df_name} = spark.read \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .option("sep", ",") \\
    .csv("{file_path}")
'''
            },
            'OTHER': {
                'pyspark_function': 'spark.read.format("jdbc")',
                'fallback': True,
                'code_template': '''
# Read from source: {source_name}
# Table: {table_name}
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", "{jdbc_url}") \\
    .option("dbtable", "{table_name}") \\
    .option("user", "{username}") \\
    .option("password", "{password}") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .load()
'''
            }
        }
        
        # Transformation type mappings
        self.transformation_mappings = {
            'DERIVED_COLUMN': {
                'pyspark_function': 'df.withColumn',
                'code_template': '''
# Derived Column: {trans_name}
{output_df} = {input_df}
{column_additions}
'''
            },
            'LOOKUP': {
                'pyspark_function': 'df.join',
                'code_template': '''
# Lookup: {trans_name}
# Join {input_df} with {lookup_df}
{output_df} = {input_df}.join(
    {lookup_df},
    {join_condition},
    "left"  # Left outer join for LOOKUP
)
'''
            },
            'CONDITIONAL_SPLIT': {
                'pyspark_function': 'df.filter',
                'code_template': '''
# Conditional Split: {trans_name}
{output_splits}
'''
            },
            'AGGREGATE': {
                'pyspark_function': 'df.groupBy',
                'code_template': '''
# Aggregate: {trans_name}
{output_df} = {input_df}.groupBy(
    {group_by_cols}
).agg(
    {aggregations}
)
'''
            },
            'SORT': {
                'pyspark_function': 'df.orderBy',
                'code_template': '''
# Sort: {trans_name}
{output_df} = {input_df}.orderBy(
    {sort_cols}
)
'''
            },
            'UNION_ALL': {
                'pyspark_function': 'df.union',
                'code_template': '''
# Union All: {trans_name}
{output_df} = {input_df1}.union({input_df2})
'''
            },
            'MERGE_JOIN': {
                'pyspark_function': 'df.join',
                'code_template': '''
# Merge Join: {trans_name}
{output_df} = {left_df}.join(
    {right_df},
    {join_condition},
    "{join_type}"  # inner, left, right, or full
)
'''
            },
            'DATA_CONVERSION': {
                'pyspark_function': 'df.withColumn + cast',
                'code_template': '''
# Data Conversion: {trans_name}
{output_df} = {input_df}
{type_conversions}
'''
            },
            'MULTICAST': {
                'pyspark_function': 'df.cache',
                'code_template': '''
# Multicast: {trans_name}
# Cache DataFrame for multiple uses
{input_df}.cache()
{output_refs}
'''
            },
            'ROW_COUNT': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Row Count: {trans_name}
{count_var} = {input_df}.count()
print(f"Row count for {trans_name}: {{count_var}}")
'''
            },
            'OLE_DB_COMMAND': {
                'pyspark_function': 'df.filter or df.withColumn',
                'code_template': '''
# OLE DB Command: {trans_name}
# For DELETE operations - use left_anti join or filter
{output_df} = {input_df}.filter({condition})
logger.info(f"OLE DB Command DELETE executed: {{ {output_df}.count() }} rows affected")

# For UPDATE operations - use withColumn to update values
# {output_df} = {input_df}.withColumn({column}, {new_value})
logger.info(f"OLE DB Command UPDATE executed")
'''
            },
            'CHECKSUM': {
                'pyspark_function': 'df.withColumn + hash',
                'code_template': '''
# Checksum: {trans_name}
from pyspark.sql.functions import hash, col
# Calculate hash for specified columns
{output_df} = {input_df}.withColumn("CheckSum", hash(*{column_list}))
logger.info(f"Checksum calculated for {{ {output_df}.count() }} rows")
'''
            },
            'MERGE': {
                'pyspark_function': 'df.join',
                'code_template': '''
# Merge Join: {trans_name}
# Join two datasets on specified keys
{output_df} = {input_df1}.join(
    {input_df2},
    on={join_keys},
    how="{join_type}"  # inner, left, right, or full
)
logger.info(f"Merge join completed: {{ {output_df}.count() }} rows")
'''
            },
            'RC_SELECT': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Row Count SELECT: {trans_name}
row_count = {input_df}.count()
logger.info(f"RC SELECT - {trans_name}: {{row_count}} rows")
{output_df} = {input_df}
'''
            },
            'RC_INSERT': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Row Count INSERT: {trans_name}
row_count = {input_df}.count()
logger.info(f"RC INSERT - {trans_name}: {{row_count}} rows to be inserted")
{output_df} = {input_df}
'''
            },
            'RC_UPDATE': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Row Count UPDATE: {trans_name}
row_count = {input_df}.count()
logger.info(f"RC UPDATE - {trans_name}: {{row_count}} rows to be updated")
{output_df} = {input_df}
'''
            },
            'RC_DELETE': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Row Count DELETE: {trans_name}
row_count = {input_df}.count()
logger.info(f"RC DELETE - {trans_name}: {{row_count}} rows to be deleted")
{output_df} = {input_df}
'''
            },
            'RC_INTERMEDIATE': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Row Count INTERMEDIATE: {trans_name}
row_count = {input_df}.count()
logger.info(f"RC INTERMEDIATE - {trans_name}: {{row_count}} rows")
{output_df} = {input_df}
'''
            },
            'TRASH_DESTINATION': {
                'pyspark_function': 'df.count',
                'code_template': '''
# Trash Destination: {trans_name}
row_count = {input_df}.count()
logger.info(f"Data discarded via TRASH DESTINATION - {trans_name}: {{row_count}} rows terminated")
# No further processing needed - path terminates here
{output_df} = {input_df}
'''
            }
        }
        
        # SQL task purpose mappings
        self.sql_task_mappings = {
            'CREATE_TABLE': {
                'pyspark_function': 'spark.sql',
                'code_template': '''
# Create Table: {task_name}
spark.sql("""
{sql_statement}
""")
'''
            },
            'DROP_TABLE': {
                'pyspark_function': 'spark.sql',
                'code_template': '''
# Drop Table: {task_name}
spark.sql("""
{sql_statement}
""")
'''
            },
            'TRUNCATE_TABLE': {
                'pyspark_function': 'spark.sql',
                'code_template': '''
# Truncate Table: {task_name}
spark.sql("""
{sql_statement}
""")
'''
            },
            'INSERT_DATA': {
                'pyspark_function': 'df.write',
                'code_template': '''
# Insert Data: {task_name}
# Use DataFrame.write instead of SQL INSERT for better performance
# Note: SQL statement provided for reference
"""
{sql_statement}
"""
'''
            },
            'SELECT_DATA': {
                'pyspark_function': 'spark.sql',
                'code_template': '''
# Select Data: {task_name}
{df_name} = spark.sql("""
{sql_statement}
""")
'''
            },
            'OTHER_SQL': {
                'pyspark_function': 'spark.sql',
                'code_template': '''
# SQL Task: {task_name}
spark.sql("""
{sql_statement}
""")
'''
            }
        }
        
        # Destination mappings
        self.destination_mappings = {
            'SQL_TABLE': {
                'pyspark_function': 'df.write.format("jdbc")' if not self.databricks_mode else 'df.write.saveAsTable',
                'code_template': '''
# Write to SQL Table: {table_name}
{input_df}.write \\
    .format("jdbc") \\
    .option("url", "{jdbc_url}") \\
    .option("dbtable", "{table_name}") \\
    .option("user", "{username}") \\
    .option("password", "{password}") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .mode("{write_mode}") \\
    .save()
''' if not self.databricks_mode else '''
# Write to Databricks table: {table_name}
{input_df}.write \\
    .mode("{write_mode}") \\
    .saveAsTable("{table_name}")
'''
            },
            'EXCEL': {
                'pyspark_function': 'df.write.format("com.crealytics.spark.excel")',
                'code_template': '''
# Write to Excel: {file_path}
{input_df}.write \\
    .format("com.crealytics.spark.excel") \\
    .option("dataAddress", "{sheet_name}!A1") \\
    .option("header", "true") \\
    .mode("{write_mode}") \\
    .save("{file_path}")
'''
            },
            'CSV': {
                'pyspark_function': 'df.write.csv',
                'code_template': '''
# Write to CSV: {file_path}
{input_df}.write \\
    .option("header", "true") \\
    .option("sep", ",") \\
    .mode("{write_mode}") \\
    .csv("{file_path}")
'''
            },
            'PARQUET': {
                'pyspark_function': 'df.write.parquet',
                'code_template': '''
# Write to Parquet: {file_path}
{input_df}.write \\
    .mode("{write_mode}") \\
    .parquet("{file_path}")
'''
            },
            'OTHER': {
                'pyspark_function': 'df.write.format("jdbc")',
                'fallback': True,
                'code_template': '''
# Write to destination: {dest_name}
# Assuming SQL destination
{input_df}.write \\
    .format("jdbc") \\
    .option("url", "{jdbc_url}") \\
    .option("dbtable", "{table_name}") \\
    .option("user", "{username}") \\
    .option("password", "{password}") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .mode("append") \\
    .save()
'''
            }
        }
    
    def map_json_package(self, json_path: str) -> Dict[str, Any]:
        """
        Map a parsed SSIS JSON file to PySpark code.
        
        Args:
            json_path: Path to the JSON file from data_engineering_parser
            
        Returns:
            Dictionary containing PySpark mappings and generated code
        """
        logger.info(f"Mapping JSON package: {json_path}")
        
        # Load JSON data
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Extract components
        package_name = data.get('package_name', 'Unknown')
        connections = data.get('connections', {})
        sql_tasks = data.get('sql_tasks', [])
        data_flows = data.get('data_flows', [])
        execution_order = data.get('execution_order', [])
        
        # Store connections for use in code generation
        self.package_connections = connections
        
        # Map each component type
        mapped_connections = self._map_connections(connections)
        mapped_sql_tasks = self._map_sql_tasks(sql_tasks)
        mapped_data_flows = self._map_data_flows(data_flows)
        execution_flow = self._map_execution_order(execution_order)
        
        # Generate PySpark code
        pyspark_code = self._generate_pyspark_code(
            package_name,
            mapped_connections,
            mapped_sql_tasks,
            mapped_data_flows,
            execution_flow
        )
        
        # Calculate statistics
        statistics = self._calculate_statistics(
            mapped_connections,
            mapped_sql_tasks,
            mapped_data_flows
        )
        
        return {
            'package_name': package_name,
            'mapped_connections': mapped_connections,
            'mapped_sql_tasks': mapped_sql_tasks,
            'mapped_data_flows': mapped_data_flows,
            'execution_flow': execution_flow,
            'pyspark_code': pyspark_code,
            'statistics': statistics
        }
    
    def _map_connections(self, connections: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Map connection managers to PySpark connection configurations."""
        mapped = []
        
        for conn_id, conn_data in connections.items():
            conn_type = conn_data.get('type', 'UNKNOWN')
            conn_name = conn_data.get('name', 'unknown_connection')
            details = conn_data.get('details', {})
            
            mapping = self.connection_mappings.get(conn_type, {})
            
            mapped.append({
                'id': conn_id,
                'name': conn_name,
                'type': conn_type,
                'details': details,
                'pyspark_reader': mapping.get('reader', 'custom_reader'),
                'pyspark_writer': mapping.get('writer', 'custom_writer'),
                'options_template': mapping.get('options_template', {}),
                'mapped': conn_type in self.connection_mappings
            })
        
        return mapped
    
    def _map_sql_tasks(self, sql_tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map SQL tasks to PySpark equivalents."""
        mapped = []
        
        for task in sql_tasks:
            task_id = task.get('task_id', '')  # Preserve task_id for execution order
            task_name = task.get('name', 'Unknown Task')
            purpose = task.get('purpose', 'OTHER_SQL')
            sql_statement = task.get('sql_statement', '')
            
            mapping = self.sql_task_mappings.get(purpose, self.sql_task_mappings['OTHER_SQL'])
            
            mapped.append({
                'task_id': task_id,  # Include task_id
                'name': task_name,
                'purpose': purpose,
                'sql_statement': sql_statement,
                'pyspark_function': mapping['pyspark_function'],
                'code_template': mapping['code_template'],
                'mapped': purpose in self.sql_task_mappings
            })
        
        return mapped
    
    def _map_data_flows(self, data_flows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map data flows to PySpark DataFrame operations."""
        mapped = []
        
        for df in data_flows:
            task_id = df.get('task_id', '')  # Preserve task_id for execution order
            df_name = df.get('name', 'Unknown Data Flow')
            sources = df.get('sources', [])
            transformations = df.get('transformations', [])
            destinations = df.get('destinations', [])
            data_flow_paths = df.get('data_flow_paths', [])  # Preserve paths for execution order
            
            mapped_sources = self._map_sources(sources)
            mapped_transformations = self._map_transformations(transformations)
            mapped_destinations = self._map_destinations(destinations)
            
            mapped.append({
                'task_id': task_id,  # Include task_id
                'name': df_name,
                'sources': mapped_sources,
                'transformations': mapped_transformations,
                'destinations': mapped_destinations,
                'data_flow_paths': data_flow_paths,  # Include paths for topological sort
                'fully_mapped': all(
                    s['mapped'] for s in mapped_sources
                ) and all(
                    t['mapped'] for t in mapped_transformations
                ) and all(
                    d['mapped'] for d in mapped_destinations
                )
            })
        
        return mapped
    
    def _map_sources(self, sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map data flow sources."""
        mapped = []
        
        for source in sources:
            source_name = source.get('name', 'Unknown Source')
            source_type = source.get('type', 'OTHER')
            table_name = source.get('table_name', source.get('table_or_query', 'Unknown'))
            
            mapping = self.source_mappings.get(source_type, self.source_mappings['OTHER'])
            
            mapped.append({
                'name': source_name,
                'type': source_type,
                'table_name': table_name,
                'pyspark_function': mapping['pyspark_function'],
                'code_template': mapping['code_template'],
                'requires_custom': mapping.get('requires_custom', False),
                'mapped': source_type in self.source_mappings,
                'connection': source.get('connection'),  # Preserve connection info
                'is_sql_query': source.get('is_sql_query', False),  # Preserve SQL query flag
                'sql_query': source.get('sql_query', ''),  # Preserve SQL query
                'access_mode': source.get('access_mode', '1'),  # Preserve access mode
                'component_id': source.get('component_id', '')  # Preserve component ID
            })
        
        return mapped
    
    def _map_transformations(self, transformations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map data flow transformations."""
        mapped = []
        
        for trans in transformations:
            trans_name = trans.get('name', 'Unknown Transformation')
            trans_type = trans.get('type', 'UNKNOWN')
            logic = trans.get('logic', {})
            
            mapping = self.transformation_mappings.get(trans_type, {})
            
            if not mapping:
                # Unmapped transformation - could use LLM fallback
                mapping = {
                    'pyspark_function': 'custom_transformation',
                    'code_template': '# TODO: Implement {trans_name} ({trans_type})'
                }
            
            mapped.append({
                'name': trans_name,
                'type': trans_type,
                'logic': logic,
                'pyspark_function': mapping['pyspark_function'],
                'code_template': mapping['code_template'],
                'mapped': trans_type in self.transformation_mappings,
                'component_id': trans.get('component_id', ''),  # Preserve component ID for paths
                'component_class': trans.get('component_class', ''),  # Preserve component class
                'expressions': trans.get('expressions', []),  # Preserve expressions
                'outputs': trans.get('outputs', [])  # Preserve outputs
            })
        
        return mapped
    
    def _map_destinations(self, destinations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map data flow destinations."""
        mapped = []
        
        for dest in destinations:
            dest_name = dest.get('name', 'Unknown Destination')
            dest_type = dest.get('type', 'OTHER')
            table_name = dest.get('table_name', dest.get('table_or_query', 'Unknown'))
            
            mapping = self.destination_mappings.get(dest_type, self.destination_mappings['OTHER'])
            
            mapped.append({
                'name': dest_name,
                'type': dest_type,
                'table_name': table_name,
                'pyspark_function': mapping['pyspark_function'],
                'code_template': mapping['code_template'],
                'mapped': dest_type in self.destination_mappings or mapping.get('fallback', False),
                'connection': dest.get('connection'),  # Preserve connection info
                'component_id': dest.get('component_id', '')  # Preserve component ID
            })
        
        return mapped
    
    def _map_execution_order(self, execution_order: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Map execution order to PySpark execution flow - preserving task IDs for correct ordering."""
        return [
            {
                'from': link.get('from', ''),
                'to': link.get('to', ''),
                'from_id': link.get('from_id', ''),  # Preserve task ID for lookup
                'to_id': link.get('to_id', ''),      # Preserve task ID for lookup
                'description': f"Execute {link.get('to', '')} after {link.get('from', '')}"
            }
            for link in execution_order
        ]
    
    def _generate_pyspark_code(self,
                               package_name: str,
                               mapped_connections: List[Dict[str, Any]],
                               mapped_sql_tasks: List[Dict[str, Any]],
                               mapped_data_flows: List[Dict[str, Any]],
                               execution_flow: List[Dict[str, Any]]) -> str:
        """Generate complete PySpark code from mappings."""
        
        code_sections = []
        
        # Header
        code_sections.append(self._generate_header(package_name))
        
        # Imports
        code_sections.append(self._generate_imports(mapped_data_flows))
        
        # Spark initialization
        code_sections.append(self._generate_spark_init(package_name))
        
        # Connection configurations
        if mapped_connections:
            code_sections.append(self._generate_connection_configs(mapped_connections))
        
        # Generate code in execution order if available, otherwise use default order
        if execution_flow:
            code_sections.append(self._generate_ordered_execution_code(
                mapped_sql_tasks, mapped_data_flows, execution_flow, mapped_connections))
        else:
            # Fallback to original order
            if mapped_sql_tasks:
                code_sections.append(self._generate_sql_tasks_code(mapped_sql_tasks))
            if mapped_data_flows:
                code_sections.append(self._generate_data_flows_code(mapped_data_flows))
        
        # Footer
        code_sections.append(self._generate_footer())
        
        code = '\n\n'.join(code_sections)
        code = self._postprocess_generated_code(code, package_name, mapped_connections)
        return code
    
    def _postprocess_generated_code(self,
                                    code: str,
                                    package_name: str = "",
                                    mapped_connections: Optional[List[Dict[str, Any]]] = None) -> str:
        """Apply cleanup rules to the generated PySpark code."""
        code = self._compress_placeholder_variable_comments(code)
        
        # Ensure header docstring exists
        header_marker = "PySpark conversion of SSIS Package"
        if package_name and header_marker not in code:
            header_block = self._generate_header(package_name)
            code = f"{header_block}\n\n{code}"

        # Ensure connection configuration comments remain when connections exist
        if mapped_connections:
            needs_connection_block = "# Connection Configurations" not in code
            if needs_connection_block:
                connection_block = self._generate_connection_configs(mapped_connections)
                if connection_block:
                    logger_init_marker = "logger = logging.getLogger(__name__)"
                    if logger_init_marker in code:
                        code = code.replace(
                            logger_init_marker,
                            f"{logger_init_marker}\n\n{connection_block}",
                            1
                        )
                    else:
                        code = f"{connection_block}\n\n{code}"

        code = re.sub(r'\n{3,}', '\n\n', code)
        return code

    def _compress_placeholder_variable_comments(self, code: str) -> str:
        """
        Simplify verbose comment blocks that precede stored procedure variables.
        Keeps EXEC comments if present and guarantees a concise TODO on the assignment.
        """
        # Pattern specifically for task_work_history_id style variables
        pattern = re.compile(
            r"((?:# [^\n]*\n)+)(\s*([A-Za-z_]\w*)\s*=\s*[^ \n][^\n]*\n)",
            flags=re.IGNORECASE
        )

        def replacer(match: re.Match) -> str:
            comment_block = match.group(1)
            assignment_line = match.group(2)
            var_name = match.group(3)

            # Only target WorkHistoryID style variables to avoid over-cleaning unrelated comments
            if 'workhistoryid' not in var_name.lower():
                return match.group(0)

            # Preserve only EXEC comments from the block (if any)
            exec_lines = [
                line for line in comment_block.splitlines(keepends=True)
                if 'EXEC' in line.upper()
            ]
            preserved_comment = ''.join(exec_lines)

            # Ensure assignment carries a concise TODO comment
            stripped_assignment = assignment_line.rstrip('\n')
            if '# TODO' not in stripped_assignment:
                stripped_assignment = f"{stripped_assignment}  # TODO: Populate with actual value"
            elif 'actual value' not in stripped_assignment.lower():
                # Normalize the TODO wording
                stripped_assignment = re.sub(
                    r'#\s*TODO:.*',
                    '# TODO: Populate with actual value',
                    stripped_assignment
                )

            return f"{preserved_comment}{stripped_assignment}\n"

        return pattern.sub(replacer, code)
    
    def _generate_ordered_execution_code(self, 
                                        mapped_sql_tasks: List[Dict[str, Any]], 
                                        mapped_data_flows: List[Dict[str, Any]], 
                                        execution_flow: List[Dict[str, Any]],
                                        mapped_connections: List[Dict[str, Any]] = None) -> str:
        """Generate code in the correct execution order based on SSIS control flow using task IDs and topological sort."""
        
        # Create unified lookup dictionary by task ID (from SSIS DTSID)
        all_tasks = {}
        
        # Index SQL tasks by their task_id
        for task in mapped_sql_tasks:
            task_id = task.get('task_id', '')
            if task_id:
                all_tasks[task_id] = ('sql_task', task)
        
        # Index data flows by their task_id
        for df in mapped_data_flows:
            task_id = df.get('task_id', '')
            if task_id:
                all_tasks[task_id] = ('data_flow', df)
        
        # Build dependency graph for topological sort
        # graph: task_id -> list of task_ids that depend on it (successors)
        # in_degree: task_id -> count of tasks it depends on (predecessors)
        graph = {task_id: [] for task_id in all_tasks.keys()}
        in_degree = {task_id: 0 for task_id in all_tasks.keys()}
        
        # Build the graph from precedence constraints
        for flow in execution_flow:
            from_id = flow.get('from_id', '')
            to_id = flow.get('to_id', '')
            
            if from_id and to_id and from_id in all_tasks and to_id in all_tasks:
                graph[from_id].append(to_id)
                in_degree[to_id] += 1
        
        # Perform topological sort using Kahn's algorithm
        # Start with all nodes that have no dependencies (in_degree == 0)
        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        execution_order = []
        
        while queue:
            # Sort queue for deterministic output (tasks at same level sorted alphabetically)
            queue.sort(key=lambda tid: all_tasks[tid][1].get('name', ''))
            
            # Process the first task with no dependencies
            current_id = queue.pop(0)
            task_type, task_data = all_tasks[current_id]
            execution_order.append((task_type, task_data))
            
            # Reduce in-degree of all successors
            for successor_id in graph[current_id]:
                in_degree[successor_id] -= 1
                # If successor now has no dependencies, add to queue
                if in_degree[successor_id] == 0:
                    queue.append(successor_id)
        
        # Check for cycles (if not all tasks were processed)
        if len(execution_order) < len(all_tasks):
            unprocessed = [tid for tid in all_tasks.keys() 
                          if tid not in [t[1].get('task_id') for t in execution_order]]
            logger.warning(f"Cycle detected or disconnected tasks: {unprocessed}")
            # Add remaining tasks at the end
            for task_id in unprocessed:
                execution_order.append(all_tasks[task_id])
        
        # Generate code sections in order
        code_sections = []
        code_sections.append("# =============================================================================")
        code_sections.append("# CONTROL FLOW EXECUTION")
        code_sections.append("# =============================================================================")
        
        step_number = 1
        for component_type, component in execution_order:
            if component_type == 'sql_task':
                code_sections.append(f"# Step {step_number}: {component['name']}")
                code_sections.append(f"logger.info(\"Step {step_number}: Executing SQL Task - {component['name']}\")")
                code_sections.append(self._generate_single_sql_task_code(component))
                step_number += 1
            elif component_type == 'data_flow':
                code_sections.append(f"# Step {step_number}: {component['name']}")
                code_sections.append(f"logger.info(\"Step {step_number}: Processing Data Flow - {component['name']}\")")
                code_sections.append(self._generate_single_data_flow_code(component, mapped_connections))
                step_number += 1
        
        return '\n\n'.join(code_sections)
    
    def _generate_single_sql_task_code(self, task: Dict[str, Any]) -> str:
        """Generate code for a single SQL task."""
        task_name_clean = task['name'].replace(' ', '_').replace('-', '_')
        sql_statement = task['sql_statement']
        
        # Get connection name for schema mapping
        connection_name = task.get('connection', {}).get('name', '') if isinstance(task.get('connection'), dict) else ''
        
        # Apply schema mapping to SQL statement if available
        # If connection is not provided, schema mapper will try to infer it from table names
        if self.schema_mapper and self.schema_mapper.is_active():
            sql_statement = self.schema_mapper.apply_mapping_to_sql(sql_statement, connection_name if connection_name else None)
        
        # Sanitize SQL for Databricks compatibility
        sql_statement = self._sanitize_sql_for_databricks(sql_statement)
        
        purpose = task.get('purpose', 'OTHER_SQL')
        
        # Check if this is a stored procedure call (EXEC statement)
        is_stored_proc = sql_statement.strip().upper().startswith('EXEC')
        
        # Add context based on purpose
        if purpose == 'SELECT_DATA':
            return f'''
{task_name_clean}_result = spark.sql("""
{sql_statement}
""")
# Store result for later use if needed
'''
        elif is_stored_proc:
            # For stored procedures, provide minimal comments (EXEC line only)
            stored_proc_name = sql_statement.strip().split()[1] if len(sql_statement.strip().split()) > 1 else "stored_procedure"
            return f'''
# EXEC {stored_proc_name}
# TODO: Implement stored procedure call
'''
        else:
            return f'''
spark.sql("""
{sql_statement}
""")
'''
    
    def _generate_single_data_flow_code(self, data_flow: Dict[str, Any], mapped_connections: List[Dict[str, Any]] = None) -> str:
        """Generate code for a single data flow with proper execution order based on paths."""
        # Reset DataFrame flow tracking for this data flow
        self.df_flow = {}
        # Track Conditional Split outputs for if statement generation
        self.conditional_split_outputs = {}  # Maps component_id -> {output_name: df_name}
        
        # Get data flow paths to understand connections
        data_flow_paths = data_flow.get('data_flow_paths', [])
        
        # Build execution order using topological sort (similar to control flow)
        ordered_components = self._build_data_flow_execution_order(
            data_flow.get('sources', []),
            data_flow.get('transformations', []),
            data_flow.get('destinations', []),
            data_flow_paths
        )
        
        code_sections = []
        
        # Generate code in execution order
        for component_type, component in ordered_components:
            if component_type == 'source':
                code_sections.append(self._generate_source_code(component, mapped_connections))
            elif component_type == 'transformation':
                code_sections.append(self._generate_transformation_code_enhanced(component, data_flow_paths))
            elif component_type == 'destination':
                code_sections.append(self._generate_destination_code(component, mapped_connections))
        
        return '\n\n'.join(code_sections)
    
    def _build_data_flow_execution_order(self, 
                                        sources: List[Dict[str, Any]], 
                                        transformations: List[Dict[str, Any]], 
                                        destinations: List[Dict[str, Any]], 
                                        data_flow_paths: List[Dict[str, str]]) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Build execution order for data flow components using topological sort.
        Similar to control flow execution order but handles data flow specific cases.
        
        Args:
            sources: List of source components
            transformations: List of transformation components
            destinations: List of destination components
            data_flow_paths: List of path connections with from_id/to_id
            
        Returns:
            List of tuples (component_type, component_data) in execution order
        """
        # Normalize component IDs for better matching (handle different separator styles)
        def normalize_id(comp_id: str) -> str:
            """Normalize component ID for matching."""
            if not comp_id:
                return ''
            # Normalize: replace forward slashes and normalize backslashes
            # Handle both single and double backslashes (escape sequences)
            normalized = comp_id.replace('/', '\\')
            # Normalize multiple backslashes to single (handles both \ and \\)
            while '\\\\' in normalized:
                normalized = normalized.replace('\\\\', '\\')
            return normalized.strip()
        
        # Create unified lookup dictionary by component_id
        all_components = {}
        name_to_id_map = {}  # Map component name -> component_id for fallback matching
        
        # Index sources by component_id
        for source in sources:
            comp_id = normalize_id(source.get('component_id', ''))
            comp_name = source.get('name', '')
            if comp_id:
                all_components[comp_id] = ('source', source)
                if comp_name:
                    name_to_id_map[comp_name] = comp_id
            elif comp_name:
                # If no component_id, use name as key
                all_components[comp_name] = ('source', source)
                name_to_id_map[comp_name] = comp_name
        
        # Index transformations by component_id
        for trans in transformations:
            comp_id = normalize_id(trans.get('component_id', ''))
            comp_name = trans.get('name', '')
            if comp_id:
                all_components[comp_id] = ('transformation', trans)
                if comp_name:
                    name_to_id_map[comp_name] = comp_id
            elif comp_name:
                all_components[comp_name] = ('transformation', trans)
                name_to_id_map[comp_name] = comp_name
        
        # Index destinations by component_id
        for dest in destinations:
            comp_id = normalize_id(dest.get('component_id', ''))
            comp_name = dest.get('name', '')
            if comp_id:
                all_components[comp_id] = ('destination', dest)
                if comp_name:
                    name_to_id_map[comp_name] = comp_id
            elif comp_name:
                all_components[comp_name] = ('destination', dest)
                name_to_id_map[comp_name] = comp_name
        
        logger.debug(f"Indexed {len(all_components)} components: {len(sources)} sources, "
                    f"{len(transformations)} transformations, {len(destinations)} destinations")
        
        # Helper to find component_id with fuzzy matching
        def find_component_id(comp_id: str, comp_name: str = '') -> Optional[str]:
            """Find component_id with fuzzy matching."""
            # Normalize input ID
            normalized_input_id = normalize_id(comp_id) if comp_id else ''
            
            # Try exact match first (normalized)
            if normalized_input_id:
                # Try direct lookup
                if normalized_input_id in all_components:
                    return normalized_input_id
                
                # Try matching against normalized keys
                for cid in all_components.keys():
                    cid_normalized = normalize_id(cid)
                    if cid_normalized == normalized_input_id:
                        return cid
            
            # Try matching by name
            if comp_name:
                # Direct name lookup
                if comp_name in name_to_id_map:
                    return name_to_id_map[comp_name]
                
                # Try partial match (component_id ends with name or name starts with component)
                # This handles cases like "DER" matching "DER_DefaultFlags"
                for cid in all_components.keys():
                    cid_normalized = normalize_id(cid)
                    comp_name_normalized = normalize_id(comp_name)
                    comp_data = all_components[cid][1]
                    comp_actual_name = normalize_id(comp_data.get('name', ''))
                    
                    # Check if normalized ID ends with normalized name
                    if cid_normalized.endswith(comp_name_normalized) or comp_name_normalized.endswith(cid_normalized):
                        return cid
                    
                    # Check if name appears in ID
                    if comp_name in cid_normalized or comp_name in cid:
                        return cid
                    
                    # Check if component name starts with path name (handles "DER" matching "DER_DefaultFlags")
                    # This is important for paths that use abbreviated names
                    # Make sure it's a prefix match (e.g., "DER" matches "DER_DefaultFlags" but not "OTHER_DER")
                    if (comp_actual_name.startswith(comp_name_normalized) or 
                        comp_actual_name.startswith(comp_name_normalized + '_') or 
                        comp_actual_name.startswith(comp_name_normalized + ' ')):
                        return cid
                    
                    # Also check if path name appears as a word boundary in component name
                    # Handles cases where "DER" should match "DER_DefaultFlags"
                    import re
                    pattern = r'\b' + re.escape(comp_name_normalized) + r'\b'
                    if re.search(pattern, comp_actual_name, re.IGNORECASE):
                        return cid
            
            return None
        
        # Build dependency graph for topological sort
        # graph: component_id -> list of component_ids that depend on it (successors)
        # in_degree: component_id -> count of components it depends on (predecessors)
        graph = {comp_id: [] for comp_id in all_components.keys()}
        in_degree = {comp_id: 0 for comp_id in all_components.keys()}
        
        matched_paths = 0
        unmatched_paths = 0
        
        # Build the graph from data flow paths
        for path in data_flow_paths:
            from_id = path.get('from_id', '')
            to_id = path.get('to_id', '')
            from_name = path.get('from', '')
            to_name = path.get('to', '')
            
            # Try to find component IDs with fuzzy matching
            resolved_from_id = find_component_id(from_id, from_name)
            resolved_to_id = find_component_id(to_id, to_name)
            
            if resolved_from_id and resolved_to_id and resolved_from_id in all_components and resolved_to_id in all_components:
                if resolved_to_id not in graph[resolved_from_id]:
                    graph[resolved_from_id].append(resolved_to_id)
                    in_degree[resolved_to_id] += 1
                    matched_paths += 1
                else:
                    matched_paths += 1  # Already in graph, but still matched
            else:
                unmatched_paths += 1
                if unmatched_paths <= 5:  # Log first few unmatched paths
                    logger.debug(f"Unmatched path: from_id='{from_id}' ({from_name}) -> to_id='{to_id}' ({to_name})")
        
        if unmatched_paths > 0:
            logger.warning(f"Found {unmatched_paths} unmatched paths out of {len(data_flow_paths)} total paths")
            # Log details about unmatched paths for debugging
            if logger.level <= logging.DEBUG:
                for i, path in enumerate(data_flow_paths[:5]):  # Show first 5 unmatched
                    from_id = path.get('from_id', '')
                    to_id = path.get('to_id', '')
                    from_name = path.get('from', '')
                    to_name = path.get('to', '')
                    resolved_from = find_component_id(from_id, from_name)
                    resolved_to = find_component_id(to_id, to_name)
                    if not resolved_from or not resolved_to:
                        logger.debug(f"  Unmatched path {i+1}: from_id='{from_id}' -> resolved='{resolved_from}', "
                                   f"to_id='{to_id}' -> resolved='{resolved_to}'")
        
        logger.debug(f"Built dependency graph: {matched_paths} paths matched, {unmatched_paths} unmatched")
        no_deps = [cid for cid, deg in in_degree.items() if deg == 0]
        logger.debug(f"Components with no dependencies ({len(no_deps)}): {[all_components[cid][1].get('name', cid) for cid in no_deps[:5]]}")
        
        # Perform topological sort using Kahn's algorithm
        # Start with all components that have no dependencies (sources typically)
        # Prioritize sources: sources have no incoming paths by definition
        queue = []
        
        # First, add all sources (they should have no dependencies)
        sources_with_no_deps = [
            comp_id for comp_id, (comp_type, _) in all_components.items() 
            if comp_type == 'source' and in_degree[comp_id] == 0
        ]
        queue.extend(sources_with_no_deps)
        
        # Then, add other components with no dependencies (transformations/destinations)
        other_with_no_deps = [
            comp_id for comp_id, degree in in_degree.items() 
            if degree == 0 and comp_id not in sources_with_no_deps
        ]
        queue.extend(other_with_no_deps)
        
        execution_order = []
        
        while queue:
            # Sort queue for deterministic output:
            # 1. Sources first
            # 2. Then transformations
            # 3. Then destinations
            # Within each type, sort alphabetically
            def sort_key(cid):
                comp_type, comp_data = all_components[cid]
                type_order = {'source': 0, 'transformation': 1, 'destination': 2}.get(comp_type, 3)
                comp_name = comp_data.get('name', '')
                return (type_order, comp_name)
            
            # ENSURE sources are processed first - always prioritize sources over transformations/destinations
            # This handles cases where transformations are incorrectly detected as having no dependencies
            # Check for sources BEFORE sorting, to ensure they're always processed first
            sources_in_queue = [cid for cid in queue if all_components[cid][0] == 'source']
            if sources_in_queue:
                # Process sources first, even if transformations also have no dependencies
                # Sort sources alphabetically to maintain deterministic order
                sources_in_queue.sort(key=lambda cid: all_components[cid][1].get('name', ''))
                current_id = sources_in_queue[0]
                queue.remove(current_id)
            else:
                # No sources in queue, sort and process first item normally
                queue.sort(key=sort_key)
                current_id = queue.pop(0)
            
            component_type, component_data = all_components[current_id]
            execution_order.append((component_type, component_data))
            
            # Reduce in-degree of all successors
            for successor_id in graph[current_id]:
                in_degree[successor_id] -= 1
                # If successor now has no dependencies, add to queue
                if in_degree[successor_id] == 0 and successor_id not in queue:
                    queue.append(successor_id)
        
        # Check for cycles or disconnected components (if not all components were processed)
        if len(execution_order) < len(all_components):
            # Get all component IDs/names from execution order
            processed_ids = set()
            for comp_type, comp_data in execution_order:
                comp_id = comp_data.get('component_id', '')
                comp_name = comp_data.get('name', '')
                if comp_id:
                    processed_ids.add(comp_id)
                if comp_name:
                    processed_ids.add(comp_name)
            
            # Find unprocessed components
            unprocessed = [cid for cid in all_components.keys() if cid not in processed_ids]
            logger.warning(f"Cycle detected or disconnected components in data flow: {len(unprocessed)} components")
            logger.warning(f"Unprocessed component IDs: {unprocessed[:10]}")  # Show first 10
            
            # Add remaining components at the end (may be cycles or disconnected)
            # Try to maintain a reasonable order: sources first, then transformations, then destinations
            unprocessed_sources = []
            unprocessed_transforms = []
            unprocessed_dests = []
            
            for comp_id in unprocessed:
                comp_type, comp_data = all_components[comp_id]
                if comp_type == 'source':
                    unprocessed_sources.append((comp_type, comp_data))
                elif comp_type == 'transformation':
                    unprocessed_transforms.append((comp_type, comp_data))
                elif comp_type == 'destination':
                    unprocessed_dests.append((comp_type, comp_data))
            
            # Add unprocessed components in order: sources, transformations, destinations
            execution_order.extend(unprocessed_sources)
            execution_order.extend(unprocessed_transforms)
            execution_order.extend(unprocessed_dests)
        
        logger.debug(f"Final execution order: {len(execution_order)} components "
                    f"({sum(1 for t, _ in execution_order if t == 'source')} sources, "
                    f"{sum(1 for t, _ in execution_order if t == 'transformation')} transformations, "
                    f"{sum(1 for t, _ in execution_order if t == 'destination')} destinations)")
        
        return execution_order
    
    def _find_component_id_by_name(self, component_name: str, all_components: Dict[str, Tuple]) -> Optional[str]:
        """Find component_id by component name (for backward compatibility)."""
        for comp_id, (_, component_data) in all_components.items():
            if component_data.get('name', '') == component_name:
                return comp_id
        return None
    
    def _generate_header(self, package_name: str) -> str:
        """Generate code header."""
        from datetime import datetime as dt
        if self.databricks_mode:
            return f'''"""
PySpark conversion of SSIS Package: {package_name}
Generated by SSIS-to-PySpark Converter (Databricks Optimized)
Date: {dt.now().strftime("%Y-%m-%d %H:%M:%S")}
"""'''
        else:
            return f'''"""
PySpark conversion of SSIS Package: {package_name}
Generated by SSIS-to-PySpark Converter
Date: {dt.now().strftime("%Y-%m-%d %H:%M:%S")}

This code was automatically generated from SSIS package.
Please review and test before using in production.
"""'''
    
    def _generate_imports(self, mapped_data_flows: List[Dict[str, Any]]) -> str:
        """Generate import statements."""
        imports = [
            "from pyspark.sql.functions import *",
            "from pyspark.sql.types import *",
            "import logging"
        ]
        
        if not self.databricks_mode:
            imports.insert(0, "from pyspark.sql import SparkSession")
        
        # Check if REST API sources are used
        for df in mapped_data_flows:
            for source in df['sources']:
                if source['type'] == 'REST_API_JSON':
                    imports.extend(["import requests", "import json"])
                    break
        
        return '\n'.join(imports)
    
    def _generate_spark_init(self, package_name: str) -> str:
        """Generate Spark session initialization."""
        if self.databricks_mode:
            return '''import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)'''
        else:
            return f'''# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("{package_name.replace(' ', '_')}") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Set log level
# Note: spark.sparkContext is not supported on Databricks serverless compute
# If not using serverless, uncomment the line below:
# spark.sparkContext.setLogLevel("WARN")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)'''
    
    def _sanitize_sql_for_databricks(self, sql_statement: str) -> str:
        """
        Sanitize SQL statements for Databricks compatibility.
        
        Fixes common issues:
        1. Removes square brackets [schema].[table] -> schema.table
        2. Removes table prefixes from INSERT column lists (e.g., Table.Column -> Column)
        3. Ensures proper SQL syntax
        4. Adds missing VALUES keyword to INSERT statements
        """
        import re
        
        # Remove square brackets for Databricks compatibility
        # [schema].[table] -> schema.table
        sql_statement = re.sub(r'\[([^\]]+)\]', r'\1', sql_statement)
        
        # Pattern to match INSERT statements with dot notation in column lists
        # Matches: INSERT INTO table(Table.col1, Table.col2) -> INSERT INTO table(col1, col2)
        def fix_insert_columns(match):
            full_match = match.group(0)
            table_name = match.group(1)
            columns_section = match.group(2)
            
            # Remove table prefixes from column names
            # Pattern: TableName.ColumnName -> ColumnName
            fixed_columns = re.sub(
                r'\b\w+\.(\[?[\w\s\.\-]+\]?)\b',  # Matches: word.word or word.[word with spaces]
                r'\1',  # Replace with just the column name
                columns_section
            )
            
            return f"insert into {table_name}({fixed_columns})"
        
        # Fix INSERT INTO table(Table.Column, ...) patterns
        sql_statement = re.sub(
            r'insert\s+into\s+([\w\[\]\.]+)\s*\(([\s\S]*?)\)\s*(?:select|values)',
            fix_insert_columns,
            sql_statement,
            flags=re.IGNORECASE
        )
        
        # Fix INSERT statements missing VALUES keyword
        # Pattern: INSERT INTO table (columns) (values) -> INSERT INTO table (columns) VALUES (values)
        sql_statement = re.sub(
            r'INSERT\s+INTO\s+([^(]+)\s*\(([^)]+)\)\s*\(([^)]+)\)',
            r'INSERT INTO \1(\2) VALUES (\3)',
            sql_statement,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        # Fix multi-row INSERT statements
        # Pattern: INSERT INTO table (columns) (row1), (row2) -> INSERT INTO table (columns) VALUES (row1), (row2)
        sql_statement = re.sub(
            r'INSERT\s+INTO\s+([^(]+)\s*\([^)]+\)\s*\(([^)]+)\)\s*,\s*\(([^)]+)\)',
            r'INSERT INTO \1 VALUES (\2), (\3)',
            sql_statement,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        return sql_statement
    
    def _generate_connection_configs(self, mapped_connections: List[Dict[str, Any]]) -> str:
        """Generate connection configuration code."""
        if self.databricks_mode:
            # Generate commented connection configurations as fallback
            config_lines = ["# Connection Configurations (Commented - Uncomment if needed)"]
            
            for conn in mapped_connections:
                # Fix variable naming - remove spaces and special characters
                conn_name = conn['name'].replace(' ', '_').replace('.', '_').replace('-', '_').replace(',', '_').lower()
                details = conn['details']
                
                if conn['type'] == 'SQL_DATABASE':
                    server = details.get('server', 'localhost')
                    database = details.get('database', 'database_name')
                    config_lines.append(f'''
# Connection: {conn['name']}
# {conn_name}_url = "jdbc:sqlserver://{server}:1433;databaseName={database}"
# {conn_name}_user = "username"  # TODO: Set actual username
# {conn_name}_password = "password"  # TODO: Set actual password
# {conn_name}_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
''')
            
            return '\n'.join(config_lines)
        
        config_lines = ["# Connection Configurations"]
        
        for conn in mapped_connections:
            # Fix variable naming - remove spaces and special characters
            conn_name = conn['name'].replace(' ', '_').replace('.', '_').replace('-', '_').replace(',', '_').lower()
            details = conn['details']
            
            if conn['type'] == 'SQL_DATABASE':
                server = details.get('server', 'localhost')
                database = details.get('database', 'database_name')
                config_lines.append(f'''
# Connection: {conn['name']}
{conn_name}_url = "jdbc:sqlserver://{server}:1433;databaseName={database}"
{conn_name}_user = "username"  # TODO: Set actual username
{conn_name}_password = "password"  # TODO: Set actual password
{conn_name}_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
''')
        
        return '\n'.join(config_lines)
    
    def _generate_sql_tasks_code(self, mapped_sql_tasks: List[Dict[str, Any]]) -> str:
        """Generate SQL tasks code with connection context."""
        code_lines = ["# SQL Tasks"]
        
        for task in mapped_sql_tasks:
            task_name_clean = task['name'].replace(' ', '_').replace('-', '_')
            sql_statement = task['sql_statement']
            
            # Sanitize SQL for Databricks compatibility
            sql_statement = self._sanitize_sql_for_databricks(sql_statement)
            
            purpose = task.get('purpose', 'OTHER_SQL')
            
            # Add context based on purpose
            if purpose == 'SELECT_DATA':
                code_lines.append(f'''
# Task: {task['name']} ({purpose})
logger.info("Executing SQL Task: {task['name']}")
{task_name_clean}_result = spark.sql("""
{sql_statement}
""")
# Store result for later use if needed
''')
            else:
                code_lines.append(f'''
# Task: {task['name']} ({purpose})
logger.info("Executing SQL Task: {task['name']}")
spark.sql("""
{sql_statement}
""")
''')
        
        return '\n'.join(code_lines)
    
    def _generate_data_flows_code(self, mapped_data_flows: List[Dict[str, Any]]) -> str:
        """Generate data flows code."""
        code_sections = []
        
        for df in mapped_data_flows:
            # Reset DataFrame flow tracking for each data flow
            self.df_flow = {}
            
            code_sections.append(f"# Data Flow: {df['name']}")
            code_sections.append(f"logger.info('Processing Data Flow: {df['name']}')")
            
            # Generate source code
            for source in df['sources']:
                code_sections.append(self._generate_source_code(source))
            
            # Generate transformation code
            for trans in df['transformations']:
                code_sections.append(self._generate_transformation_code(trans))
            
            # Generate destination code
            for dest in df['destinations']:
                code_sections.append(self._generate_destination_code(dest))
        
        return '\n\n'.join(code_sections)
    
    def _generate_databricks_source_code(self, source: Dict[str, Any]) -> str:
        """Generate Databricks-optimized source code."""
        component_id = source.get('component_id', '')
        df_name = self._get_or_create_df_name(component_id, source['name'])
        table_name = source.get('table_name', '')
        
        # Get connection name for schema mapping
        source_connection = source.get('connection', {})
        connection_name = source_connection.get('name', '') if isinstance(source_connection, dict) else ''
        
        # If table name is empty, try to extract from source name or use a default
        if not table_name or table_name.strip() == '':
            # Try to extract table name from source name
            source_name = source.get('name', '')
            # Use generic fallback - schema mapping will handle actual table names
            table_name = 'unknown_table'  # Fallback
        
        # Apply schema mapping if available
        if self.schema_mapper and self.schema_mapper.is_active() and connection_name:
            table_name = self.schema_mapper.get_databricks_table_name(connection_name, table_name)
        
        # Check if this is a SQL query (AccessMode=2) or table (AccessMode=1)
        is_sql_query = source.get('is_sql_query', False)
        sql_query = source.get('sql_query', '')
        
        if is_sql_query and sql_query:
            # Apply schema mapping to SQL query if available
            if self.schema_mapper and self.schema_mapper.is_active() and connection_name:
                sql_query = self.schema_mapper.apply_mapping_to_sql(sql_query, connection_name)
            
            # Use spark.sql for SQL queries
            # Use original SQL query
            sanitized_sql = self._sanitize_sql_for_databricks(sql_query)
            sql_first_line = sanitized_sql.split('\n')[0].strip()
            return f'''
# Source: {source['name']}
# SQL Query: {sql_first_line}...
logger.info("Reading from source: {source['name']}")
{df_name} = spark.sql("""
    {sanitized_sql}
""")

logger.info(f"Source data loaded: {{ {df_name}.count() }} rows")
'''
        elif is_sql_query and not sql_query:
            # Handle case where SQL query is empty - use table name directly
            return f'''
# Source: {source['name']}
# Note: SQL query was empty in source definition
logger.info("Reading from source: {source['name']}")
{df_name} = spark.table("{table_name}")

logger.info(f"Source data loaded: {{ {df_name}.count() }} rows")
'''
        else:
            # Use spark.table for table access
            return f'''
# Source: Read from Databricks table ({table_name})
logger.info("Reading from source: {table_name}")
{df_name} = spark.table("{table_name}")

logger.info(f"Source data loaded: {{ {df_name}.count() }} rows")
'''
    
    def _generate_source_code(self, source: Dict[str, Any], mapped_connections: List[Dict[str, Any]] = None) -> str:
        """Generate source reading code with actual connection details."""
        if self.databricks_mode:
            return self._generate_databricks_source_code(source)
        
        component_id = source.get('component_id', '')
        df_name = self._get_or_create_df_name(component_id, source['name'])
        table_name = source.get('table_name', '')
        
        # Get connection details if available
        source_connection = source.get('connection')
        conn_var_prefix = None
        
        if source_connection and mapped_connections:
            # Find the mapped connection
            conn_name = source_connection.get('name', '')
            for mapped_conn in mapped_connections:
                if mapped_conn['name'] == conn_name:
                    conn_var_prefix = self._get_connection_var_name(mapped_conn)
                    break
        
        
        # If table name is empty, try to extract from source name or use a default
        if not table_name or table_name.strip() == '':
            # Try to extract table name from source name
            source_name = source.get('name', '')
            # Use generic fallback - schema mapping will handle actual table names
            table_name = 'unknown_table'  # Fallback
        
        if source['type'] == 'REST_API_JSON':
            return f'''
# Source: {source['name']} (REST API)
# TODO: Set actual API URL and authentication
api_url = "https://api.example.com/data"
response = requests.get(api_url)
data = response.json()
{df_name} = spark.createDataFrame(data)
'''
        else:
            if conn_var_prefix:
                # Check if this is a SQL query (AccessMode=2) or table (AccessMode=1)
                is_sql_query = source.get('is_sql_query', False)
                sql_query = source.get('sql_query', '')
                
                if is_sql_query and sql_query:
                    # Use query option for SQL commands
                    # Format SQL query for comment (first line only)
                    sql_first_line = sql_query.split('\n')[0].strip()
                    return f'''
# Source: {source['name']}
# SQL Query: {sql_first_line}...
logger.info("Reading from source: {source['name']}")
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", {conn_var_prefix}_url) \\
    .option("query", """{sql_query}""") \\
    .option("user", {conn_var_prefix}_user) \\
    .option("password", {conn_var_prefix}_password) \\
    .option("driver", {conn_var_prefix}_driver) \\
    .load()

# Log source data info
logger.info(f"Source data loaded: {{ {df_name}.count() }} rows")
{df_name}.printSchema()
'''
                else:
                    # Use dbtable option for table access
                    return f'''
# Source: {source['name']}
# Table: {table_name}
logger.info("Reading from source: {table_name}")
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", {conn_var_prefix}_url) \\
    .option("dbtable", "{table_name}") \\
    .option("user", {conn_var_prefix}_user) \\
    .option("password", {conn_var_prefix}_password) \\
    .option("driver", {conn_var_prefix}_driver) \\
    .load()

# Log source data info
logger.info(f"Source data loaded: {{ {df_name}.count() }} rows")
{df_name}.printSchema()
'''
            else:
                # Fallback to placeholders
                return f'''
# Source: {source['name']}
# Table: {table_name}
logger.info("Reading from source: {table_name}")
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:sqlserver://server:1433;databaseName=database") \\
    .option("dbtable", "{table_name}") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .load()

# Log source data info
logger.info(f"Source data loaded: {{ {df_name}.count() }} rows")
{df_name}.printSchema()
'''
    
    def _get_connection_var_name(self, connection_info: Optional[Dict[str, Any]]) -> Optional[str]:
        """Get the connection variable name from connection info."""
        if not connection_info:
            return None
        
        conn_name = connection_info.get('name', '')
        if not conn_name:
            return None
        
        # Convert connection name to variable name (match the new naming convention)
        var_name = conn_name.replace(' ', '_').replace('.', '_').replace('-', '_').replace(',', '_').lower()
        return var_name
    
    def _generate_transformation_code(self, trans: Dict[str, Any]) -> str:
        """Generate transformation code with actual logic extraction."""
        trans_name = trans['name']
        trans_type = trans['type']
        logic = trans.get('logic', {})
        component_id = trans.get('component_id', '')
        
        # Get input DataFrame name
        input_df = self._get_or_create_df_name(component_id, f'{trans_name}_input')
        output_df = self._get_or_create_df_name(component_id, trans_name)
        
        if trans_type == 'DERIVED_COLUMN':
            return self._generate_derived_column_code(trans_name, input_df, output_df, logic)
        
        elif trans_type == 'LOOKUP':
            # Get connection name for schema mapping
            lookup_connection = trans.get('connection', {})
            connection_name = lookup_connection.get('name', '') if isinstance(lookup_connection, dict) else ''
            return self._generate_lookup_code(trans_name, input_df, output_df, logic, connection_name)
        
        elif trans_type == 'CONDITIONAL_SPLIT':
            return self._generate_conditional_split_code(trans_name, input_df, logic)
        
        elif trans_type == 'AGGREGATE':
            return self._generate_aggregate_code(trans_name, input_df, output_df, logic)
        
        # NEW: Handle CHECKSUM transformation
        elif trans_type == 'CHECKSUM':
            return self._generate_checksum_code(trans_name, input_df, output_df, logic)
        
        # NEW: Handle OLE_DB_COMMAND transformation
        elif trans_type == 'OLE_DB_COMMAND':
            return self._generate_oledb_command_code(trans_name, input_df, output_df, logic)
        
        # NEW: Handle MERGE transformation
        elif trans_type == 'MERGE' or trans_type == 'MERGE_JOIN':
            return self._generate_merge_code(trans_name, input_df, output_df, logic)
        
        # NEW: Handle RC_SELECT transformation
        elif trans_type == 'RC_SELECT':
            return self._generate_rc_select_code(trans_name, input_df, output_df)
        
        # NEW: Handle RC_INSERT transformation
        elif trans_type == 'RC_INSERT':
            return self._generate_rc_insert_code(trans_name, input_df, output_df)
        
        # NEW: Handle RC_UPDATE transformation
        elif trans_type == 'RC_UPDATE':
            return self._generate_rc_update_code(trans_name, input_df, output_df)
        
        # NEW: Handle RC_DELETE transformation
        elif trans_type == 'RC_DELETE':
            return self._generate_rc_delete_code(trans_name, input_df, output_df)
        
        # NEW: Handle RC_INTERMEDIATE transformation
        elif trans_type == 'RC_INTERMEDIATE':
            return self._generate_rc_intermediate_code(trans_name, input_df, output_df)
        
        # NEW: Handle TRASH_DESTINATION transformation
        elif trans_type == 'TRASH_DESTINATION':
            return self._generate_trash_destination_code(trans_name, input_df, output_df)
        
        else:
            if self.databricks_mode:
                return f'''
# Transformation: {trans_name} ({trans_type})
logger.info("Processing transformation: {trans_name}")
{output_df} = {input_df}
logger.info(f"Transformation processed: {{ {output_df}.count() }} rows")
'''
            else:
                return f'''
# Transformation: {trans_name} ({trans_type})
logger.info("Processing transformation: {trans_name}")
{output_df} = {input_df}
logger.info(f"Transformation processed: {{ {output_df}.count() }} rows")
'''
    
    def _generate_derived_column_code(self, trans_name: str, input_df: str, 
                                      output_df: str, logic: Any) -> str:
        """Generate derived column transformation code."""
        # Handle case where logic is a string instead of dict
        if isinstance(logic, str):
            return f'''
# Transformation: {trans_name} (Derived Column)
logger.info("Processing transformation: {trans_name}")
# Derived Column Logic: {logic}
{output_df} = {input_df}
# Note: Complex derived column expressions should be implemented in the LLM validator
'''
        
        # Extract column additions using expression translator
        columns = self.expression_translator.translate_derived_column(logic)
        
        if not columns:
            expressions_raw = logic.get('expressions', 'No expressions found') if isinstance(logic, dict) else str(logic)
            return f'''
# Transformation: {trans_name} (Derived Column)
# Expression: {expressions_raw}
# Note: Derived column logic will be refined by LLM validator
{output_df} = {input_df}
'''
        
        # Generate withColumn chain
        code_lines = [f'# Transformation: {trans_name} (Derived Column)']
        code_lines.append(f'{output_df} = {input_df} \\')
        
        for i, col_info in enumerate(columns):
            col_name = col_info['name']
            col_expr = col_info['expression']
            code_lines.append(f'    .withColumn("{col_name}", {col_expr}) \\')
        
        # Remove last backslash
        code_lines[-1] = code_lines[-1].rstrip(' \\')
        
        return '\n'.join(code_lines) + '\n'
    
    def _generate_lookup_code(self, trans_name: str, input_df: str,
                              output_df: str, logic: Dict[str, Any], connection_name: str = '') -> str:
        """Generate lookup (join) transformation code."""
        # Handle both dict and string logic (for backward compatibility)
        if isinstance(logic, str):
            # Fallback for old format
            lookup_query = ""
            join_type = "left"
            column_mappings = []
        else:
            lookup_info = self.expression_translator.translate_lookup_query(logic)
            lookup_query = lookup_info.get('lookup_query', '').strip()
            join_type = lookup_info.get('join_type', 'left')
            column_mappings = logic.get('column_mappings', [])
        
        # Apply schema mapping to lookup query if available
        if self.schema_mapper and self.schema_mapper.is_active() and lookup_query:
            original_query = lookup_query
            # Try with connection name first if provided
            if connection_name:
                lookup_query = self.schema_mapper.apply_mapping_to_sql(lookup_query, connection_name)
            # If connection name not provided or mapping didn't change, search all connections
            if not connection_name or lookup_query == original_query:
                lookup_query = self.schema_mapper.apply_mapping_to_sql(lookup_query, None)
        
        # Remove brackets from column names to convert SSIS format to Databricks format
        # This converts [ColumnName] to ColumnName in the SQL query
        if lookup_query:
            # Pattern: [identifier] - matches column names in brackets
            # Replace [ColumnName] with ColumnName
            column_bracket_pattern = r'\[([a-zA-Z_][a-zA-Z0-9_]*)\]'
            lookup_query = re.sub(column_bracket_pattern, r'\1', lookup_query)
        
        # If no query, generate a placeholder
        if not lookup_query:
            lookup_query = f"-- Lookup query for {trans_name}"
        
        lookup_df = f'{trans_name.lower().replace(" ", "_")}_lookup_df'
        lookup_df_var = lookup_df.replace(" ", "_")
        
        # Build join condition from column mappings
        join_conditions = []
        if column_mappings:
            for mapping in column_mappings:
                input_col = mapping.get('input_column', '')
                ref_col = mapping.get('join_to_reference_column', '')
                if input_col and ref_col:
                    join_conditions.append(f"{input_df}.{input_col} == {lookup_df_var}.{ref_col}")
        
        # Build join condition string
        if join_conditions:
            join_condition_str = " & ".join(join_conditions)
            join_code = f"""{output_df} = {input_df}.join(
    {lookup_df_var},
    {join_condition_str},
    "{join_type}"
)"""
        else:
            join_code = f"""# TODO: Specify join condition based on lookup column mappings
# Example: {output_df} = {input_df}.join({lookup_df_var}, {input_df}.id == {lookup_df_var}.id, "{join_type}")"""
        
        # Build output columns list (columns being copied from lookup)
        output_columns = []
        if column_mappings:
            for mapping in column_mappings:
                copy_col = mapping.get('copy_from_reference_column', '')
                input_col = mapping.get('input_column', '')
                if copy_col:
                    # Alias the lookup column to avoid conflicts
                    alias_col = copy_col
                    if copy_col == input_col:
                        alias_col = f"{copy_col}_lookup"
                    output_columns.append(f"'{copy_col}' as '{alias_col}'")
        
        selection_comment = ', '.join(output_columns) if output_columns else 'All columns'

        return f'''
# Transformation: {trans_name} (Lookup)
logger.info("Processing transformation: {trans_name}")

# Create lookup DataFrame from query
{lookup_df_var} = spark.sql("""
{lookup_query}
""")

# Perform {join_type} join
{join_code}

# Input columns plus lookup output columns: {selection_comment}
'''

    def _generate_sort_code(self, trans_name: str, input_df: str, output_df: str, logic: Any) -> str:
        """Generate sort transformation code."""
        sort_columns: List[str] = []

        if isinstance(logic, dict):
            # Preferred location if parser captured explicit sort columns
            sort_columns = logic.get('sort_columns', []) or []

            # Fallback: check expressions for column hints
            if not sort_columns:
                expressions = logic.get('expressions', [])
                for expr in expressions:
                    name = expr.get('name', '').lower()
                    value = expr.get('value')
                    if name.startswith('sortcolumn') and value:
                        sort_columns.append(value)

        comment_suffix = ""
        if sort_columns:
            sort_expr = ', '.join(f'col("{col}")' for col in sort_columns)
            order_stmt = f'{output_df} = {input_df}.orderBy({sort_expr})'
        else:
            # Default to all columns to maintain deterministic ordering; add guidance for refinement
            order_stmt = f'{output_df} = {input_df}.orderBy(*[col(c) for c in {input_df}.columns])'
            comment_suffix = "# TODO: Replace with explicit sort columns extracted from SSIS configuration."

        return f'''
# Transformation: {trans_name} (Sort)
logger.info("Processing transformation: {trans_name}")
{order_stmt}
{comment_suffix}
'''
    
    def _generate_conditional_split_code(self, trans_name: str, input_df: str,
                                         logic: Any) -> str:
        """Generate conditional split transformation code."""
        # Handle both dict and string logic formats
        if isinstance(logic, str):
            logic_dict = {'conditions': logic}
        else:
            logic_dict = logic
        
        # Extract conditions using expression translator
        conditions = self.expression_translator.translate_conditional_split(logic_dict)
        
        if not conditions:
            conditions_raw = logic_dict.get('conditions', str(logic) if isinstance(logic, str) else 'No conditions found')
            return f'''
# Transformation: {trans_name} (Conditional Split)
# Condition: {conditions_raw}
# TODO: Manual translation needed
'''
        
        code_lines = [f'# Transformation: {trans_name} (Conditional Split)']
        
        for cond_info in conditions:
            cond_name = cond_info['name']
            filter_expr = cond_info['filter_expression']
            # Sanitize output name for DataFrame variable - replace spaces, hyphens, commas, and other special chars
            safe_name = cond_name.replace(" ", "_").replace("-", "_").replace(",", "_")
            safe_name = safe_name.replace("(", "_").replace(")", "_").replace(".", "_")
            safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
            output_df = f'{input_df}_{safe_name}'
            
            code_lines.append(f'{output_df} = {input_df}.filter({filter_expr})')
        
        return '\n'.join(code_lines) + '\n'
    
    def _generate_aggregate_code(self, trans_name: str, input_df: str,
                                 output_df: str, logic: Dict[str, Any]) -> str:
        """Generate aggregate transformation code."""
        group_by_cols = logic.get('group_by_columns', [])
        aggregations = logic.get('aggregations', [])
        
        return f'''
# Transformation: {trans_name} (Aggregate)
# TODO: Specify group by columns and aggregations
{output_df} = {input_df}.groupBy(
    # GROUP_BY_COLUMNS_HERE
).agg(
    # AGGREGATIONS_HERE
)
'''
    
    def _get_or_create_df_name(self, component_id: str, component_name: str) -> str:
        """Get or create DataFrame variable name for a component."""
        if component_id in self.df_flow:
            return self.df_flow[component_id]
        
        # Create new DataFrame name - remove invalid characters
        safe_name = component_name.lower()
        safe_name = safe_name.replace(' ', '_').replace('-', '_')
        safe_name = safe_name.replace('(', '').replace(')', '')
        safe_name = safe_name.replace('[', '').replace(']', '')
        safe_name = safe_name.replace('.', '_').replace('/', '_')
        
        # Remove any remaining invalid characters
        safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
        
        df_name = f'{safe_name}_df'
        
        # Store mapping
        self.df_flow[component_id] = df_name
        
        return df_name
    
    def _generate_databricks_destination_code(self, dest: Dict[str, Any]) -> str:
        """Generate Databricks-optimized destination code."""
        table_name = dest.get('table_name', '')
        dest_name = dest.get('name', 'Unknown Destination')
        
        # Get connection name for schema mapping
        dest_connection = dest.get('connection', {})
        connection_name = dest_connection.get('name', '') if isinstance(dest_connection, dict) else ''
        
        # Get the last DataFrame variable name from the flow
        df_name = self._get_last_df_in_flow()
        
        # If table name is empty, try to extract from destination name
        if not table_name or table_name.strip() == '':
            # Use generic fallback - schema mapping will handle actual table names
            table_name = 'unknown_table'
        
        # Apply schema mapping if available
        if self.schema_mapper and self.schema_mapper.is_active() and connection_name:
            table_name = self.schema_mapper.get_databricks_table_name(connection_name, table_name)
        
        # Sanitize table name for Databricks compatibility
        sanitized_table_name = self._sanitize_sql_for_databricks(table_name)
        
        return f'''
# Destination: Write to Databricks table ({sanitized_table_name})
logger.info("Writing to destination: {sanitized_table_name}")

# Write data to destination table using Delta format
{df_name}.write.format("delta").mode("append").saveAsTable("{sanitized_table_name}")

logger.info("Data written successfully to {sanitized_table_name}")
'''
    
    def _generate_destination_code(self, dest: Dict[str, Any], mapped_connections: List[Dict[str, Any]] = None) -> str:
        """Generate destination writing code."""
        if self.databricks_mode:
            return self._generate_databricks_destination_code(dest)
        
        table_name = dest.get('table_name', '')
        dest_name = dest.get('name', 'Unknown Destination')
        
        # Get the last DataFrame variable name from the flow
        df_name = self._get_last_df_in_flow()
        
        # Get connection details if available
        dest_connection = dest.get('connection')
        conn_var_prefix = None
        
        if dest_connection and mapped_connections:
            conn_name = dest_connection.get('name', '')
            for mapped_conn in mapped_connections:
                if mapped_conn['name'] == conn_name:
                    conn_var_prefix = self._get_connection_var_name(mapped_conn)
                    break
        
        # If table name is empty, try to extract from destination name
        if not table_name or table_name.strip() == '':
            # Use generic fallback - schema mapping will handle actual table names
            table_name = 'unknown_table'
        
        if conn_var_prefix:
            return f'''
# Destination: {dest['name']}
# Write to: {table_name}
# Write data to destination
logger.info("Writing to destination: {table_name}")
{df_name}.write \\
    .format("jdbc") \\
    .option("url", {conn_var_prefix}_url) \\
    .option("dbtable", "{table_name}") \\
    .option("user", {conn_var_prefix}_user) \\
    .option("password", {conn_var_prefix}_password) \\
    .option("driver", {conn_var_prefix}_driver) \\
    .mode("append") \\
    .save()

logger.info("Data written successfully to {table_name}")
'''
        else:
            return f'''
# Destination: {dest['name']}
# Write to: {table_name}
# Write data to destination
logger.info("Writing to destination: {table_name}")
{df_name}.write \\
    .format("jdbc") \\
    .option("url", "jdbc:sqlserver://server:1433;databaseName=database") \\
    .option("dbtable", "{table_name}") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .mode("append") \\
    .save()

logger.info("Data written successfully to {table_name}")
'''
    
    def _get_last_df_in_flow(self) -> str:
        """Get the last DataFrame variable name in the current flow."""
        if self.df_flow:
            # Get the last entry in the DataFrame flow
            return list(self.df_flow.values())[-1]
        return "df"  # Fallback
    
    def _generate_transformation_code_enhanced(self, trans: Dict[str, Any], data_flow_paths: List[Dict[str, str]] = None) -> str:
        """Generate enhanced transformation code with proper DataFrame flow using paths."""
        trans_name = trans['name']
        trans_type = trans['type']
        logic = trans.get('logic', {})
        component_id = trans.get('component_id', '')
        
        # Get input DataFrame name from paths instead of just using last in flow
        input_df_name = self._get_input_dataframe_from_paths(component_id, data_flow_paths)
        
        if not input_df_name:
            # Fallback to last DataFrame in flow
            if self.df_flow:
                input_df_name = list(self.df_flow.values())[-1]
            else:
                # Fallback to a generic name
                input_df_name = 'source_df'
        
        # Generate output DataFrame name
        output_df_name = self._get_or_create_df_name(component_id, trans_name)
        
        # Store in flow tracking
        self.df_flow[component_id] = output_df_name
        
        # Handle different transformation types
        if trans_type == 'ROW_COUNT':
            return f'''
# Transformation: {trans_name} (Row Count)
logger.info("Processing transformation: {trans_name}")
{output_df_name} = {input_df_name}
# Add row count logging
row_count = {output_df_name}.count()
logger.info(f"Row count: {{row_count}}")
'''
        elif trans_type == 'RC_INSERT':
            return self._generate_rc_insert_code(trans_name, input_df_name, output_df_name)
        elif trans_type == 'RC_SELECT':
            return self._generate_rc_select_code(trans_name, input_df_name, output_df_name)
        elif trans_type == 'RC_UPDATE':
            return self._generate_rc_update_code(trans_name, input_df_name, output_df_name)
        elif trans_type == 'RC_DELETE':
            return self._generate_rc_delete_code(trans_name, input_df_name, output_df_name)
        elif trans_type == 'RC_INTERMEDIATE':
            return self._generate_rc_intermediate_code(trans_name, input_df_name, output_df_name)
        elif trans_type == 'TRASH_DESTINATION':
            return self._generate_trash_destination_code(trans_name, input_df_name, output_df_name)
        elif trans_type == 'MERGE':
            return self._generate_merge_code(trans_name, input_df_name, output_df_name, logic)
        elif trans_type == 'CHECKSUM':
            return self._generate_checksum_code(trans_name, input_df_name, output_df_name, logic)
        elif trans_type == 'OLE_DB_COMMAND':
            return self._generate_oledb_command_code(trans_name, input_df_name, output_df_name, logic, component_id, data_flow_paths)
        elif trans_type == 'CONDITIONAL_SPLIT':
            # Use actual conditional split logic from XML
            logic_str = trans.get('logic', 'No conditional logic found')
            expressions = trans.get('expressions', [])
            outputs = trans.get('outputs', [])
            
            # Track Conditional Split outputs for downstream OLE DB Commands
            conditional_outputs = {}
            # Handle both dict and string logic formats
            if isinstance(logic, str):
                logic_dict = {'conditions': logic}
            else:
                logic_dict = logic
            
            conditions = self.expression_translator.translate_conditional_split(logic_dict)
            # Map condition indices to output names
            output_name_map = {}
            if outputs:
                for i, output in enumerate(outputs):
                    output_name = output.get('name', f'condition_{i+1}')
                    if i < len(conditions):
                        output_name_map[conditions[i]['name']] = output_name
            
            for cond_info in conditions:
                cond_name = output_name_map.get(cond_info['name'], cond_info['name'])
                filter_expr = cond_info['filter_expression']
                # Sanitize output name for DataFrame variable - replace spaces, hyphens, commas, and other special chars
                safe_name = cond_name.replace(" ", "_").replace("-", "_").replace(",", "_")
                safe_name = safe_name.replace("(", "_").replace(")", "_").replace(".", "_")
                safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
                output_df = f'{input_df_name}_{safe_name}'
                conditional_outputs[cond_name] = output_df
            
            # Store for later use by OLE DB Commands
            if component_id:
                self.conditional_split_outputs[component_id] = conditional_outputs
            
            # Generate Conditional Split code
            conditional_code = self._generate_conditional_split_code(trans_name, input_df_name, logic)
            
            return f'''
# Transformation: {trans_name} (Conditional Split)
logger.info("Processing transformation: {trans_name}")
{conditional_code}
'''
        elif trans_type == 'LOOKUP':
            return self._generate_lookup_code(trans_name, input_df_name, output_df_name, logic)
        elif trans_type == 'SORT':
            return self._generate_sort_code(trans_name, input_df_name, output_df_name, logic)
        elif trans_type == 'DERIVED_COLUMN':
            return self._generate_derived_column_code(trans_name, input_df_name, output_df_name, logic)
        else:
            # Generic transformation with logic extraction
            logic_str = trans.get('logic', '')
            expressions = trans.get('expressions', [])
            outputs = trans.get('outputs', [])
            
            logic_comment = ""
            if logic_str and logic_str != f"Transformation type: {trans_type}":
                logic_comment = f"\n# Transformation Logic: {logic_str}"
            if expressions:
                logic_comment += "\n# Expressions/Properties:"
                for expr in expressions:
                    if expr.get('value'):
                        expr_name = expr.get('name', 'Unknown')
                        expr_value = expr.get('value', '')
                        # Truncate long values for readability
                        if len(expr_value) > 100:
                            expr_value = expr_value[:100] + "..."
                        logic_comment += f"\n#   - {expr_name}: {expr_value}"
            if outputs:
                logic_comment += "\n# Outputs:"
                for output in outputs:
                    if output.get('name'):
                        logic_comment += f"\n#   - {output.get('name', 'Unknown')}"
            
            # Use LLM fallback for generic transformations
            if self.use_llm_fallback and self.llm_generator:
                try:
                    llm_code = self._generate_llm_fallback_transformation_code(trans, input_df_name, output_df_name)
                    return f'''
# Transformation: {trans_name} ({trans_type})
logger.info("Processing transformation: {trans_name}"){logic_comment}
{llm_code}
'''
                except Exception as e:
                    logger.warning(f"LLM fallback failed for generic transformation {trans_name}: {e}")
            
            return f'''
# Transformation: {trans_name} ({trans_type})
logger.info("Processing transformation: {trans_name}"){logic_comment}
{output_df_name} = {input_df_name}
# TODO: Implement transformation logic for {trans_type}
# Note: This transformation type may need manual implementation based on the extracted logic above
'''
    
    def _get_input_dataframe_from_paths(self, component_id: str, data_flow_paths: List[Dict[str, str]] = None) -> Optional[str]:
        """
        Get the input DataFrame name by looking up paths to find what feeds this component.
        
        Args:
            component_id: The component ID to find inputs for
            data_flow_paths: List of path connections
            
        Returns:
            DataFrame variable name if found, None otherwise
        """
        if not data_flow_paths or not component_id:
            return None
        
        # Find all paths that end at this component
        input_paths = []
        for path in data_flow_paths:
            to_id = path.get('to_id', '')
            to_name = path.get('to', '')
            
            # Match by component_id first (preferred), then by name (backward compatibility)
            if to_id == component_id:
                input_paths.append(path)
            elif not to_id and to_name and component_id.endswith(to_name):
                # Fallback: match by component name if ID not available
                input_paths.append(path)
        
        if not input_paths:
            return None
        
        # For components with single input, return that DataFrame
        if len(input_paths) == 1:
            from_id = input_paths[0].get('from_id', '')
            from_name = input_paths[0].get('from', '')
            
            # Look up DataFrame name from flow tracking
            # Try component_id first (preferred), then name
            if from_id and from_id in self.df_flow:
                return self.df_flow[from_id]
            elif from_name:
                # Find component by name (backward compatibility)
                for cid, df_name in self.df_flow.items():
                    if cid.endswith(from_name) or from_name in cid:
                        return df_name
        
        # Multiple inputs - handled by specific transformation logic (like Merge Join)
        # For now, return the first input found
        # Special transformations like MERGE_JOIN will handle multiple inputs separately
        if input_paths:
            from_id = input_paths[0].get('from_id', '')
            from_name = input_paths[0].get('from', '')
            
            if from_id and from_id in self.df_flow:
                return self.df_flow[from_id]
            elif from_name:
                for cid, df_name in self.df_flow.items():
                    if cid.endswith(from_name) or from_name in cid:
                        return df_name
        
        return None
    
    def _generate_llm_fallback_transformation_code(self, trans: Dict[str, Any], input_df_name: str, output_df_name: str) -> str:
        """Generate PySpark code using LLM fallback for complex transformations."""
        if not self.use_llm_fallback or not self.llm_generator:
            return f"{output_df_name} = {input_df_name}"
        
        try:
            # Create a mock SSIS component for LLM processing
            from models import SSISComponent, PySparkMapping
            
            # Extract transformation details
            trans_name = trans['name']
            trans_type = trans['type']
            logic = trans.get('logic', '')
            expressions = trans.get('expressions', [])
            outputs = trans.get('outputs', [])
            component_class = trans.get('component_class', '')
            
            # Create SSIS component
            from models import TransformationType
            trans_type_enum = getattr(TransformationType, trans_type, TransformationType.GENERIC_TRANSFORMATION)
            
            component = SSISComponent(
                id=trans.get('component_id', ''),
                name=trans_name,
                type=trans_type_enum,
                properties={
                    'logic': logic,
                    'component_class': component_class,
                    'expressions': expressions,
                    'outputs': outputs
                },
                expressions={expr.get('name', 'expression'): expr.get('value', '') for expr in expressions if isinstance(expr, dict)}
            )
            
            # Create PySpark mapping
            mapping = PySparkMapping(
                ssis_component=component,
                pyspark_function=f"transform_{trans_type.lower()}",
                parameters={
                    'input_df': input_df_name,
                    'output_df': output_df_name,
                    'logic': logic,
                    'expressions': expressions,
                    'outputs': outputs
                },
                dependencies=['pyspark'],
                imports=['from pyspark.sql.functions import *']
            )
            
            # Generate code using LLM
            llm_code = self.llm_generator.generate_code_for_component(mapping)
            
            # Clean up and format the LLM-generated code
            if llm_code and llm_code.strip():
                # Remove any extra imports or boilerplate that might be duplicated
                lines = llm_code.strip().split('\n')
                filtered_lines = []
                for line in lines:
                    if not line.strip().startswith('from ') and not line.strip().startswith('import '):
                        filtered_lines.append(line)
                
                return '\n'.join(filtered_lines)
            else:
                return f"{output_df_name} = {input_df_name}"
                
        except Exception as e:
            logger.warning(f"LLM fallback failed for {trans_name}: {e}")
            return f"{output_df_name} = {input_df_name}"
    
    def _implement_conditional_split_logic(self, input_df_name: str, output_df_name: str, expressions: List[Dict], outputs: List[Dict]) -> str:
        """Implement actual conditional split logic in PySpark."""
        if not expressions:
            return f"{output_df_name} = {input_df_name}"
        
        code_lines = [f"{output_df_name} = {input_df_name}"]
        
        # Add conditional logic based on expressions
        for expr in expressions:
            if expr.get('name') == 'FriendlyExpression':
                condition = expr.get('value', '')
                if condition:
                    # Convert SSIS expression to PySpark filter
                    pyspark_condition = self._convert_ssis_expression_to_pyspark(condition)
                    code_lines.append(f"# Filter condition: {condition}")
                    code_lines.append(f"# PySpark equivalent: {pyspark_condition}")
                    
                    # Add actual PySpark filter implementation
                    # For now, we'll add a comment showing how to implement it
                    code_lines.append(f"# TODO: Implement filter: {output_df_name} = {output_df_name}.filter({pyspark_condition})")
        
        return "\n".join(code_lines)
    
    def _implement_derived_column_logic(self, input_df_name: str, output_df_name: str, expressions: List[Dict], logic_str: str = "") -> str:
        """Implement actual derived column logic in PySpark."""
        code_lines = []
        
        # For derived columns, we need to look at the logic string to get column names
        # The expressions contain the values, but we need to extract column names from the logic
        if logic_str and '=' in logic_str and logic_str != "No derived column logic found":
            # Parse logic like "DeletedFlag = 0 | TemplateFlag = 0"
            parts = logic_str.split('|')
            
            # Start with the input dataframe
            code_lines.append(f"{output_df_name} = {input_df_name}")
            
            # Add withColumn for each part
            for part in parts:
                part = part.strip()
                if '=' in part:
                    column_name, column_value = part.split('=', 1)
                    column_name = column_name.strip()
                    column_value = column_value.strip()
                    
                    # Convert to PySpark withColumn
                    if column_value.isdigit():
                        code_lines.append(f"{output_df_name} = {output_df_name}.withColumn(\"{column_name}\", lit({column_value}))")
                    elif column_value.startswith('!'):
                        # Handle negation like !ISNULL(column)
                        code_lines.append(f"# TODO: Implement expression: {column_name} = {column_value}")
                        code_lines.append(f"{output_df_name} = {output_df_name}.withColumn(\"{column_name}\", lit(0))  # Placeholder")
                    elif '==' in column_value or '!=' in column_value:
                        # Handle comparison like CheckSum_New == CheckSum_OLD
                        code_lines.append(f"# TODO: Implement comparison: {column_name} = {column_value}")
                        code_lines.append(f"{output_df_name} = {output_df_name}.withColumn(\"{column_name}\", lit(False))  # Placeholder")
                    elif 'ISNULL' in column_value:
                        # Handle ISNULL function
                        code_lines.append(f"# TODO: Implement ISNULL: {column_name} = {column_value}")
                        code_lines.append(f"{output_df_name} = {output_df_name}.withColumn(\"{column_name}\", lit(0))  # Placeholder")
                    elif '@[' in column_value:
                        # Handle variable like @[User::TaskWorkHistoryID]
                        code_lines.append(f"# TODO: Implement variable: {column_name} = {column_value}")
                        code_lines.append(f"{output_df_name} = {output_df_name}.withColumn(\"{column_name}\", lit(0))  # Placeholder")
                    else:
                        code_lines.append(f"{output_df_name} = {output_df_name}.withColumn(\"{column_name}\", lit(\"{column_value}\"))")
        else:
            # No logic found, just pass through
            code_lines.append(f"{output_df_name} = {input_df_name}")
        
        return "\n".join(code_lines)
    
    def _convert_ssis_expression_to_pyspark(self, ssis_expr: str) -> str:
        """Convert SSIS expression to PySpark filter condition."""
        # Basic conversion - in a real implementation, this would be more sophisticated
        pyspark_expr = ssis_expr.replace('==', '==')  # Keep == as is
        pyspark_expr = pyspark_expr.replace('&&', '&')  # Convert && to &
        pyspark_expr = pyspark_expr.replace('||', '|')  # Convert || to |
        pyspark_expr = pyspark_expr.replace('TRUE', 'True')  # Convert TRUE to True
        pyspark_expr = pyspark_expr.replace('FALSE', 'False')  # Convert FALSE to False
        
        return f"df.filter({pyspark_expr})"
    
    def _generate_execution_flow_code(self, execution_flow: List[Dict[str, Any]]) -> str:
        """Generate execution flow documentation."""
        lines = ["# Execution Flow"]
        lines.append("# The following dependencies were identified:")
        
        for link in execution_flow:
            lines.append(f"#   - {link['from']} -> {link['to']}")
        
        return '\n'.join(lines)
    
    def _generate_footer(self) -> str:
        """Generate code footer."""
        if self.databricks_mode:
            return '''logger.info("Process completed successfully")'''
        else:
            return '''
# Stop Spark Session
logger.info("Process completed successfully")
# spark.stop()  # Uncomment if you want to stop the session
'''
    
    # NEW: Transformation code generators for Databricks
    def _generate_checksum_code(self, trans_name: str, input_df: str, output_df: str, logic: Any) -> str:
        """Generate CHECKSUM transformation code."""
        return f'''
# Transformation: {trans_name} (CHECKSUM)
logger.info("Processing transformation: {trans_name}")
# Calculate checksum/hash for detecting data changes
from pyspark.sql.functions import hash, col
{output_df} = {input_df}.withColumn("CheckSum", hash(*[col for col in {input_df}.columns]))
row_count = {output_df}.count()
logger.info(f"CHECKSUM calculated: {{row_count}} rows")
'''
    
    def _generate_oledb_command_code(self, trans_name: str, input_df: str, output_df: str, logic: Any, component_id: str = '', data_flow_paths: List[Dict[str, str]] = None) -> str:
        """Generate OLE_DB_COMMAND transformation code with parameter mappings and Conditional Split if statements."""
        sql_command = ""
        input_columns = []
        
        # Handle both dict and string logic
        if isinstance(logic, dict):
            sql_command = logic.get('sql_command', '').strip()
            input_columns = logic.get('input_columns', [])
        elif isinstance(logic, str):
            sql_command = logic.strip()
        
        # Check if input comes from Conditional Split output (may be through Row Count components)
        conditional_split_df = None
        if data_flow_paths and component_id and hasattr(self, 'conditional_split_outputs'):
            # Trace back through paths to find Conditional Split output
            # Path chain: CSPL → RC → CMD (we need to find the CSPL output)
            current_to = trans_name
            found_cs_path = None
            max_iterations = 5  # Prevent infinite loops
            iteration = 0
            
            while iteration < max_iterations:
                # Find path where current component is the destination
                for path in data_flow_paths:
                    path_to_name = path.get('to', '')
                    # Match by component name (paths use component names, not IDs)
                    if current_to in path_to_name or (component_id and component_id.split('\\')[-1] in path_to_name):
                        from_name = path.get('from', '')
                        path_name = path.get('path_name', '')
                        
                        # Check if from_component is a Conditional Split
                        for cs_id, cs_outputs in self.conditional_split_outputs.items():
                            cs_name = cs_id.split('\\')[-1]  # Extract component name from ID
                            if from_name == cs_name or cs_id.endswith(from_name):
                                # Found Conditional Split! Match output by path name
                                path_name_upper = path_name.upper()
                                for output_name, df_name in cs_outputs.items():
                                    output_upper = output_name.upper()
                                    # Match path name to output name
                                    if (output_upper in path_name_upper or 
                                        'UPDATE' in path_name_upper and 'UPDATE' in output_upper or
                                        'DELETE' in path_name_upper and 'DELETE' in output_upper or
                                        any(keyword in path_name_upper for keyword in output_name.split(' '))):
                                        conditional_split_df = df_name
                                        found_cs_path = path_name
                                        break
                            if conditional_split_df:
                                break
                        
                        if conditional_split_df:
                            break
                        
                        # If not found, check if from_name is a Row Count - trace back further
                        if 'RC' in from_name or 'Row Count' in from_name:
                            # Find the path that leads TO this Row Count FROM Conditional Split
                            for cs_path in data_flow_paths:
                                if cs_path.get('to', '') == from_name:
                                    cs_from = cs_path.get('from', '')
                                    cs_path_name = cs_path.get('path_name', '')
                                    # Check if this path comes from a Conditional Split we've tracked
                                    for cs_id, cs_outputs in self.conditional_split_outputs.items():
                                        cs_name = cs_id.split('\\')[-1]
                                        if cs_from == cs_name:
                                            # Match output by path name
                                            cs_path_name_upper = cs_path_name.upper()
                                            for output_name, df_name in cs_outputs.items():
                                                output_upper = output_name.upper()
                                                if (output_upper in cs_path_name_upper or 
                                                    'UPDATE' in cs_path_name_upper and 'UPDATE' in output_upper or
                                                    'DELETE' in cs_path_name_upper and 'DELETE' in output_upper):
                                                    conditional_split_df = df_name
                                                    break
                                            if conditional_split_df:
                                                break
                                    if conditional_split_df:
                                        break
                            break
                        else:
                            # Not a Row Count, stop searching
                            break
                
                if conditional_split_df or iteration >= max_iterations:
                    break
                iteration += 1
        
        if sql_command and sql_command != "OLE DB command transformation":
            # Build parameter mapping comments (simplified - one line per parameter)
            param_comments = ""
            if input_columns:
                for col_info in input_columns:
                    col_name = col_info.get('column_name', '')
                    param_name = col_info.get('parameter_name', '')
                    if param_name:
                        param_comments += f"\n# {param_name} = {col_name}"
                    else:
                        param_comments += f"\n# ? = {col_name}"
            
            # Generate if statement if input comes from Conditional Split
            if conditional_split_df:
                return f'''
if {conditional_split_df}.count() > 0:
    # OLE DB Command: {trans_name}
    # {sql_command}{param_comments}
    {output_df} = {conditional_split_df}
'''
            else:
                return f'''
# OLE DB Command: {trans_name}
# {sql_command}{param_comments}
{output_df} = {input_df}
'''
        
        # Default fallback
        if conditional_split_df:
            return f'''
if {conditional_split_df}.count() > 0:
    # OLE DB Command: {trans_name}
    {output_df} = {conditional_split_df}
'''
        else:
            return f'''
# OLE DB Command: {trans_name}
{output_df} = {input_df}
'''
    
    def _generate_merge_code(self, trans_name: str, input_df: str, output_df: str, logic: Any) -> str:
        """Generate MERGE (join) transformation code."""
        
        # Handle both dict and string logic (for backward compatibility)
        if isinstance(logic, str):
            return f'''
# Transformation: {trans_name} (MERGE)
logger.info("Processing transformation: {trans_name}")
# Merge/Join operation - pass through for now (join logic to be specified)
{output_df} = {input_df}
row_count = {output_df}.count()
logger.info(f"MERGE processed: {{row_count}} rows")
'''
        
        # Extract join information from logic
        join_type = logic.get('join_type', 'inner')
        join_conditions = logic.get('join_conditions', [])
        
        # Get source component names to find input DataFrames
        left_sources = logic.get('left_source_components', [])
        right_sources = logic.get('right_source_components', [])
        
        # Build DataFrame variable names from source component names
        # Try to infer DataFrame names from component names
        left_df_var = None
        right_df_var = None
        
        if left_sources:
            # Use first source component to infer DataFrame name
            left_comp = left_sources[0]
            # Convert component name to DataFrame variable name
            safe_name = left_comp.lower().replace(' ', '_').replace('.', '_').replace('-', '_')
            safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
            left_df_var = f'df_{safe_name}'
        
        if right_sources:
            right_comp = right_sources[0]
            safe_name = right_comp.lower().replace(' ', '_').replace('.', '_').replace('-', '_')
            safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
            right_df_var = f'df_{safe_name}'
        
        # Build join conditions
        if join_conditions and left_df_var and right_df_var:
            join_cond_strs = []
            for cond in join_conditions:
                left_col = cond.get('left_column', '')
                right_col = cond.get('right_column', '')
                
                if left_col and right_col:
                    # Build join condition - if columns have same name, still need to specify both sides
                    join_cond_strs.append(f"{left_df_var}.{left_col} == {right_df_var}.{right_col}")
            
            join_condition_str = " & ".join(join_cond_strs)
            
            return f'''
# Transformation: {trans_name} (MERGE)
# Merge Join: {join_type} join on {len(join_conditions)} key column(s)
logger.info("Processing transformation: {trans_name}")

# Perform {join_type} join
{output_df} = {left_df_var}.join(
    {right_df_var},
    ({join_condition_str}),
    "{join_type}"
)

row_count = {output_df}.count()
logger.info(f"MERGE join completed: {{row_count}} rows")
'''
        elif join_conditions:
            # Have join conditions but missing DataFrame names - provide detailed template
            join_cols_str = ", ".join([f"{c['left_column']}=={c['right_column']}" for c in join_conditions])
            return f'''
# Transformation: {trans_name} (MERGE)
# Merge Join: {join_type} join on columns: {join_cols_str}
logger.info("Processing transformation: {trans_name}")

# TODO: Specify left and right DataFrames
# Left source components: {left_sources}
# Right source components: {right_sources}
# Expected join condition: {" & ".join([f"left.{c['left_column']} == right.{c['right_column']}" for c in join_conditions])}

# Example:
# {output_df} = df_left_source.join(
#     df_right_source,
#     {" & ".join([f"df_left_source.{c['left_column']} == df_right_source.{c['right_column']}" for c in join_conditions])},
#     "{join_type}"
# )

{output_df} = {input_df}  # Placeholder - needs two input DataFrames
row_count = {output_df}.count()
logger.info(f"MERGE processed: {{row_count}} rows")
'''
        else:
            # Partial information - provide basic template
            return f'''
# Transformation: {trans_name} (MERGE)
# Merge Join: {join_type} join
logger.info("Processing transformation: {trans_name}")

# TODO: Specify left and right DataFrames and join condition
# Left sources: {left_sources if left_sources else "To be determined"}
# Right sources: {right_sources if right_sources else "To be determined"}

{output_df} = {input_df}  # Placeholder - needs two input DataFrames
row_count = {output_df}.count()
logger.info(f"MERGE processed: {{row_count}} rows")
'''
    
    def _generate_rc_select_code(self, trans_name: str, input_df: str, output_df: str) -> str:
        """Generate RC_SELECT (row count) transformation code."""
        return f'''
# Transformation: {trans_name} (RC_SELECT)
logger.info("Processing transformation: {trans_name}")
row_count = {input_df}.count()
logger.info(f"RC SELECT - {{row_count}} rows")
{output_df} = {input_df}
'''
    
    def _generate_rc_insert_code(self, trans_name: str, input_df: str, output_df: str) -> str:
        """Generate RC_INSERT (row count) transformation code."""
        return f'''
# Transformation: {trans_name} (RC_INSERT)
logger.info("Processing transformation: {trans_name}")
row_count = {input_df}.count()
logger.info(f"RC INSERT - {{row_count}} rows to be inserted")
{output_df} = {input_df}
'''
    
    def _generate_rc_update_code(self, trans_name: str, input_df: str, output_df: str) -> str:
        """Generate RC_UPDATE (row count) transformation code."""
        return f'''
# Transformation: {trans_name} (RC_UPDATE)
logger.info("Processing transformation: {trans_name}")
row_count = {input_df}.count()
logger.info(f"RC UPDATE - {{row_count}} rows to be updated")
{output_df} = {input_df}
'''
    
    def _generate_rc_delete_code(self, trans_name: str, input_df: str, output_df: str) -> str:
        """Generate RC_DELETE (row count) transformation code."""
        return f'''
# Transformation: {trans_name} (RC_DELETE)
logger.info("Processing transformation: {trans_name}")
row_count = {input_df}.count()
logger.info(f"RC DELETE - {{row_count}} rows to be deleted")
{output_df} = {input_df}
'''
    
    def _generate_rc_intermediate_code(self, trans_name: str, input_df: str, output_df: str) -> str:
        """Generate RC_INTERMEDIATE (row count) transformation code."""
        return f'''
# Transformation: {trans_name} (RC_INTERMEDIATE)
logger.info("Processing transformation: {trans_name}")
row_count = {input_df}.count()
logger.info(f"RC INTERMEDIATE - {{row_count}} rows")
{output_df} = {input_df}
'''
    
    def _generate_trash_destination_code(self, trans_name: str, input_df: str, output_df: str) -> str:
        """Generate TRASH_DESTINATION transformation code."""
        return f'''
# Transformation: {trans_name} (TRASH_DESTINATION)
logger.info("Processing transformation: {trans_name}")
# Trash Destination - data path terminates here
row_count = {input_df}.count()
logger.info(f"Data discarded via TRASH DESTINATION: {{row_count}} rows terminated")
{output_df} = {input_df}
'''
    
    def _generate_checksum_code(self, trans_name: str, input_df: str, output_df: str, logic: Any) -> str:
        """Generate CHECKSUM transformation code."""
        return f'''
# Transformation: {trans_name} (CHECKSUM)
logger.info("Processing transformation: {trans_name}")
# Calculate checksum/hash for detecting data changes
from pyspark.sql.functions import hash, col
{output_df} = {input_df}.withColumn("CheckSum", hash(*[col(c) for c in {input_df}.columns]))
row_count = {output_df}.count()
logger.info(f"CHECKSUM calculated: {{row_count}} rows")
'''
    
    def _generate_oledb_command_code(self, trans_name: str, input_df: str, output_df: str, logic: Any, component_id: str = '', data_flow_paths: List[Dict[str, str]] = None) -> str:
        """Generate OLE_DB_COMMAND transformation code with parameter mappings and Conditional Split if statements."""
        sql_command = ""
        input_columns = []
        
        # Handle both dict and string logic
        if isinstance(logic, dict):
            sql_command = logic.get('sql_command', '').strip()
            input_columns = logic.get('input_columns', [])
        elif isinstance(logic, str):
            sql_command = logic.strip()
        
        # Check if input comes from Conditional Split output (may be through Row Count components)
        conditional_split_df = None
        if data_flow_paths and component_id and hasattr(self, 'conditional_split_outputs'):
            # Trace back through paths to find Conditional Split output
            # Path chain: CSPL → RC → CMD (we need to find the CSPL output)
            current_to = trans_name
            found_cs_path = None
            max_iterations = 5  # Prevent infinite loops
            iteration = 0
            
            while iteration < max_iterations:
                # Find path where current component is the destination
                for path in data_flow_paths:
                    path_to_name = path.get('to', '')
                    # Match by component name (paths use component names, not IDs)
                    if current_to in path_to_name or (component_id and component_id.split('\\')[-1] in path_to_name):
                        from_name = path.get('from', '')
                        path_name = path.get('path_name', '')
                        
                        # Check if from_component is a Conditional Split
                        for cs_id, cs_outputs in self.conditional_split_outputs.items():
                            cs_name = cs_id.split('\\')[-1]  # Extract component name from ID
                            if from_name == cs_name or cs_id.endswith(from_name):
                                # Found Conditional Split! Match output by path name
                                path_name_upper = path_name.upper()
                                for output_name, df_name in cs_outputs.items():
                                    output_upper = output_name.upper()
                                    # Match path name to output name
                                    if (output_upper in path_name_upper or 
                                        'UPDATE' in path_name_upper and 'UPDATE' in output_upper or
                                        'DELETE' in path_name_upper and 'DELETE' in output_upper or
                                        any(keyword in path_name_upper for keyword in output_name.split(' '))):
                                        conditional_split_df = df_name
                                        found_cs_path = path_name
                                        break
                            if conditional_split_df:
                                break
                        
                        if conditional_split_df:
                            break
                        
                        # If not found, check if from_name is a Row Count - trace back further
                        if 'RC' in from_name or 'Row Count' in from_name:
                            # Find the path that leads TO this Row Count FROM Conditional Split
                            for cs_path in data_flow_paths:
                                if cs_path.get('to', '') == from_name:
                                    cs_from = cs_path.get('from', '')
                                    cs_path_name = cs_path.get('path_name', '')
                                    # Check if this path comes from a Conditional Split we've tracked
                                    for cs_id, cs_outputs in self.conditional_split_outputs.items():
                                        cs_name = cs_id.split('\\')[-1]
                                        if cs_from == cs_name:
                                            # Match output by path name
                                            cs_path_name_upper = cs_path_name.upper()
                                            for output_name, df_name in cs_outputs.items():
                                                output_upper = output_name.upper()
                                                if (output_upper in cs_path_name_upper or 
                                                    'UPDATE' in cs_path_name_upper and 'UPDATE' in output_upper or
                                                    'DELETE' in cs_path_name_upper and 'DELETE' in output_upper):
                                                    conditional_split_df = df_name
                                                    break
                                            if conditional_split_df:
                                                break
                                    if conditional_split_df:
                                        break
                            break
                        else:
                            # Not a Row Count, stop searching
                            break
                
                if conditional_split_df or iteration >= max_iterations:
                    break
                iteration += 1
        
        if sql_command and sql_command != "OLE DB command transformation":
            # Build parameter mapping comments (simplified - one line per parameter)
            param_comments = ""
            if input_columns:
                for col_info in input_columns:
                    col_name = col_info.get('column_name', '')
                    param_name = col_info.get('parameter_name', '')
                    if param_name:
                        param_comments += f"\n# {param_name} = {col_name}"
                    else:
                        param_comments += f"\n# ? = {col_name}"
            
            # Generate if statement if input comes from Conditional Split
            if conditional_split_df:
                return f'''
if {conditional_split_df}.count() > 0:
    # OLE DB Command: {trans_name}
    # {sql_command}{param_comments}
    {output_df} = {conditional_split_df}
'''
            else:
                return f'''
# OLE DB Command: {trans_name}
# {sql_command}{param_comments}
{output_df} = {input_df}
'''
        
        # Default fallback
        if conditional_split_df:
            return f'''
if {conditional_split_df}.count() > 0:
    # OLE DB Command: {trans_name}
    {output_df} = {conditional_split_df}
'''
        else:
            return f'''
# OLE DB Command: {trans_name}
{output_df} = {input_df}
'''
    
    def _generate_merge_code(self, trans_name: str, input_df: str, output_df: str, logic: Any) -> str:
        """Generate MERGE (join) transformation code."""
        
        # Handle both dict and string logic (for backward compatibility)
        if isinstance(logic, str):
            return f'''
# Transformation: {trans_name} (MERGE)
logger.info("Processing transformation: {trans_name}")
# Merge/Join operation - pass through for now (join logic to be specified)
{output_df} = {input_df}
row_count = {output_df}.count()
logger.info(f"MERGE processed: {{row_count}} rows")
'''
        
        # Extract join information from logic
        join_type = logic.get('join_type', 'inner')
        join_conditions = logic.get('join_conditions', [])
        
        # Get source component names to find input DataFrames
        left_sources = logic.get('left_source_components', [])
        right_sources = logic.get('right_source_components', [])
        
        # Build DataFrame variable names from source component names
        # Try to infer DataFrame names from component names
        left_df_var = None
        right_df_var = None
        
        if left_sources:
            # Use first source component to infer DataFrame name
            left_comp = left_sources[0]
            # Convert component name to DataFrame variable name
            safe_name = left_comp.lower().replace(' ', '_').replace('.', '_').replace('-', '_')
            safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
            left_df_var = f'df_{safe_name}'
        
        if right_sources:
            right_comp = right_sources[0]
            safe_name = right_comp.lower().replace(' ', '_').replace('.', '_').replace('-', '_')
            safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
            right_df_var = f'df_{safe_name}'
        
        # Build join conditions
        if join_conditions and left_df_var and right_df_var:
            join_cond_strs = []
            for cond in join_conditions:
                left_col = cond.get('left_column', '')
                right_col = cond.get('right_column', '')
                
                if left_col and right_col:
                    # Build join condition - if columns have same name, still need to specify both sides
                    join_cond_strs.append(f"{left_df_var}.{left_col} == {right_df_var}.{right_col}")
            
            join_condition_str = " & ".join(join_cond_strs)
            
            return f'''
# Transformation: {trans_name} (MERGE)
# Merge Join: {join_type} join on {len(join_conditions)} key column(s)
logger.info("Processing transformation: {trans_name}")

# Perform {join_type} join
{output_df} = {left_df_var}.join(
    {right_df_var},
    ({join_condition_str}),
    "{join_type}"
)

row_count = {output_df}.count()
logger.info(f"MERGE join completed: {{row_count}} rows")
'''
        elif join_conditions:
            # Have join conditions but missing DataFrame names - provide detailed template
            join_cols_str = ", ".join([f"{c['left_column']}=={c['right_column']}" for c in join_conditions])
            return f'''
# Transformation: {trans_name} (MERGE)
# Merge Join: {join_type} join on columns: {join_cols_str}
logger.info("Processing transformation: {trans_name}")

# TODO: Specify left and right DataFrames
# Left source components: {left_sources}
# Right source components: {right_sources}
# Expected join condition: {" & ".join([f"left.{c['left_column']} == right.{c['right_column']}" for c in join_conditions])}

# Example:
# {output_df} = df_left_source.join(
#     df_right_source,
#     {" & ".join([f"df_left_source.{c['left_column']} == df_right_source.{c['right_column']}" for c in join_conditions])},
#     "{join_type}"
# )

{output_df} = {input_df}  # Placeholder - needs two input DataFrames
row_count = {output_df}.count()
logger.info(f"MERGE processed: {{row_count}} rows")
'''
        else:
            # Partial information - provide basic template
            return f'''
# Transformation: {trans_name} (MERGE)
# Merge Join: {join_type} join
logger.info("Processing transformation: {trans_name}")

# TODO: Specify left and right DataFrames and join condition
# Left sources: {left_sources if left_sources else "To be determined"}
# Right sources: {right_sources if right_sources else "To be determined"}

{output_df} = {input_df}  # Placeholder - needs two input DataFrames
row_count = {output_df}.count()
logger.info(f"MERGE processed: {{row_count}} rows")
'''
    
    def _calculate_statistics(self,
                              mapped_connections: List[Dict[str, Any]],
                              mapped_sql_tasks: List[Dict[str, Any]],
                              mapped_data_flows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate mapping statistics."""
        total_sources = sum(len(df['sources']) for df in mapped_data_flows)
        total_transformations = sum(len(df['transformations']) for df in mapped_data_flows)
        total_destinations = sum(len(df['destinations']) for df in mapped_data_flows)
        
        mapped_sources = sum(
            sum(1 for s in df['sources'] if s['mapped'])
            for df in mapped_data_flows
        )
        mapped_transformations = sum(
            sum(1 for t in df['transformations'] if t['mapped'])
            for df in mapped_data_flows
        )
        mapped_destinations = sum(
            sum(1 for d in df['destinations'] if d['mapped'])
            for df in mapped_data_flows
        )
        
        return {
            'total_connections': len(mapped_connections),
            'mapped_connections': sum(1 for c in mapped_connections if c['mapped']),
            'total_sql_tasks': len(mapped_sql_tasks),
            'mapped_sql_tasks': sum(1 for t in mapped_sql_tasks if t['mapped']),
            'total_sources': total_sources,
            'mapped_sources': mapped_sources,
            'total_transformations': total_transformations,
            'mapped_transformations': mapped_transformations,
            'total_destinations': total_destinations,
            'mapped_destinations': mapped_destinations,
            'overall_mapping_rate': self._calculate_mapping_rate(
                mapped_sources + mapped_transformations + mapped_destinations + len(mapped_sql_tasks),
                total_sources + total_transformations + total_destinations + len(mapped_sql_tasks)
            )
        }
    
    def _calculate_mapping_rate(self, mapped: int, total: int) -> float:
        """Calculate mapping success rate."""
        if total == 0:
            return 100.0
        return round((mapped / total) * 100, 2)

