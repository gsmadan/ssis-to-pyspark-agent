"""
Control flow mapping utilities for SSIS to PySpark conversion.
"""
from typing import Dict, List, Any, Optional
import logging
from models import ControlFlow, SSISComponent, PySparkMapping
from .component_mapper import ComponentMapper

logger = logging.getLogger(__name__)


class ControlFlowMapper:
    """Maps SSIS control flow to PySpark execution logic."""
    
    def __init__(self, use_llm_fallback: bool = True):
        self.component_mapper = ComponentMapper(use_llm_fallback=use_llm_fallback)
    
    def map_control_flow(self, control_flow: ControlFlow) -> Dict[str, Any]:
        """
        Map SSIS control flow to PySpark execution logic.
        
        Args:
            control_flow: SSIS control flow to map
            
        Returns:
            Dictionary containing PySpark mapping for the control flow
        """
        try:
            logger.info("Mapping control flow")
            
            # Map tasks
            task_mappings = self._map_tasks(control_flow.tasks)
            
            # Map precedence constraints
            constraint_mappings = self._map_precedence_constraints(control_flow.precedence_constraints)
            
            # Map variables
            variable_mappings = self._map_variables(control_flow.variables)
            
            # Map connection managers
            connection_mappings = self._map_connection_managers(control_flow.connection_managers)
            
            # Generate execution structure
            execution_structure = self._generate_execution_structure(
                task_mappings,
                constraint_mappings,
                variable_mappings,
                connection_mappings
            )
            
            return {
                'tasks': task_mappings,
                'precedence_constraints': constraint_mappings,
                'variables': variable_mappings,
                'connection_managers': connection_mappings,
                'execution_structure': execution_structure,
                'imports': self._collect_imports(task_mappings),
                'dependencies': self._collect_dependencies(task_mappings)
            }
            
        except Exception as e:
            logger.error(f"Error mapping control flow: {e}")
            raise
    
    def _map_tasks(self, tasks: List[SSISComponent]) -> List[PySparkMapping]:
        """Map control flow tasks to PySpark operations."""
        task_mappings = []
        
        for task in tasks:
            try:
                mapping = self.component_mapper.map_component(task)
                task_mappings.append(mapping)
                logger.debug(f"Mapped task: {task.id}")
            except Exception as e:
                logger.error(f"Error mapping task {task.id}: {e}")
                # Add fallback mapping
                task_mappings.append(self._create_fallback_task_mapping(task))
        
        return task_mappings
    
    def _map_precedence_constraints(self, constraints: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Map precedence constraints to execution order."""
        constraint_mappings = []
        
        for constraint in constraints:
            try:
                constraint_mapping = {
                    'from_task': constraint.get('from', ''),
                    'to_task': constraint.get('to', ''),
                    'condition': constraint.get('value', 'Success'),
                    'logical_and': constraint.get('logical_and', 'True'),
                    'pyspark_equivalent': self._map_constraint_to_pyspark(constraint)
                }
                constraint_mappings.append(constraint_mapping)
                logger.debug(f"Mapped constraint: {constraint.get('from', '')} -> {constraint.get('to', '')}")
            except Exception as e:
                logger.error(f"Error mapping constraint: {e}")
        
        return constraint_mappings
    
    def _map_variables(self, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Map SSIS variables to Python variables."""
        variable_mappings = {}
        
        for var_name, var_info in variables.items():
            try:
                variable_mappings[var_name] = {
                    'name': var_name,
                    'type': var_info.get('type', 'String'),
                    'value': var_info.get('value', ''),
                    'namespace': var_info.get('namespace', 'User'),
                    'python_equivalent': self._map_variable_to_python(var_info)
                }
                logger.debug(f"Mapped variable: {var_name}")
            except Exception as e:
                logger.error(f"Error mapping variable {var_name}: {e}")
        
        return variable_mappings
    
    def _map_connection_managers(self, connections: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Map connection managers to PySpark connection configurations."""
        connection_mappings = {}
        
        for conn_id, conn_info in connections.items():
            try:
                connection_mappings[conn_id] = {
                    'id': conn_id,
                    'type': conn_info.get('type', ''),
                    'properties': conn_info.get('properties', {}),
                    'pyspark_equivalent': self._map_connection_to_pyspark(conn_info)
                }
                logger.debug(f"Mapped connection: {conn_id}")
            except Exception as e:
                logger.error(f"Error mapping connection {conn_id}: {e}")
        
        return connection_mappings
    
    def _map_constraint_to_pyspark(self, constraint: Dict[str, str]) -> str:
        """Map precedence constraint to PySpark execution logic."""
        condition = constraint.get('value', 'Success')
        
        if condition == 'Success':
            return "if previous_task_success:"
        elif condition == 'Failure':
            return "if previous_task_failed:"
        elif condition == 'Completion':
            return "if previous_task_completed:"
        else:
            return f"if {condition.lower()}:"
    
    def _map_variable_to_python(self, var_info: Dict[str, Any]) -> str:
        """Map SSIS variable to Python variable."""
        var_type = var_info.get('type', 'String')
        var_value = var_info.get('value', '')
        
        # Map SSIS data types to Python types
        type_mapping = {
            'String': 'str',
            'Int32': 'int',
            'Int64': 'int',
            'Boolean': 'bool',
            'DateTime': 'datetime',
            'Double': 'float',
            'Decimal': 'decimal.Decimal'
        }
        
        python_type = type_mapping.get(var_type, 'str')
        
        if python_type == 'str':
            return f'"{var_value}"'
        elif python_type == 'bool':
            return 'True' if var_value.lower() == 'true' else 'False'
        elif python_type in ['int', 'float']:
            return str(var_value) if var_value else '0'
        else:
            return f'"{var_value}"'
    
    def _map_connection_to_pyspark(self, conn_info: Dict[str, Any]) -> Dict[str, Any]:
        """Map connection manager to PySpark connection configuration."""
        conn_type = conn_info.get('type', '')
        properties = conn_info.get('properties', {})
        
        if 'SqlServer' in conn_type:
            return {
                'type': 'jdbc',
                'url': properties.get('ConnectionString', ''),
                'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            }
        elif 'Oracle' in conn_type:
            return {
                'type': 'jdbc',
                'url': properties.get('ConnectionString', ''),
                'driver': 'oracle.jdbc.driver.OracleDriver'
            }
        elif 'MySQL' in conn_type:
            return {
                'type': 'jdbc',
                'url': properties.get('ConnectionString', ''),
                'driver': 'com.mysql.jdbc.Driver'
            }
        else:
            return {
                'type': 'generic',
                'properties': properties
            }
    
    def _generate_execution_structure(self,
                                    task_mappings: List[PySparkMapping],
                                    constraint_mappings: List[Dict[str, Any]],
                                    variable_mappings: Dict[str, Any],
                                    connection_mappings: Dict[str, Any]) -> Dict[str, Any]:
        """Generate PySpark execution structure."""
        
        structure = {
            'initialization': self._generate_initialization_code(variable_mappings, connection_mappings),
            'task_execution': self._generate_task_execution_code(task_mappings),
            'error_handling': self._generate_error_handling_code(),
            'cleanup': self._generate_cleanup_code()
        }
        
        return structure
    
    def _generate_initialization_code(self, 
                                    variable_mappings: Dict[str, Any],
                                    connection_mappings: Dict[str, Any]) -> str:
        """Generate PySpark initialization code."""
        code_lines = ["# Control Flow Initialization"]
        
        # Add variable declarations
        if variable_mappings:
            code_lines.append("# SSIS Variables converted to Python variables")
            for var_name, var_info in variable_mappings.items():
                python_value = var_info['python_equivalent']
                code_lines.append(f"{var_name} = {python_value}")
        
        # Add connection configurations
        if connection_mappings:
            code_lines.append("\n# Connection Manager configurations")
            for conn_id, conn_info in connection_mappings.items():
                if conn_info['pyspark_equivalent']['type'] == 'jdbc':
                    code_lines.append(f"""
# Connection: {conn_id}
{conn_id.lower()}_config = {{
    "url": "{conn_info['pyspark_equivalent']['url']}",
    "driver": "{conn_info['pyspark_equivalent']['driver']}"
}}
""")
        
        return "\n".join(code_lines)
    
    def _generate_task_execution_code(self, task_mappings: List[PySparkMapping]) -> str:
        """Generate PySpark task execution code."""
        code_lines = ["# Task Execution"]
        
        for i, task in enumerate(task_mappings):
            task_id = task.ssis_component.id
            task_name = task.ssis_component.name
            
            code_lines.append(f"""
# Task {i+1}: {task_name} ({task_id})
try:
    # Execute {task.pyspark_function}
    # TODO: Implement actual task logic
    print(f"Executing task: {task_name}")
    
    # Placeholder for task execution
    task_{i+1}_result = True
    
except Exception as e:
    print(f"Error executing task {task_name}: {{e}}")
    task_{i+1}_result = False
""")
        
        return "\n".join(code_lines)
    
    def _generate_error_handling_code(self) -> str:
        """Generate PySpark error handling code."""
        return """
# Error Handling
def handle_task_error(task_name, error):
    \"\"\"Handle task execution errors.\"\"\"
    print(f"Task {task_name} failed with error: {error}")
    # Log error details
    # Send notifications if needed
    return False

def validate_task_result(task_name, result):
    \"\"\"Validate task execution result.\"\"\"
    if result is None or result is False:
        raise Exception(f"Task {task_name} validation failed")
    return True
"""
    
    def _generate_cleanup_code(self) -> str:
        """Generate PySpark cleanup code."""
        return """
# Cleanup
def cleanup_resources():
    \"\"\"Clean up resources after execution.\"\"\"
    try:
        # Close any open connections
        # Clean up temporary files
        # Release resources
        print("Cleanup completed successfully")
    except Exception as e:
        print(f"Error during cleanup: {e}")

# Execute cleanup
cleanup_resources()
"""
    
    def _collect_imports(self, task_mappings: List[PySparkMapping]) -> List[str]:
        """Collect all required imports."""
        imports = set()
        
        # Add base imports
        imports.add("from pyspark.sql import SparkSession")
        imports.add("from pyspark.sql.functions import *")
        imports.add("from pyspark.sql.types import *")
        imports.add("import logging")
        imports.add("from datetime import datetime")
        
        # Collect imports from mappings
        for mapping in task_mappings:
            imports.update(mapping.imports)
        
        return list(imports)
    
    def _collect_dependencies(self, task_mappings: List[PySparkMapping]) -> List[str]:
        """Collect all required dependencies."""
        dependencies = set()
        
        # Add base dependencies
        dependencies.add("pyspark")
        
        # Collect dependencies from mappings
        for mapping in task_mappings:
            dependencies.update(mapping.dependencies)
        
        return list(dependencies)
    
    def _create_fallback_task_mapping(self, task: SSISComponent) -> PySparkMapping:
        """Create fallback mapping for task."""
        return PySparkMapping(
            ssis_component=task,
            pyspark_function="fallback_task",
            parameters={
                "description": f"Fallback task mapping for {task.id}",
                "warning": "This task could not be properly mapped"
            },
            dependencies=["pyspark.sql"],
            imports=["from pyspark.sql import SparkSession"]
        )
