"""
Data Engineering Focused Parser for SSIS to PySpark Conversion

This parser extracts ONLY the information needed for data engineering and PySpark conversion:
- Data sources (tables, files, connections)
- Data transformations (logic, filters, joins)
- Data destinations (targets, outputs)
- Column mappings and data types
- SQL logic and business rules

It filters out unnecessary metadata like GUIDs, designer info, logging configs, etc.
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
import re


class DataEngineeringParser:
    """Extracts data engineering relevant information from SSIS packages."""
    
    def __init__(self):
        self.namespaces = {
            'DTS': 'www.microsoft.com/SqlServer/Dts',
            'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'
        }
    
    def parse_dtsx_file(self, file_path: str) -> Dict[str, Any]:
        """
        Parse DTSX file and extract data engineering relevant information.
        
        Returns:
            Dictionary with data sources, transformations, destinations, and SQL logic
        """
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract package name
        package_name = self._extract_package_name(root)
        
        # Extract connections (data sources/destinations)
        connections = self._extract_connections(root)
        
        # Extract data flows
        data_flows = self._extract_data_flows(root, connections)
        
        # Extract SQL tasks (DDL/DML operations)
        sql_tasks = self._extract_sql_tasks(root, connections)
        
        # Build execution order
        execution_order = self._extract_execution_order(root)
        
        return {
            'package_name': package_name,
            'connections': connections,
            'sql_tasks': sql_tasks,
            'data_flows': data_flows,
            'execution_order': execution_order
        }
    
    def _extract_package_name(self, root: ET.Element) -> str:
        """Extract package name."""
        # Try to get from root element attribute first
        package_name = root.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
        if package_name:
            return package_name
        
        # Fallback to searching in properties
        for prop in root.findall('.//{www.microsoft.com/SqlServer/Dts}Property'):
            name = prop.get('{www.microsoft.com/SqlServer/Dts}Name')
            if name == 'ObjectName':
                return prop.text or 'Unknown'
        return 'Unknown'
    
    def _extract_connections(self, root: ET.Element) -> Dict[str, Dict[str, Any]]:
        """
        Extract connection information (data sources/destinations).
        
        Focus on: connection type, server, database, file path, credentials needed
        """
        connections = {}
        
        for conn_mgr in root.findall('.//{www.microsoft.com/SqlServer/Dts}ConnectionManager'):
            # Get connection attributes directly
            conn_name = conn_mgr.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
            conn_type = conn_mgr.get('{www.microsoft.com/SqlServer/Dts}CreationName')
            conn_id = conn_mgr.get('{www.microsoft.com/SqlServer/Dts}refId')
            
            if not conn_name:
                continue
            
            conn_string = None
            
            # Extract connection string from ObjectData
            obj_data = conn_mgr.find('{www.microsoft.com/SqlServer/Dts}ObjectData')
            if obj_data is not None:
                inner_conn = obj_data.find('{www.microsoft.com/SqlServer/Dts}ConnectionManager')
                if inner_conn is not None:
                    conn_string = inner_conn.get('{www.microsoft.com/SqlServer/Dts}ConnectionString')
                    if not conn_string:
                        # Try to find in properties
                        for prop in inner_conn.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                            if prop.get('{www.microsoft.com/SqlServer/Dts}Name') == 'ConnectionString':
                                conn_string = prop.text or ''
                                break
            
            if conn_id:
                # Parse connection string for relevant details
                conn_details = self._parse_connection_string(conn_type, conn_string)
                
                connections[conn_id] = {
                    'name': conn_name,
                    'type': self._simplify_connection_type(conn_type),
                    'connection_string': conn_string,
                    'details': conn_details
                }
        
        return connections
    
    def _simplify_connection_type(self, conn_type: str) -> str:
        """Simplify connection type to data engineering relevant types."""
        if not conn_type:
            return 'UNKNOWN'
        
        type_map = {
            'OLEDB': 'SQL_DATABASE',
            'SQLNCLI': 'SQL_DATABASE',
            'EXCEL': 'EXCEL_FILE',
            'FLATFILE': 'FLAT_FILE',
            'CSV': 'CSV_FILE',
            'ADO.NET': 'SQL_DATABASE',
            'ODBC': 'SQL_DATABASE',
            'FILE': 'FILE_SYSTEM'
        }
        
        conn_type_upper = conn_type.upper()
        for key, value in type_map.items():
            if key in conn_type_upper:
                return value
        
        return 'OTHER'
    
    def _parse_connection_string(self, conn_type: str, conn_string: str) -> Dict[str, str]:
        """Parse connection string to extract relevant details."""
        details = {}
        
        if not conn_string:
            return details
        
        # For SQL connections
        if 'Data Source' in conn_string or 'Server' in conn_string:
            # Extract server
            server_match = re.search(r'Data Source=([^;]+)', conn_string)
            if not server_match:
                server_match = re.search(r'Server=([^;]+)', conn_string)
            if server_match:
                details['server'] = server_match.group(1)
            
            # Extract database
            db_match = re.search(r'Initial Catalog=([^;]+)', conn_string)
            if not db_match:
                db_match = re.search(r'Database=([^;]+)', conn_string)
            if db_match:
                details['database'] = db_match.group(1)
            
            # Extract provider
            provider_match = re.search(r'Provider=([^;]+)', conn_string)
            if provider_match:
                details['provider'] = provider_match.group(1)
        
        # For file connections (Excel, CSV, etc.)
        elif 'Data Source=' in conn_string or '.xls' in conn_string.lower() or '.csv' in conn_string.lower():
            # Extract file path
            file_match = re.search(r'Data Source=([^;]+)', conn_string)
            if file_match:
                file_path = file_match.group(1)
                details['file_path'] = file_path
                details['file_name'] = file_path.split('\\')[-1] if '\\' in file_path else file_path
                
                # Determine file type
                if '.xls' in file_path.lower():
                    details['file_type'] = 'EXCEL'
                elif '.csv' in file_path.lower():
                    details['file_type'] = 'CSV'
                elif '.txt' in file_path.lower():
                    details['file_type'] = 'TEXT'
        
        return details
    
    def _extract_sql_tasks(self, root: ET.Element, connections: Dict) -> List[Dict[str, Any]]:
        """Extract SQL tasks with their SQL statements and purpose."""
        sql_tasks = []
        
        for executable in root.findall('.//{www.microsoft.com/SqlServer/Dts}Executable'):
            exec_type = executable.get('{www.microsoft.com/SqlServer/Dts}ExecutableType')
            
            # Skip disabled tasks
            is_disabled = executable.get('{www.microsoft.com/SqlServer/Dts}Disabled', 'False')
            if is_disabled.lower() == 'true':
                continue
            
            if exec_type and 'ExecuteSQLTask' in exec_type:
                # Get task ID (DTSID) for execution order tracking
                task_id = executable.get('{www.microsoft.com/SqlServer/Dts}DTSID', '')
                
                # Get task name from attribute first
                task_name = executable.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
                
                # Fallback to properties if not found
                if not task_name:
                    for prop in executable.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                        if prop.get('{www.microsoft.com/SqlServer/Dts}Name') == 'ObjectName':
                            task_name = prop.text
                            break
                
                # Extract SQL statement
                obj_data = executable.find('{www.microsoft.com/SqlServer/Dts}ObjectData')
                if obj_data is not None:
                    sql_data = self._extract_sql_task_data(obj_data)
                    
                    if sql_data.get('sql_statement'):
                        # Determine SQL purpose
                        sql_purpose = self._determine_sql_purpose(sql_data['sql_statement'])
                        
                        # Get connection details
                        conn_id = sql_data.get('connection', '').strip('{}')
                        connection_info = None
                        for conn_name, conn_data in connections.items():
                            if conn_id in str(conn_data):
                                connection_info = {
                                    'name': conn_name,
                                    'type': conn_data['type'],
                                    'details': conn_data['details']
                                }
                                break
                        
                        sql_tasks.append({
                            'task_id': task_id,
                            'name': task_name,
                            'purpose': sql_purpose,
                            'sql_statement': sql_data['sql_statement'],
                            'connection': connection_info
                        })
        
        return sql_tasks
    
    def _extract_sql_task_data(self, obj_data: ET.Element) -> Dict[str, Any]:
        """Extract SQL task specific data."""
        data = {}
        
        for child in obj_data:
            if 'SqlTaskData' in child.tag:
                data['connection'] = child.get('{www.microsoft.com/sqlserver/dts/tasks/sqltask}Connection', '')
                data['sql_statement'] = child.get('{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlStatementSource', '')
                break
        
        return data
    
    def _determine_sql_purpose(self, sql: str) -> str:
        """Determine the purpose of SQL statement."""
        sql_upper = sql.upper().strip()
        
        if sql_upper.startswith('CREATE TABLE'):
            return 'CREATE_TABLE'
        elif sql_upper.startswith('DROP TABLE'):
            return 'DROP_TABLE'
        elif sql_upper.startswith('TRUNCATE'):
            return 'TRUNCATE_TABLE'
        elif sql_upper.startswith('INSERT'):
            return 'INSERT_DATA'
        elif sql_upper.startswith('UPDATE'):
            return 'UPDATE_DATA'
        elif sql_upper.startswith('DELETE'):
            return 'DELETE_DATA'
        elif sql_upper.startswith('SELECT'):
            return 'SELECT_DATA'
        elif sql_upper.startswith('EXEC') or sql_upper.startswith('EXECUTE'):
            return 'EXECUTE_PROCEDURE'
        else:
            return 'OTHER_SQL'
    
    def _extract_data_flows(self, root: ET.Element, connections: Dict) -> List[Dict[str, Any]]:
        """Extract data flow tasks with sources, transformations, and destinations.
        Handles nested containers (For Loop, Foreach Loop, Sequence)."""
        data_flows = []
        
        # Recursively find all executables (including those in containers)
        for executable in root.findall('.//{www.microsoft.com/SqlServer/Dts}Executable'):
            exec_type = executable.get('{www.microsoft.com/SqlServer/Dts}ExecutableType')
            
            # Skip disabled tasks
            is_disabled = executable.get('{www.microsoft.com/SqlServer/Dts}Disabled', 'False')
            if is_disabled.lower() == 'true':
                continue
            
            if exec_type and 'Pipeline' in exec_type:
                # Get task ID (DTSID) for execution order tracking
                task_id = executable.get('{www.microsoft.com/SqlServer/Dts}DTSID', '')
                
                # Get task name from attribute first
                task_name = executable.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
                
                # Fallback to properties if not found
                if not task_name:
                    for prop in executable.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                        if prop.get('{www.microsoft.com/SqlServer/Dts}Name') == 'ObjectName':
                            task_name = prop.text
                            break
                
                # Get parent container info if exists
                parent_container = self._get_parent_container(executable, root)
                
                # Extract pipeline components
                obj_data = executable.find('{www.microsoft.com/SqlServer/Dts}ObjectData')
                if obj_data is not None:
                    pipeline_data = self._extract_pipeline_data(obj_data, connections)
                    
                    if pipeline_data:
                        data_flow = {
                            'task_id': task_id,
                            'name': task_name,
                            'sources': pipeline_data['sources'],
                            'transformations': pipeline_data['transformations'],
                            'destinations': pipeline_data['destinations'],
                            'data_flow_paths': pipeline_data.get('data_flow_paths', [])
                        }
                        
                        # Add container info if exists
                        if parent_container:
                            data_flow['parent_container'] = parent_container
                        
                        data_flows.append(data_flow)
        
        return data_flows
    
    def _get_parent_container(self, executable: ET.Element, root: ET.Element) -> Optional[Dict[str, str]]:
        """Get parent container information if the task is inside a container."""
        exec_ref_id = executable.get('{www.microsoft.com/SqlServer/Dts}refId', '')
        
        # Check if this executable is inside a container
        for container in root.findall('.//{www.microsoft.com/SqlServer/Dts}Executable'):
            container_type = container.get('{www.microsoft.com/SqlServer/Dts}ExecutableType', '')
            
            # Check for container types
            if any(ct in container_type for ct in ['STOCK:FORLOOP', 'STOCK:FOREACHLOOP', 'STOCK:SEQUENCE']):
                # Check if our executable is a child of this container
                container_executables = container.find('{www.microsoft.com/SqlServer/Dts}Executables')
                if container_executables is not None:
                    for child in container_executables.findall('{www.microsoft.com/SqlServer/Dts}Executable'):
                        if child == executable or child.get('{www.microsoft.com/SqlServer/Dts}refId') == exec_ref_id:
                            container_name = container.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown Container')
                            container_type_simple = 'FOR_LOOP' if 'FORLOOP' in container_type else \
                                                   'FOREACH_LOOP' if 'FOREACHLOOP' in container_type else \
                                                   'SEQUENCE' if 'SEQUENCE' in container_type else 'CONTAINER'
                            
                            return {
                                'name': container_name,
                                'type': container_type_simple
                            }
        
        return None
    
    def _extract_pipeline_data(self, obj_data: ET.Element, connections: Dict) -> Dict[str, Any]:
        """Extract data flow pipeline components with lineage and paths."""
        sources = []
        transformations = []
        destinations = []
        component_map = {}  # Map component refId to component info
        
        # First pass: Extract all components
        for component in obj_data.findall('.//component'):
            comp_ref_id = component.get('refId', '')
            comp_name = component.get('name', 'Unknown')
            comp_class = component.get('componentClassID', '')
            
            # Extract component properties
            properties = self._extract_component_properties(component)
            
            # Store component info for lineage tracking
            component_map[comp_ref_id] = {
                'name': comp_name,
                'class': comp_class,
                'properties': properties
            }
            
            # Determine component type and extract relevant info
            if self._is_source_component(comp_class, comp_name):
                source_info = self._extract_source_info(comp_name, comp_class, properties, connections, component)
                source_info['component_id'] = comp_ref_id
                sources.append(source_info)
            
            elif self._is_destination_component(comp_class, comp_name):
                dest_info = self._extract_destination_info(comp_name, comp_class, properties, connections, component)
                dest_info['component_id'] = comp_ref_id
                destinations.append(dest_info)
            
            else:
                trans_info = self._extract_transformation_info(comp_name, comp_class, properties, component)
                if trans_info:
                    trans_info['component_id'] = comp_ref_id
                    transformations.append(trans_info)
        
        # Second pass: Extract data flow paths (lineage)
        paths = self._extract_data_flow_paths(obj_data, component_map)
        
        return {
            'sources': sources,
            'transformations': transformations,
            'destinations': destinations,
            'data_flow_paths': paths
        }
    
    def _extract_data_flow_paths(self, obj_data: ET.Element, component_map: Dict) -> List[Dict[str, str]]:
        """Extract data flow paths showing how components are connected with component IDs."""
        paths = []
        
        # Find all path elements in the pipeline
        for path in obj_data.findall('.//path'):
            path_name = path.get('name', '')
            start_id = path.get('startId', '')
            end_id = path.get('endId', '')
            
            # Extract component IDs (refId) from path IDs - this gives us unique identifiers
            from_component_id = self._get_component_id_from_output_id(start_id, component_map)
            to_component_id = self._get_component_id_from_input_id(end_id, component_map)
            
            # Extract component names for backward compatibility
            from_component_name = None
            to_component_name = None
            if from_component_id and from_component_id in component_map:
                from_component_name = component_map[from_component_id]['name']
            if to_component_id and to_component_id in component_map:
                to_component_name = component_map[to_component_id]['name']
            
            if from_component_id and to_component_id and from_component_name and to_component_name:
                paths.append({
                    'from': from_component_name,      # Keep for backward compatibility
                    'to': to_component_name,          # Keep for backward compatibility
                    'from_id': from_component_id,     # NEW: Full component ID (refId)
                    'to_id': to_component_id,         # NEW: Full component ID (refId)
                    'path_name': path_name
                })
        
        return paths
    
    def _get_component_name_from_output_id(self, output_id: str, component_map: Dict) -> Optional[str]:
        """Extract component name from output ID."""
        # Output ID format: Package\Data Flow Task\ComponentName.Outputs[OutputName]
        for comp_id, comp_info in component_map.items():
            if comp_info['name'] in output_id:
                return comp_info['name']
        return None
    
    def _get_component_name_from_input_id(self, input_id: str, component_map: Dict) -> Optional[str]:
        """Extract component name from input ID."""
        # Input ID format: Package\Data Flow Task\ComponentName.Inputs[InputName]
        for comp_id, comp_info in component_map.items():
            if comp_info['name'] in input_id:
                return comp_info['name']
        return None
    
    def _get_component_id_from_output_id(self, output_id: str, component_map: Dict) -> Optional[str]:
        """Extract component ID (refId) from output ID - returns unique component identifier."""
        # Output ID format: Package\Data Flow Task\ComponentName.Outputs[OutputName]
        # We need to match the component refId, not just the name
        # The output_id contains the full path like "Package\\DFT Load\\ComponentName.Outputs[OutputName]"
        for comp_id, comp_info in component_map.items():
            comp_name = comp_info['name']
            # Check if component_id (refId) appears in the output_id path
            # Format: "Package\\DFT Load\\ComponentName" should match comp_id format
            if comp_id in output_id:
                return comp_id
            # Also check if the component name is in the output_id AND comp_id ends with that name
            # This handles cases where comp_id is like "Package\\DFT Load\\DER"
            if comp_name in output_id and comp_id.endswith(comp_name):
                return comp_id
        return None
    
    def _get_component_id_from_input_id(self, input_id: str, component_map: Dict) -> Optional[str]:
        """Extract component ID (refId) from input ID - returns unique component identifier."""
        # Input ID format: Package\Data Flow Task\ComponentName.Inputs[InputName]
        # Similar logic to output ID
        for comp_id, comp_info in component_map.items():
            comp_name = comp_info['name']
            if comp_id in input_id:
                return comp_id
            if comp_name in input_id and comp_id.endswith(comp_name):
                return comp_id
        return None
    
    def _is_source_component(self, comp_class: str, comp_name: str) -> bool:
        """Check if component is a data source."""
        source_indicators = [
            'Source', 'OLE DB Source', 'Excel Source', 'Flat File Source', 
            'ADO NET Source', 'XML Source', 'JSON Source', 'CSV Source',
            'ODBC Source', 'Azure Blob Source', 'Data Lake Source'
        ]
        return any(indicator in comp_name or indicator in comp_class for indicator in source_indicators)
    
    def _is_destination_component(self, comp_class: str, comp_name: str) -> bool:
        """Check if component is a data destination."""
        dest_indicators = [
            'Destination', 'OLE DB Destination', 'Excel Destination', 
            'Flat File Destination', 'ADO NET Destination', 'Azure Blob Destination',
            'Data Lake Destination', 'Recordset Destination'
        ]
        return any(indicator in comp_name or indicator in comp_class for indicator in dest_indicators)
    
    def _extract_component_properties(self, component: ET.Element) -> Dict[str, str]:
        """Extract properties from a component."""
        properties = {}
        
        props_elem = component.find('.//properties')
        if props_elem is not None:
            for prop in props_elem.findall('.//property'):
                prop_name = prop.get('name', '')
                # Get all text content including text in child elements
                prop_value = ''.join(prop.itertext()).strip()
                if prop_name and prop_value:
                    properties[prop_name] = prop_value
        
        return properties
    
    def _extract_source_info(self, name: str, comp_class: str, properties: Dict, connections: Dict, component: ET.Element = None) -> Dict[str, Any]:
        """Extract data source information."""
        source_type = self._determine_source_type(comp_class, name)
        
        # Extract table name from various possible properties
        table_name = (properties.get('OpenRowset', '') or 
                     properties.get('SqlCommand', '') or 
                     properties.get('TableName', '') or
                     properties.get('Table', '') or
                     '')
        
        # Extract AccessMode to determine if it's a SQL query or table
        access_mode = properties.get('AccessMode', '1')  # Default to 1 (table)
        is_sql_query = access_mode == '2'  # AccessMode 2 = SQL Command
        
        # Find connection for this source
        connection_info = None
        connections_elem = component.find('.//connections')
        if connections_elem is not None:
            conn_elem = connections_elem.find('.//connection')
            if conn_elem is not None:
                conn_manager_id = conn_elem.get('connectionManagerID', '')
                if conn_manager_id:
                    # Extract connection name from ID like "Package.ConnectionManagers[dwCOMMON]"
                    conn_name = conn_manager_id.split('[')[-1].rstrip(']')
                    connection_info = {
                        'name': conn_name,
                        'id': conn_manager_id
                    }
        
        source_info = {
            'name': name,
            'type': source_type,
            'table_or_query': table_name,
            'table_name': table_name,  # Add explicit table_name field
            'access_mode': access_mode,
            'is_sql_query': is_sql_query,
            'sql_query': table_name if is_sql_query else '',
            'columns': [],  # Would need column metadata extraction
            'connection': connection_info
        }
        
        # Handle REST API / JSON sources
        if source_type == 'REST_API_JSON':
            # Extract API URL
            api_url = properties.get('DirectPath', properties.get('Url', ''))
            if api_url:
                source_info['api_url'] = api_url
                source_info['table_or_query'] = api_url
            
            # Extract filter/path expression
            filter_expr = properties.get('Filter', '')
            if filter_expr:
                source_info['json_path'] = filter_expr
        
        # Parse table/query information for SQL sources
        elif source_info['table_or_query']:
            source_info['is_query'] = 'SELECT' in source_info['table_or_query'].upper()
            
            if not source_info['is_query']:
                # Clean table name - remove all brackets first
                table_name = source_info['table_or_query'].replace('[', '').replace(']', '').strip('"')
                source_info['table_name'] = table_name
                
                # Parse schema and table
                if '.' in table_name:
                    parts = table_name.split('.')
                    if len(parts) == 3:
                        source_info['database'] = parts[0].strip()
                        source_info['schema'] = parts[1].strip()
                        source_info['table'] = parts[2].strip()
                    elif len(parts) == 2:
                        source_info['schema'] = parts[0].strip()
                        source_info['table'] = parts[1].strip()
        
        return source_info
    
    def _extract_destination_info(self, name: str, comp_class: str, properties: Dict, connections: Dict, component: ET.Element = None) -> Dict[str, Any]:
        """Extract data destination information."""
        # Extract table name from various possible properties
        table_name = (properties.get('OpenRowset', '') or 
                     properties.get('TableName', '') or
                     properties.get('Table', '') or
                     '')
        
        # Find connection for this destination
        connection_info = None
        if component is not None:
            connections_elem = component.find('.//connections')
            if connections_elem is not None:
                conn_elem = connections_elem.find('.//connection')
                if conn_elem is not None:
                    conn_manager_id = conn_elem.get('connectionManagerID', '')
                    if conn_manager_id:
                        # Extract connection name from ID like "Package.ConnectionManagers[dmRegulatory]"
                        conn_name = conn_manager_id.split('[')[-1].rstrip(']')
                        connection_info = {
                            'name': conn_name,
                            'id': conn_manager_id
                        }
        
        dest_info = {
            'name': name,
            'type': self._determine_destination_type(comp_class, name),
            'table_or_file': table_name,
            'table_name': table_name,  # Add explicit table_name field
            'write_mode': self._determine_write_mode(properties),
            'connection': connection_info
        }
        
        # Parse destination details
        if dest_info['table_or_file']:
            # Clean table/file name - remove all brackets first
            table_name = dest_info['table_or_file'].replace('[', '').replace(']', '').strip('"')
            dest_info['target_name'] = table_name
            
            # Parse schema and table for SQL destinations
            if '.' in table_name:
                parts = table_name.split('.')
                if len(parts) == 3:
                    dest_info['database'] = parts[0].strip()
                    dest_info['schema'] = parts[1].strip()
                    dest_info['table'] = parts[2].strip()
                elif len(parts) == 2:
                    dest_info['schema'] = parts[0].strip()
                    dest_info['table'] = parts[1].strip()
        
        return dest_info
    
    def _extract_transformation_info(self, name: str, comp_class: str, properties: Dict, component: ET.Element = None) -> Optional[Dict[str, Any]]:
        """Extract transformation information."""
        trans_type = self._determine_transformation_type(comp_class, name, properties)
        
        # Don't skip unknown transformations - include them as generic transformations
        # This ensures all components are extracted from the XML
        if trans_type == 'UNKNOWN':
            trans_type = 'GENERIC_TRANSFORMATION'
        
        # Extract actual transformation logic from XML
        transformation_logic = self._extract_actual_transformation_logic(component, trans_type)
        
        # For LOOKUP, merge with properties-based logic if needed
        if trans_type == 'LOOKUP' and isinstance(transformation_logic, dict):
            # If lookup_logic is already a dict, use it; otherwise merge with properties
            if not transformation_logic.get('lookup_query'):
                properties_logic = self._extract_transformation_logic(trans_type, properties)
                transformation_logic.update(properties_logic)
        
        trans_info = {
            'name': name,
            'type': trans_type,
            'logic': transformation_logic,
            'component_class': comp_class,  # Include component class for reference
            'expressions': self._extract_transformation_expressions(component),
            'outputs': self._extract_transformation_outputs(component)
        }
        
        return trans_info
    
    def _extract_actual_transformation_logic(self, component: ET.Element, trans_type: str):
        """Extract actual transformation logic from XML component."""
        if component is None:
            if trans_type == 'LOOKUP':
                return {}
            return "No transformation logic available"
        
        if trans_type == 'CONDITIONAL_SPLIT':
            return self._extract_conditional_split_logic(component)
        elif trans_type == 'DERIVED_COLUMN':
            return self._extract_derived_column_logic(component)
        elif trans_type == 'MERGE_JOIN':
            return self._extract_merge_join_logic(component)
        elif trans_type == 'MERGE':
            return self._extract_merge_logic(component)
        elif trans_type == 'CHECKSUM':
            return self._extract_checksum_logic(component)
        elif trans_type == 'OLE_DB_COMMAND':
            return self._extract_ole_db_command_logic(component)
        elif trans_type == 'LOOKUP':
            return self._extract_lookup_logic(component)
        else:
            return f"Transformation type: {trans_type}"
    
    def _extract_conditional_split_logic(self, component: ET.Element) -> str:
        """Extract conditional split logic from XML."""
        outputs = []
        for output in component.findall('.//output'):
            output_name = output.get('name', '')
            for prop in output.findall('.//property'):
                if prop.get('name') == 'FriendlyExpression':
                    expression = prop.text or ''
                    if expression:
                        outputs.append(f"{output_name}: {expression}")
        return " | ".join(outputs) if outputs else "No conditional logic found"
    
    def _extract_derived_column_logic(self, component: ET.Element) -> str:
        """Extract derived column logic from XML."""
        columns = []
        for output_col in component.findall('.//outputColumn'):
            col_name = output_col.get('name', '')
            for prop in output_col.findall('.//property'):
                if prop.get('name') == 'FriendlyExpression':
                    expression = prop.text or ''
                    if expression:
                        columns.append(f"{col_name} = {expression}")
        return " | ".join(columns) if columns else "No derived column logic found"
    
    def _extract_merge_join_logic(self, component: ET.Element) -> Dict[str, Any]:
        """Extract merge join logic from XML."""
        logic = {}
        
        if component is not None:
            properties = self._extract_component_properties(component)
            
            # Extract join type (0=Inner, 1=Left, 2=Full)
            join_type_val = properties.get('JoinType', '0')
            join_type_map = {'0': 'inner', '1': 'leftouter', '2': 'fullouter'}
            logic['join_type'] = join_type_map.get(join_type_val, 'inner')
            
            # Extract join columns from left and right inputs
            left_join_cols = []
            right_join_cols = []
            
            for input_elem in component.findall('.//inputs/input'):
                input_name = input_elem.get('name', '')
                
                # Identify left vs right input
                is_left = 'Left' in input_name or input_name == 'Merge Join Left Input'
                
                for col in input_elem.findall('.//inputColumns/inputColumn'):
                    col_name = col.get('cachedName', col.get('name', ''))
                    sort_pos = col.get('cachedSortKeyPosition', '')
                    lineage_id = col.get('lineageId', '')
                    
                    # Columns with sort position are join keys
                    if sort_pos and sort_pos.isdigit():
                        col_info = {
                            'column': col_name,
                            'position': int(sort_pos),
                            'lineage': lineage_id
                        }
                        
                        if is_left:
                            left_join_cols.append(col_info)
                        else:
                            right_join_cols.append(col_info)
            
            # Match join columns by position and build join conditions
            left_join_cols.sort(key=lambda x: x['position'])
            right_join_cols.sort(key=lambda x: x['position'])
            
            join_conditions = []
            for left_col, right_col in zip(left_join_cols, right_join_cols):
                join_conditions.append({
                    'left_column': left_col['column'],
                    'right_column': right_col['column'],
                    'operator': '=='
                })
            
            if join_conditions:
                logic['join_conditions'] = join_conditions
            
            # Extract source component names from lineage IDs for identifying input DataFrames
            left_sources = set()
            right_sources = set()
            
            for col_list, source_set in [(left_join_cols, left_sources), (right_join_cols, right_sources)]:
                for col_info in col_list:
                    lineage = col_info.get('lineage', '')
                    # Extract component name from lineage: ...\ComponentName.Outputs[...]
                    if '\\' in lineage and '.Outputs[' in lineage:
                        parts = lineage.split('\\')
                        for part in parts:
                            if '.Outputs[' in part:
                                comp_name = part.split('.Outputs[')[0]
                                source_set.add(comp_name)
                                break
            
            if left_sources:
                logic['left_source_components'] = list(left_sources)
            if right_sources:
                logic['right_source_components'] = list(right_sources)
            
            # Extract null handling
            logic['treat_nulls_as_equal'] = properties.get('TreatNullsAsEqual', 'true')
        
        return logic
    
    def _extract_merge_logic(self, component: ET.Element) -> Dict[str, Any]:
        """Extract merge transformation logic from XML.
        
        MERGE combines two sorted inputs (like UNION ALL) - it does not perform joins.
        Extracts input sources and sort columns.
        """
        logic = {}
        
        if component is not None:
            properties = self._extract_component_properties(component)
            
            # MERGE combines inputs, not joins them
            logic['description'] = 'Combines multiple sorted inputs into single output'
            
            # Extract input sources and their sort columns
            input_sources = []
            for input_elem in component.findall('.//inputs/input'):
                input_name = input_elem.get('name', '')
                
                # Extract sort columns for this input (MERGE requires sorted inputs)
                sort_columns = []
                for col in input_elem.findall('.//inputColumns/inputColumn'):
                    col_name = col.get('cachedName', col.get('name', ''))
                    sort_pos = col.get('cachedSortKeyPosition', '')
                    
                    if sort_pos and sort_pos.isdigit():
                        sort_columns.append({
                            'column': col_name,
                            'position': int(sort_pos)
                        })
                
                if sort_columns:
                    sort_columns.sort(key=lambda x: x['position'])
                
                input_info = {
                    'input_name': input_name,
                    'sort_columns': [col['column'] for col in sort_columns] if sort_columns else []
                }
                input_sources.append(input_info)
            
            if input_sources:
                logic['input_sources'] = input_sources
            
            # Extract source component names from lineage IDs
            source_components = set()
            for input_elem in component.findall('.//inputs/input'):
                for col in input_elem.findall('.//inputColumns/inputColumn'):
                    lineage_id = col.get('lineageId', '')
                    if '\\' in lineage_id and '.Outputs[' in lineage_id:
                        parts = lineage_id.split('\\')
                        for part in parts:
                            if '.Outputs[' in part:
                                comp_name = part.split('.Outputs[')[0]
                                source_components.add(comp_name)
                                break
            
            if source_components:
                logic['source_components'] = list(source_components)
        
        return logic
    
    def _extract_checksum_logic(self, component: ET.Element) -> str:
        """Extract checksum logic from XML."""
        # Extract checksum algorithm and columns
        return "Checksum transformation"
    
    def _extract_ole_db_command_logic(self, component: ET.Element) -> Dict[str, Any]:
        """Extract OLE DB command logic from XML including input columns and parameter mappings."""
        logic = {}
        
        if component is not None:
            properties = self._extract_component_properties(component)
            sql_command = properties.get('SqlCommand', '')
            if sql_command:
                logic['sql_command'] = sql_command.strip()
            
            # Extract input columns and their mappings to stored procedure parameters
            input_columns = []
            input_elem = component.find('.//input[@name="OLE DB Command Input"]')
            if input_elem is not None:
                for input_col in input_elem.findall('.//inputColumn'):
                    col_name = input_col.get('cachedName', '')
                    external_metadata_id = input_col.get('externalMetadataColumnId', '')
                    
                    # Find the corresponding external metadata column to get parameter name
                    param_name = None
                    if external_metadata_id:
                        external_cols = input_elem.findall('.//externalMetadataColumn')
                        for ext_col in external_cols:
                            if ext_col.get('refId') == external_metadata_id:
                                param_name = ext_col.get('name', '')  # e.g., "@piWorkHistoryID"
                                break
                    
                    if col_name:
                        input_columns.append({
                            'column_name': col_name,
                            'parameter_name': param_name
                        })
            
            if input_columns:
                logic['input_columns'] = input_columns
        
        if not logic or 'sql_command' not in logic:
            return "OLE DB command transformation"
        
        return logic
    
    def _extract_lookup_logic(self, component: ET.Element) -> Dict[str, Any]:
        """Extract lookup transformation logic from XML."""
        logic = {}
        
        if component is not None:
            properties = self._extract_component_properties(component)
            
            # Extract SQL query
            sql_command = properties.get('SqlCommand', '').strip()
            if sql_command:
                logic['lookup_query'] = sql_command
            else:
                sql_command_var = properties.get('SqlCommandVariable', '').strip()
                if sql_command_var:
                    logic['lookup_query'] = sql_command_var
            
            # Extract cache configuration
            logic['cache_type'] = properties.get('CacheType', '0')  # 0=FULL, 1=PARTIAL, 2=NO_CACHE
            logic['connection_type'] = properties.get('ConnectionType', '0')
            
            # Extract no match behavior (0=Error, 1=Ignore/Reroute)
            no_match = properties.get('NoMatchBehavior', '0')
            logic['no_match_behavior'] = no_match  # '0' = Error, '1' = Ignore/Left join
            
            # Extract column mappings for join conditions
            column_mappings = []
            for input_col in component.findall('.//inputColumns/inputColumn'):
                input_col_name = input_col.get('cachedName', input_col.get('name', ''))
                join_to_ref = None
                copy_from_ref = None
                
                for prop in input_col.findall('.//property'):
                    prop_name = prop.get('name', '')
                    prop_value = ''.join(prop.itertext()).strip()
                    
                    if prop_name == 'JoinToReferenceColumn' and prop_value:
                        join_to_ref = prop_value
                    elif prop_name == 'CopyFromReferenceColumn' and prop_value:
                        copy_from_ref = prop_value
                
                if input_col_name and (join_to_ref or copy_from_ref):
                    column_mappings.append({
                        'input_column': input_col_name,
                        'join_to_reference_column': join_to_ref,
                        'copy_from_reference_column': copy_from_ref
                    })
            
            # Also extract output columns that are copied from lookup (in outputColumns)
            for output_col in component.findall('.//outputColumns/outputColumn'):
                copy_from_ref = None
                output_col_name = output_col.get('name', '')
                
                for prop in output_col.findall('.//property'):
                    prop_name = prop.get('name', '')
                    prop_value = ''.join(prop.itertext()).strip()
                    
                    if prop_name == 'CopyFromReferenceColumn' and prop_value:
                        copy_from_ref = prop_value
                        # Add to column_mappings if not already present
                        found = False
                        for mapping in column_mappings:
                            if mapping.get('copy_from_reference_column') == copy_from_ref:
                                found = True
                                break
                        if not found:
                            column_mappings.append({
                                'input_column': '',  # Empty for output-only columns
                                'join_to_reference_column': None,
                                'copy_from_reference_column': copy_from_ref,
                                'output_column': output_col_name
                            })
            
            if column_mappings:
                logic['column_mappings'] = column_mappings
        
        return logic
    
    def _extract_transformation_expressions(self, component: ET.Element) -> List[Dict[str, str]]:
        """Extract transformation expressions from XML."""
        expressions = []
        if component is not None:
            for prop in component.findall('.//property'):
                if prop.get('name') in ['Expression', 'FriendlyExpression']:
                    expressions.append({
                        'name': prop.get('name', ''),
                        'value': prop.text or ''
                    })
        return expressions
    
    def _extract_transformation_outputs(self, component: ET.Element) -> List[Dict[str, str]]:
        """Extract transformation outputs from XML."""
        outputs = []
        if component is not None:
            for output in component.findall('.//output'):
                output_name = output.get('name', '')
                output_desc = output.get('description', '')
                outputs.append({
                    'name': output_name,
                    'description': output_desc
                })
        return outputs
    
    def _determine_source_type(self, comp_class: str, name: str) -> str:
        """Determine source type."""
        if 'OLE DB' in name or 'OLE DB' in comp_class:
            return 'SQL_TABLE'
        elif 'Excel' in name or 'Excel' in comp_class:
            return 'EXCEL_FILE'
        elif 'Flat File' in name or 'Flat File' in comp_class:
            return 'FLAT_FILE'
        elif 'CSV' in name:
            return 'CSV_FILE'
        elif 'JSON' in name or 'REST' in name or 'API' in name:
            return 'REST_API_JSON'
        else:
            return 'OTHER'
    
    def _determine_destination_type(self, comp_class: str, name: str) -> str:
        """Determine destination type."""
        if 'OLE DB' in name or 'OLE DB' in comp_class:
            return 'SQL_TABLE'
        elif 'Excel' in name or 'Excel' in comp_class:
            return 'EXCEL_FILE'
        elif 'Flat File' in name or 'Flat File' in comp_class:
            return 'FLAT_FILE'
        else:
            return 'OTHER'
    
    def _determine_write_mode(self, properties: Dict) -> str:
        """Determine write mode (append, overwrite, etc.)."""
        # Check for fast load options
        if properties.get('FastLoadOptions'):
            return 'FAST_LOAD'
        
        # Check for table lock
        if properties.get('FastLoadKeepIdentity') == 'true':
            return 'APPEND'
        
        return 'INSERT'
    
    def _determine_transformation_type(self, comp_class: str, name: str, properties: Dict = None) -> str:
        """Determine transformation type - comprehensive list of SSIS transformations."""
        
        # Check UserComponentTypeName for managed components
        if properties and 'UserComponentTypeName' in properties:
            user_component = properties['UserComponentTypeName']
            if 'TrashDestination' in user_component:
                return 'TRASH_DESTINATION'
            elif 'ChecksumTransform' in user_component:
                return 'CHECKSUM'
            elif 'RowCount' in user_component:
                # Check the name to determine specific type
                if 'RC INSERT' in name:
                    return 'RC_INSERT'
                elif 'RC SELECT' in name:
                    return 'RC_SELECT'
                elif 'RC UPDATE' in name:
                    return 'RC_UPDATE'
                elif 'RC DELETE' in name:
                    return 'RC_DELETE'
                elif 'RC INT' in name:
                    return 'RC_INTERMEDIATE'
                else:
                    return 'ROW_COUNT'
        
        trans_map = {
            # Row transformations
            'Derived Column': 'DERIVED_COLUMN',
            'DerivedColumn': 'DERIVED_COLUMN',
            'Data Conversion': 'DATA_CONVERSION',
            'DataConversion': 'DATA_CONVERSION',
            'Copy Column': 'COPY_COLUMN',
            'Character Map': 'CHARACTER_MAP',
            
            # Rowset transformations
            'Aggregate': 'AGGREGATE',
            'Sort': 'SORT',
            'Percentage Sampling': 'PERCENTAGE_SAMPLING',
            'Row Sampling': 'ROW_SAMPLING',
            'Pivot': 'PIVOT',
            'Unpivot': 'UNPIVOT',
            
            # Split and join transformations
            'Conditional Split': 'CONDITIONAL_SPLIT',
            'ConditionalSplit': 'CONDITIONAL_SPLIT',
            'Multicast': 'MULTICAST',
            'Union All': 'UNION_ALL',
            'UnionAll': 'UNION_ALL',
            'Merge': 'MERGE',
            'Merge Join': 'MERGE_JOIN',
            'MergeJoin': 'MERGE_JOIN',
            
            # Lookup and reference
            'Lookup': 'LOOKUP',
            'Fuzzy Lookup': 'FUZZY_LOOKUP',
            'Fuzzy Grouping': 'FUZZY_GROUPING',
            'Cache Transform': 'CACHE_TRANSFORM',
            
            # Business intelligence
            'Slowly Changing Dimension': 'SCD',
            'Term Extraction': 'TERM_EXTRACTION',
            'Term Lookup': 'TERM_LOOKUP',
            
            # Auditing and profiling
            'Audit': 'AUDIT',
            'Row Count': 'ROW_COUNT',
            'RowCount': 'ROW_COUNT',
            'RC Insert': 'RC_INSERT',
            'RC_Insert': 'RC_INSERT',
            'RC INSERT': 'RC_INSERT',
            'RC SELECT': 'RC_SELECT',
            'RC UPDATE': 'RC_UPDATE',
            'RC DELETE': 'RC_DELETE',
            'RC INT': 'RC_INTERMEDIATE',
            
            # Checksum and OLE DB Command
            'CHK': 'CHECKSUM',
            'Checksum': 'CHECKSUM',
            'CMD': 'OLE_DB_COMMAND',
            'OLE DB Command': 'OLE_DB_COMMAND',
            'OLEDBCommand': 'OLE_DB_COMMAND',
            
            # Script and custom
            'Script Component': 'SCRIPT_COMPONENT',
            'Script': 'SCRIPT_COMPONENT',
            
            # Data quality
            'Data Mining Query': 'DATA_MINING_QUERY',
            'Export Column': 'EXPORT_COLUMN',
            'Import Column': 'IMPORT_COLUMN',
            
            # XML
            'XML': 'XML_TRANSFORM'
        }
        
        for key, value in trans_map.items():
            if key in name or key in comp_class:
                return value
        
        # If not found, check if it's a generic transformation
        if 'Transform' in comp_class or 'Transform' in name:
            return 'CUSTOM_TRANSFORM'
        
        return 'UNKNOWN'
    
    def _extract_transformation_logic(self, trans_type: str, properties: Dict) -> Dict[str, Any]:
        """Extract transformation logic details - comprehensive extraction."""
        logic = {}
        
        # Row transformations
        if trans_type == 'DERIVED_COLUMN':
            logic['expressions'] = properties.get('Expression', properties.get('FriendlyExpression', ''))
            logic['replace_existing'] = properties.get('ReplaceExisting', 'false')
        
        elif trans_type == 'DATA_CONVERSION':
            logic['data_type'] = properties.get('DataType', '')
            logic['length'] = properties.get('Length', '')
            logic['precision'] = properties.get('Precision', '')
            logic['scale'] = properties.get('Scale', '')
            logic['code_page'] = properties.get('CodePage', '')
        
        # Split and join transformations
        elif trans_type == 'CONDITIONAL_SPLIT':
            logic['conditions'] = properties.get('FriendlyExpression', properties.get('Expression', ''))
            logic['default_output_name'] = properties.get('DefaultOutputName', '')
            logic['evaluation_order'] = properties.get('EvaluationOrder', '')
        
        elif trans_type == 'MERGE_JOIN':
            logic['join_type'] = properties.get('JoinType', 'INNER')  # INNER, LEFT, FULL
            logic['treat_nulls_as_equal'] = properties.get('TreatNullsAsEqual', 'false')
            logic['max_buffers_per_input'] = properties.get('MaxBuffersPerInput', '')
        
        elif trans_type == 'UNION_ALL':
            logic['description'] = 'Combines multiple inputs into single output'
        
        elif trans_type == 'MULTICAST':
            logic['description'] = 'Sends input to multiple outputs'
        
        # Lookup and reference
        elif trans_type == 'LOOKUP':
            logic['lookup_query'] = properties.get('SqlCommand', properties.get('SqlCommandVariable', ''))
            logic['cache_type'] = properties.get('CacheType', 'FULL')  # FULL, PARTIAL, NO_CACHE
            logic['connection_type'] = properties.get('ConnectionType', '')
            logic['no_match_behavior'] = properties.get('NoMatchBehavior', 'IGNORE')
        
        elif trans_type == 'FUZZY_LOOKUP':
            logic['reference_table'] = properties.get('ReferenceTableName', '')
            logic['similarity_threshold'] = properties.get('MinSimilarity', '0.8')
            logic['max_matches'] = properties.get('MaxOutputMatchesPerInput', '1')
        
        # Rowset transformations
        elif trans_type == 'AGGREGATE':
            logic['aggregations'] = properties.get('AggregationType', '')
            logic['group_by'] = properties.get('GroupBy', '')
            logic['keys'] = properties.get('Keys', '')
        
        elif trans_type == 'SORT':
            logic['sort_columns'] = properties.get('SortKeyPosition', '')
            logic['sort_order'] = properties.get('SortOrder', 'ASC')
            logic['remove_duplicates'] = properties.get('EliminateDuplicates', 'false')
        
        elif trans_type == 'PIVOT':
            logic['pivot_key'] = properties.get('PivotKeyValue', '')
            logic['set_key'] = properties.get('SetKeyValue', '')
        
        elif trans_type == 'UNPIVOT':
            logic['pivot_key_value'] = properties.get('PivotKeyValue', '')
        
        # Business intelligence
        elif trans_type == 'SCD':
            logic['scd_type'] = properties.get('ChangingAttributeType', 'TYPE_1')
            logic['business_key'] = properties.get('BusinessKey', '')
            logic['fixed_attribute'] = properties.get('FixedAttribute', '')
            logic['changing_attribute'] = properties.get('ChangingAttribute', '')
        
        # Auditing
        elif trans_type == 'AUDIT':
            logic['audit_type'] = properties.get('AuditType', '')
        
        elif trans_type == 'ROW_COUNT':
            logic['variable_name'] = properties.get('VariableName', '')
        
        # Script component
        elif trans_type == 'SCRIPT_COMPONENT':
            logic['script_language'] = properties.get('ScriptLanguage', 'C#')
            logic['read_only_variables'] = properties.get('ReadOnlyVariables', '')
            logic['read_write_variables'] = properties.get('ReadWriteVariables', '')
        
        # Add all other properties for custom/unknown transformations
        else:
            for key, value in properties.items():
                if value and key not in ['refId', 'name', 'componentClassID']:
                    logic[key] = value
        
        return logic
    
    def _extract_execution_order(self, root: ET.Element) -> List[Dict[str, str]]:
        """Extract task execution order from precedence constraints with proper task name and ID mapping."""
        execution_order = []
        
        # Build TWO mappings:
        # 1. DTSID (GUID) → task name (for internal use)
        # 2. refId (path) → DTSID (to resolve precedence constraints)
        dtsid_to_name = {}
        refid_to_dtsid = {}
        
        for executable in root.findall('.//{www.microsoft.com/SqlServer/Dts}Executable'):
            task_id = executable.get('{www.microsoft.com/SqlServer/Dts}DTSID', '')
            task_name = executable.get('{www.microsoft.com/SqlServer/Dts}ObjectName', '')
            ref_id = executable.get('{www.microsoft.com/SqlServer/Dts}refId', '')
            
            if task_id and task_name:
                dtsid_to_name[task_id] = task_name
            
            if ref_id and task_id:
                refid_to_dtsid[ref_id] = task_id
        
        # Now extract precedence constraints
        # Note: DTS:From and DTS:To contain refId paths (e.g., "Package\TaskName"), NOT DTSIDs
        for pc in root.findall('.//{www.microsoft.com/SqlServer/Dts}PrecedenceConstraint'):
            from_ref = pc.get('{www.microsoft.com/SqlServer/Dts}From', '')
            to_ref = pc.get('{www.microsoft.com/SqlServer/Dts}To', '')
            
            if from_ref and to_ref:
                # Convert refId to DTSID
                from_dtsid = refid_to_dtsid.get(from_ref, '')
                to_dtsid = refid_to_dtsid.get(to_ref, '')
                
                # Get task names from DTSIDs
                from_name = dtsid_to_name.get(from_dtsid, from_ref)
                to_name = dtsid_to_name.get(to_dtsid, to_ref)
                
                if from_dtsid and to_dtsid:
                    execution_order.append({
                        'from': from_name,
                        'to': to_name,
                        'from_id': from_dtsid,
                        'to_id': to_dtsid
                    })
        
        return execution_order

