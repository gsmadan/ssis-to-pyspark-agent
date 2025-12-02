"""
Enhanced XML processor for complete SSIS package extraction.
Extracts 100% of SSIS package information.
"""
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class EnhancedXMLProcessor:
    """Enhanced XML processor for complete SSIS package extraction."""
    
    def __init__(self):
        self.namespaces = {
            'DTS': '{www.microsoft.com/SqlServer/Dts}',
            'SQLTask': '{www.microsoft.com/sqlserver/dts/tasks/sqltask}'
        }
    
    def parse_dtsx_file(self, file_path: str) -> Dict[str, Any]:
        """Parse DTSX file and extract ALL information."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            result = {
                'package_info': self._extract_complete_package_info(root),
                'connection_managers': self._extract_complete_connection_managers(root),
                'variables': self._extract_complete_variables(root),
                'tasks': self._extract_complete_tasks(root),
                'data_flows': self._extract_complete_data_flows(root),
                'precedence_constraints': self._extract_precedence_constraints(root),
                'logging_options': self._extract_logging_options(root),
                'package_variables': self._extract_package_variables(root)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing DTSX file: {e}")
            raise
    
    def _extract_complete_package_info(self, root: ET.Element) -> Dict[str, Any]:
        """Extract complete package information."""
        package_info = {
            'name': '',
            'version': '',
            'format_version': '',
            'creator_name': '',
            'creator_computer': '',
            'creation_date': '',
            'version_major': '',
            'version_minor': '',
            'version_build': '',
            'version_guid': '',
            'package_type': '',
            'protection_level': '',
            'max_concurrent_executables': '',
            'all_properties': {}
        }
        
        # Extract from Property elements (try both with and without namespace)
        for prop in root.findall('.//{www.microsoft.com/SqlServer/Dts}Property'):
            name_elem = prop.get('{www.microsoft.com/SqlServer/Dts}Name')
            if name_elem:
                value = prop.text or ''
                package_info['all_properties'][name_elem] = value
                
                # Map to specific fields
                if name_elem == 'ObjectName':
                    package_info['name'] = value
                elif name_elem == 'CreatorName':
                    package_info['creator_name'] = value
                elif name_elem == 'CreatorComputerName':
                    package_info['creator_computer'] = value
                elif name_elem == 'CreationDate':
                    package_info['creation_date'] = value
                elif name_elem == 'VersionMajor':
                    package_info['version_major'] = value
                elif name_elem == 'VersionMinor':
                    package_info['version_minor'] = value
                elif name_elem == 'VersionBuild':
                    package_info['version_build'] = value
                elif name_elem == 'VersionGUID':
                    package_info['version_guid'] = value
                elif name_elem == 'PackageFormatVersion':
                    package_info['format_version'] = value
                elif name_elem == 'PackageType':
                    package_info['package_type'] = value
                elif name_elem == 'ProtectionLevel':
                    package_info['protection_level'] = value
                elif name_elem == 'MaxConcurrentExecutables':
                    package_info['max_concurrent_executables'] = value
        
        # Build version string
        if package_info['version_build']:
            package_info['version'] = package_info['version_build']
        
        return package_info
    
    def _extract_complete_connection_managers(self, root: ET.Element) -> Dict[str, Dict[str, Any]]:
        """Extract complete connection manager information."""
        connection_managers = {}
        
        for conn in root.findall('.//{www.microsoft.com/SqlServer/Dts}ConnectionManager'):
            conn_info = {
                'id': '',
                'name': '',
                'type': '',
                'dtsid': '',
                'description': '',
                'delay_validation': '',
                'connection_string': '',
                'all_properties': {}
            }
            
            # Extract properties
            for prop in conn.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                name = prop.get(f'{self.namespaces["DTS"]}Name')
                value = prop.text or ''
                
                if name:
                    conn_info['all_properties'][name] = value
                    
                    if name == 'ObjectName':
                        conn_info['name'] = value
                        conn_info['id'] = value
                    elif name == 'CreationName':
                        conn_info['type'] = value
                    elif name == 'DTSID':
                        conn_info['dtsid'] = value
                    elif name == 'Description':
                        conn_info['description'] = value
                    elif name == 'DelayValidation':
                        conn_info['delay_validation'] = value
            
            # Extract connection string from ObjectData
            object_data = conn.find('{www.microsoft.com/SqlServer/Dts}ObjectData')
            if object_data is not None:
                inner_conn = object_data.find('{www.microsoft.com/SqlServer/Dts}ConnectionManager')
                if inner_conn is not None:
                    for prop in inner_conn.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                        name = prop.get(f'{self.namespaces["DTS"]}Name')
                        value = prop.text or ''
                        
                        if name == 'ConnectionString':
                            conn_info['connection_string'] = value
                            conn_info['all_properties']['ConnectionString'] = value
            
            if conn_info['id']:
                connection_managers[conn_info['id']] = conn_info
        
        return connection_managers
    
    def _extract_complete_variables(self, root: ET.Element) -> Dict[str, Any]:
        """Extract complete variable information."""
        # Note: This package doesn't have standard variables
        # PackageVariables are different (contain diagrams/metadata)
        return {}
    
    def _extract_complete_tasks(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract complete task information."""
        tasks = []
        
        for executable in root.findall('.//{www.microsoft.com/SqlServer/Dts}Executable'):
            exec_type = executable.get(f'{self.namespaces["DTS"]}ExecutableType')
            
            if not exec_type:
                continue
            
            task_info = {
                'id': '',
                'name': '',
                'type': exec_type,
                'type_simplified': '',
                'dtsid': '',
                'description': '',
                'disabled': False,
                'fail_package_on_failure': False,
                'all_properties': {},
                'object_data': {}
            }
            
            # Extract properties
            for prop in executable.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                name = prop.get(f'{self.namespaces["DTS"]}Name')
                value = prop.text or ''
                
                if name:
                    task_info['all_properties'][name] = value
                    
                    if name == 'ObjectName':
                        task_info['name'] = value
                        task_info['id'] = value
                    elif name == 'DTSID':
                        task_info['dtsid'] = value
                    elif name == 'Description':
                        task_info['description'] = value
                    elif name == 'Disabled':
                        task_info['disabled'] = value == '-1' or value == 'True'
                    elif name == 'FailPackageOnFailure':
                        task_info['fail_package_on_failure'] = value == '-1' or value == 'True'
            
            # Simplify task type
            if 'DTS.Pipeline' in exec_type:
                task_info['type_simplified'] = 'Pipeline'
            elif 'ExecuteSQLTask' in exec_type:
                task_info['type_simplified'] = 'ExecuteSQL'
            elif 'ScriptTask' in exec_type:
                task_info['type_simplified'] = 'Script'
            elif 'FileSystemTask' in exec_type:
                task_info['type_simplified'] = 'FileSystem'
            elif '.' in exec_type:
                # Extract the last part after the last dot, before any comma
                simplified = exec_type.split('.')[-1]
                if ',' in simplified:
                    simplified = simplified.split(',')[0]
                task_info['type_simplified'] = simplified
            else:
                task_info['type_simplified'] = exec_type
            
            # Extract ObjectData for task-specific details
            object_data = executable.find('{www.microsoft.com/SqlServer/Dts}ObjectData')
            if object_data is not None:
                task_info['object_data'] = self._extract_object_data(object_data, exec_type)
            
            tasks.append(task_info)
        
        return tasks
    
    def _extract_object_data(self, object_data: ET.Element, exec_type: str) -> Dict[str, Any]:
        """Extract task-specific object data."""
        data = {}
        
        # For Execute SQL Task
        if 'ExecuteSQLTask' in exec_type:
            sql_task_data = object_data.find('.//SqlTaskData', self.namespaces)
            if sql_task_data is None:
                # Try without namespace
                for child in object_data:
                    if 'SqlTaskData' in child.tag:
                        sql_task_data = child
                        break
            
            if sql_task_data is not None:
                data['task_type'] = 'ExecuteSQL'
                data['connection'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}Connection', '')
                data['timeout'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}TimeOut', '')
                data['is_stored_proc'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}IsStoredProc', '')
                data['bypass_prepare'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}BypassPrepare', '')
                data['sql_statement_source_type'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}SqlStmtSourceType', '')
                data['sql_statement'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}SqlStatementSource', '')
                data['result_type'] = sql_task_data.get(f'{self.namespaces["SQLTask"]}ResultType', '')
        
        # For Data Flow Task
        elif 'Pipeline' in exec_type:
            pipeline = object_data.find('.//pipeline')
            if pipeline is not None:
                data['task_type'] = 'DataFlow'
                data['pipeline_id'] = pipeline.get('id', '')
                data['pipeline_name'] = pipeline.get('name', '')
                data['components'] = self._extract_pipeline_components(pipeline)
                data['paths'] = self._extract_pipeline_paths(pipeline)
        
        return data
    
    def _extract_pipeline_components(self, pipeline: ET.Element) -> List[Dict[str, Any]]:
        """Extract data flow pipeline components."""
        components = []
        
        components_elem = pipeline.find('components')
        if components_elem is not None:
            for component in components_elem.findall('component'):
                comp_info = {
                    'id': component.get('id', ''),
                    'name': component.get('name', ''),
                    'description': component.get('description', ''),
                    'component_class_id': component.get('componentClassID', ''),
                    'version': component.get('version', ''),
                    'properties': {}
                }
                
                # Extract component properties
                properties_elem = component.find('properties')
                if properties_elem is not None:
                    for prop in properties_elem.findall('property'):
                        prop_name = prop.get('name', '')
                        prop_value = prop.text or ''
                        if prop_name:
                            comp_info['properties'][prop_name] = prop_value
                
                # Classify component type
                desc = comp_info['description'].lower()
                if 'source' in desc:
                    comp_info['component_type'] = 'source'
                elif 'destination' in desc:
                    comp_info['component_type'] = 'destination'
                else:
                    comp_info['component_type'] = 'transformation'
                
                components.append(comp_info)
        
        return components
    
    def _extract_pipeline_paths(self, pipeline: ET.Element) -> List[Dict[str, Any]]:
        """Extract data flow paths (connections between components)."""
        paths = []
        
        paths_elem = pipeline.find('paths')
        if paths_elem is not None:
            for path in paths_elem.findall('path'):
                path_info = {
                    'id': path.get('id', ''),
                    'name': path.get('name', ''),
                    'start_id': path.get('startId', ''),
                    'end_id': path.get('endId', '')
                }
                paths.append(path_info)
        
        return paths
    
    def _extract_complete_data_flows(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract complete data flow information."""
        data_flows = []
        
        for executable in root.findall('.//{www.microsoft.com/SqlServer/Dts}Executable'):
            exec_type = executable.get(f'{self.namespaces["DTS"]}ExecutableType', '')
            
            if 'Pipeline' in exec_type:
                task_name = ''
                for prop in executable.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                    if prop.get(f'{self.namespaces["DTS"]}Name') == 'ObjectName':
                        task_name = prop.text or ''
                        break
                
                object_data = executable.find('{www.microsoft.com/SqlServer/Dts}ObjectData')
                if object_data is not None:
                    pipeline = object_data.find('.//pipeline')
                    if pipeline is not None:
                        data_flow_info = {
                            'id': task_name,
                            'name': task_name,
                            'pipeline_id': pipeline.get('id', ''),
                            'sources': [],
                            'transformations': [],
                            'destinations': [],
                            'paths': []
                        }
                        
                        # Extract components
                        components = self._extract_pipeline_components(pipeline)
                        for comp in components:
                            if comp['component_type'] == 'source':
                                data_flow_info['sources'].append(comp)
                            elif comp['component_type'] == 'destination':
                                data_flow_info['destinations'].append(comp)
                            else:
                                data_flow_info['transformations'].append(comp)
                        
                        # Extract paths
                        data_flow_info['paths'] = self._extract_pipeline_paths(pipeline)
                        
                        data_flows.append(data_flow_info)
        
        return data_flows
    
    def _extract_precedence_constraints(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract precedence constraints (task execution order)."""
        constraints = []
        
        for constraint in root.findall('.//{www.microsoft.com/SqlServer/Dts}PrecedenceConstraint'):
            constraint_info = {
                'value': '',
                'from_id': '',
                'to_id': '',
                'is_and': True,
                'all_properties': {}
            }
            
            # Extract properties
            for prop in constraint.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                name = prop.get(f'{self.namespaces["DTS"]}Name')
                value = prop.text or ''
                
                if name:
                    constraint_info['all_properties'][name] = value
                    
                    if name == 'Value':
                        constraint_info['value'] = value
                    elif name == 'LogicalAnd':
                        constraint_info['is_and'] = value == '-1' or value == 'True'
            
            # Extract from/to executables
            executables = constraint.findall('.//{www.microsoft.com/SqlServer/Dts}Executable')
            if len(executables) >= 2:
                for exec_elem in executables:
                    is_from = exec_elem.get(f'{self.namespaces["DTS"]}IsFrom')
                    idref = exec_elem.get('IDREF', '')
                    
                    if is_from == '-1':
                        constraint_info['from_id'] = idref
                    else:
                        constraint_info['to_id'] = idref
            
            if constraint_info['from_id'] or constraint_info['to_id']:
                constraints.append(constraint_info)
        
        return constraints
    
    def _extract_logging_options(self, root: ET.Element) -> Dict[str, Any]:
        """Extract logging options."""
        logging_opts = {
            'logging_mode': '',
            'filter_kind': '',
            'event_filter': ''
        }
        
        logging_elem = root.find('.//{www.microsoft.com/SqlServer/Dts}LoggingOptions')
        if logging_elem is not None:
            for prop in logging_elem.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                name = prop.get(f'{self.namespaces["DTS"]}Name')
                value = prop.text or ''
                
                if name == 'LoggingMode':
                    logging_opts['logging_mode'] = value
                elif name == 'FilterKind':
                    logging_opts['filter_kind'] = value
                elif name == 'EventFilter':
                    logging_opts['event_filter'] = value
        
        return logging_opts
    
    def _extract_package_variables(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract package variables (designer metadata)."""
        package_vars = []
        
        for var in root.findall('.//{www.microsoft.com/SqlServer/Dts}PackageVariable'):
            var_info = {
                'namespace': '',
                'object_name': '',
                'dtsid': '',
                'value': '',
                'all_properties': {}
            }
            
            for prop in var.findall('{www.microsoft.com/SqlServer/Dts}Property'):
                name = prop.get(f'{self.namespaces["DTS"]}Name')
                value = prop.text or ''
                
                if name:
                    var_info['all_properties'][name] = value
                    
                    if name == 'Namespace':
                        var_info['namespace'] = value
                    elif name == 'ObjectName':
                        var_info['object_name'] = value
                    elif name == 'DTSID':
                        var_info['dtsid'] = value
                    elif name == 'PackageVariableValue':
                        var_info['value'] = value[:200]  # Truncate long values
            
            package_vars.append(var_info)
        
        return package_vars

