"""
Main SSIS to PySpark mapper that orchestrates the mapping process.
"""
from typing import Dict, List, Any, Optional
import logging
from models import SSISPackage, GeneratedCode, PySparkMapping
from .component_mapper import ComponentMapper
from .data_flow_mapper import DataFlowMapper
from .control_flow_mapper import ControlFlowMapper

logger = logging.getLogger(__name__)


class SSISToPySparkMapper:
    """
    Main mapper for converting SSIS packages to PySpark equivalents.
    
    This class orchestrates the mapping process by:
    1. Mapping individual components
    2. Mapping data flows
    3. Mapping control flow
    4. Generating comprehensive PySpark mappings
    """
    
    def __init__(self, use_llm_fallback: bool = True):
        self.use_llm_fallback = use_llm_fallback
        self.component_mapper = ComponentMapper(use_llm_fallback=use_llm_fallback)
        self.data_flow_mapper = DataFlowMapper(use_llm_fallback=use_llm_fallback)
        self.control_flow_mapper = ControlFlowMapper(use_llm_fallback=use_llm_fallback)
    
    def map_package(self, ssis_package: SSISPackage) -> Dict[str, Any]:
        """
        Map an entire SSIS package to PySpark equivalents.
        
        Args:
            ssis_package: Parsed SSIS package to map
            
        Returns:
            Dictionary containing comprehensive PySpark mappings
        """
        try:
            logger.info(f"Starting to map SSIS package: {ssis_package.name}")
            
            # Map control flow
            control_flow_mapping = self.control_flow_mapper.map_control_flow(ssis_package.control_flow)
            
            # Map data flows
            data_flow_mappings = []
            for data_flow in ssis_package.data_flows:
                data_flow_mapping = self.data_flow_mapper.map_data_flow(data_flow)
                data_flow_mappings.append(data_flow_mapping)
            
            # Generate overall mapping structure
            overall_mapping = self._generate_overall_mapping(
                ssis_package,
                control_flow_mapping,
                data_flow_mappings
            )
            
            logger.info(f"Successfully mapped SSIS package: {ssis_package.name}")
            
            return overall_mapping
            
        except Exception as e:
            logger.error(f"Error mapping SSIS package {ssis_package.name}: {e}")
            raise
    
    def _generate_overall_mapping(self,
                                 ssis_package: SSISPackage,
                                 control_flow_mapping: Dict[str, Any],
                                 data_flow_mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate overall mapping structure."""
        
        # Collect all mappings
        all_mappings = []
        
        # Add control flow mappings
        for task_mapping in control_flow_mapping['tasks']:
            all_mappings.append(task_mapping)
        
        # Add data flow mappings
        for data_flow_mapping in data_flow_mappings:
            all_mappings.extend(data_flow_mapping['sources'])
            all_mappings.extend(data_flow_mapping['transformations'])
            all_mappings.extend(data_flow_mapping['destinations'])
        
        # Generate comprehensive mapping
        overall_mapping = {
            'package_info': {
                'name': ssis_package.name,
                'version': ssis_package.version,
                'properties': ssis_package.package_properties
            },
            'control_flow': control_flow_mapping,
            'data_flows': data_flow_mappings,
            'all_mappings': all_mappings,
            'statistics': self._calculate_mapping_statistics(all_mappings),
            'imports': self._collect_all_imports(all_mappings),
            'dependencies': self._collect_all_dependencies(all_mappings),
            'conversion_notes': self._generate_conversion_notes(ssis_package, all_mappings)
        }
        
        return overall_mapping
    
    def _calculate_mapping_statistics(self, all_mappings: List[PySparkMapping]) -> Dict[str, int]:
        """Calculate mapping statistics."""
        stats = {
            'total_components': len(all_mappings),
            'successfully_mapped': 0,
            'fallback_mappings': 0,
            'data_sources': 0,
            'transformations': 0,
            'destinations': 0,
            'tasks': 0
        }
        
        for mapping in all_mappings:
            if 'fallback' in mapping.pyspark_function:
                stats['fallback_mappings'] += 1
            else:
                stats['successfully_mapped'] += 1
            
            # Count by component type
            component_type = str(mapping.ssis_component.type)
            if 'DataSource' in component_type:
                stats['data_sources'] += 1
            elif 'Transformation' in component_type:
                stats['transformations'] += 1
            elif 'destination' in component_type.lower():
                stats['destinations'] += 1
            elif 'Task' in component_type:
                stats['tasks'] += 1
        
        return stats
    
    def _collect_all_imports(self, all_mappings: List[PySparkMapping]) -> List[str]:
        """Collect all required imports."""
        imports = set()
        
        # Add base imports
        imports.add("from pyspark.sql import SparkSession")
        imports.add("from pyspark.sql.functions import *")
        imports.add("from pyspark.sql.types import *")
        imports.add("import logging")
        imports.add("from datetime import datetime")
        
        # Collect imports from mappings
        for mapping in all_mappings:
            imports.update(mapping.imports)
        
        return sorted(list(imports))
    
    def _collect_all_dependencies(self, all_mappings: List[PySparkMapping]) -> List[str]:
        """Collect all required dependencies."""
        dependencies = set()
        
        # Add base dependencies
        dependencies.add("pyspark")
        
        # Collect dependencies from mappings
        for mapping in all_mappings:
            dependencies.update(mapping.dependencies)
        
        return sorted(list(dependencies))
    
    def _generate_conversion_notes(self, 
                                  ssis_package: SSISPackage,
                                  all_mappings: List[PySparkMapping]) -> List[str]:
        """Generate conversion notes and recommendations."""
        notes = []
        
        # Package-level notes
        notes.append(f"Converted SSIS package: {ssis_package.name}")
        notes.append(f"Package version: {ssis_package.version}")
        
        # Count fallback mappings
        fallback_count = sum(1 for mapping in all_mappings if 'fallback' in mapping.pyspark_function)
        if fallback_count > 0:
            notes.append(f"Warning: {fallback_count} components required fallback mappings")
            notes.append("Manual review and implementation required for fallback components")
        
        # Data flow notes
        if ssis_package.data_flows:
            notes.append(f"Found {len(ssis_package.data_flows)} data flow(s)")
            notes.append("Data flows converted to PySpark DataFrame operations")
        
        # Control flow notes
        if ssis_package.control_flow.tasks:
            notes.append(f"Found {len(ssis_package.control_flow.tasks)} control flow task(s)")
            notes.append("Control flow converted to Python execution logic")
        
        # Variable notes
        if ssis_package.control_flow.variables:
            notes.append(f"Found {len(ssis_package.control_flow.variables)} variable(s)")
            notes.append("Variables converted to Python variables")
        
        # Connection manager notes
        if ssis_package.control_flow.connection_managers:
            notes.append(f"Found {len(ssis_package.control_flow.connection_managers)} connection manager(s)")
            notes.append("Connection managers converted to PySpark connection configurations")
        
        # General recommendations
        notes.append("Recommendations:")
        notes.append("- Review and test all generated PySpark code")
        notes.append("- Update connection strings and credentials")
        notes.append("- Implement error handling for production use")
        notes.append("- Add logging and monitoring as needed")
        notes.append("- Consider performance optimization for large datasets")
        
        return notes
    
    def generate_mapping_summary(self, mapping_result: Dict[str, Any]) -> str:
        """Generate a human-readable mapping summary."""
        summary_lines = []
        
        # Package info
        package_info = mapping_result['package_info']
        summary_lines.append(f"SSIS Package: {package_info['name']}")
        summary_lines.append(f"Version: {package_info['version']}")
        summary_lines.append("")
        
        # Statistics
        stats = mapping_result['statistics']
        summary_lines.append("Mapping Statistics:")
        summary_lines.append(f"  Total Components: {stats['total_components']}")
        summary_lines.append(f"  Successfully Mapped: {stats['successfully_mapped']}")
        summary_lines.append(f"  Fallback Mappings: {stats['fallback_mappings']}")
        summary_lines.append(f"  Data Sources: {stats['data_sources']}")
        summary_lines.append(f"  Transformations: {stats['transformations']}")
        summary_lines.append(f"  Destinations: {stats['destinations']}")
        summary_lines.append(f"  Tasks: {stats['tasks']}")
        summary_lines.append("")
        
        # Data flows
        data_flows = mapping_result['data_flows']
        if data_flows:
            summary_lines.append(f"Data Flows ({len(data_flows)}):")
            for i, df in enumerate(data_flows):
                summary_lines.append(f"  {i+1}. {df['data_flow_name']}")
        summary_lines.append("")
        
        # Dependencies
        dependencies = mapping_result['dependencies']
        if dependencies:
            summary_lines.append("Required Dependencies:")
            for dep in dependencies:
                summary_lines.append(f"  - {dep}")
        summary_lines.append("")
        
        # Conversion notes
        notes = mapping_result['conversion_notes']
        if notes:
            summary_lines.append("Conversion Notes:")
            for note in notes:
                summary_lines.append(f"  - {note}")
        
        return "\n".join(summary_lines)
