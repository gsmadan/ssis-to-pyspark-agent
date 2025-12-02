"""
SSIS Package Parsing Module.

This module provides production-ready parsing for SSIS packages (.dtsx files).
Extracts data engineering relevant information including sources, transformations,
destinations, connections, and complete data flow lineage.

Usage:
    from parsing.data_engineering_parser import DataEngineeringParser
    
    parser = DataEngineeringParser()
    result = parser.parse_dtsx_file('path/to/package.dtsx')
"""

from .data_engineering_parser import DataEngineeringParser
from .enhanced_xml_processor import EnhancedXMLProcessor

__all__ = ["DataEngineeringParser", "EnhancedXMLProcessor"]

# Version info
__version__ = "2.0.0"
__author__ = "SSIS to PySpark Team"

# Convenience imports for users
# The main parser to use:
Parser = DataEngineeringParser
