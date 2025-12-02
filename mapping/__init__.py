"""
SSIS to PySpark Mapping Module.

This module provides functionality to map SSIS components to equivalent PySpark functions
and operations.
"""
from .ssis_to_pyspark_mapper import SSISToPySparkMapper
from .component_mapper import ComponentMapper

__all__ = ["SSISToPySparkMapper", "ComponentMapper"]
