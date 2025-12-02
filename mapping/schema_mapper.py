"""
Schema Mapper for SSIS to Databricks Schema Mapping

This module handles mapping SSIS connection names and table names to Databricks schema/table names.
"""
import json
import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class SchemaMapper:
    """
    Maps SSIS connection names and table names to Databricks schema/table names.
    
    Supports:
    - Connection-level schema mapping (all tables in a connection use the same schema)
    - Table-level overrides (specific table mappings override connection defaults)
    - SQL query transformation (replaces schema/table names in SQL statements)
    """
    
    def __init__(self, mapping_file_path: Optional[str] = None):
        """
        Initialize SchemaMapper.
        
        Args:
            mapping_file_path: Path to JSON mapping file. If None, mapper will be inactive.
        """
        self.mapping_file_path = mapping_file_path
        self.connection_mappings: Dict[str, Dict[str, Any]] = {}
        self.active = False
        
        if mapping_file_path:
            self.load_mapping_file(mapping_file_path)
    
    def load_mapping_file(self, file_path: str) -> bool:
        """
        Load and validate schema mapping file.
        
        Args:
            file_path: Path to JSON mapping file
            
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            mapping_path = Path(file_path)
            if not mapping_path.exists():
                logger.warning(f"Schema mapping file not found: {file_path}")
                return False
            
            with open(mapping_path, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
            
            # Validate structure
            if 'connection_mappings' not in mapping_data:
                logger.error("Schema mapping file missing 'connection_mappings' key")
                return False
            
            self.connection_mappings = mapping_data['connection_mappings']
            self.active = True
            
            logger.info(f"Schema mapping loaded: {len(self.connection_mappings)} connection(s) mapped")
            for conn_name in self.connection_mappings:
                logger.debug(f"  - {conn_name} -> {self.connection_mappings[conn_name].get('databricks_schema', 'N/A')}")
            
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in schema mapping file: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading schema mapping file: {e}")
            return False
    
    def get_databricks_table_name(self, ssis_connection: str, ssis_table: str) -> str:
        """
        Get Databricks table name for a given SSIS connection and table.
        
        Args:
            ssis_connection: SSIS connection name (e.g., "SRC_OLEDB")
            ssis_table: SSIS table name (e.g., "dbo.SRC_InputTable" or "SRC_InputTable")
            
        Returns:
            Mapped Databricks table name (e.g., "dev_bronze.regulatory.src_input_table")
            If no mapping exists, returns the original table name
        """
        if not self.active:
            return ssis_table
        
        # Normalize connection name (case-insensitive lookup)
        conn_key = self._find_connection_key(ssis_connection)
        if not conn_key:
            logger.debug(f"No mapping found for connection: {ssis_connection}")
            return ssis_table
        
        conn_mapping = self.connection_mappings[conn_key]
        databricks_schema = conn_mapping.get('databricks_schema', '')
        
        # Check for table-level override
        table_mappings = conn_mapping.get('table_mappings', {})
        
        # Try exact match first (with schema prefix)
        if ssis_table in table_mappings:
            mapped_table = table_mappings[ssis_table]
            # If mapped table already includes schema, return as-is
            if '.' in mapped_table:
                return mapped_table
            # Otherwise, prepend connection schema
            return f"{databricks_schema}.{mapped_table}" if databricks_schema else mapped_table
        
        # Try match without schema prefix (e.g., "SRC_InputTable" matches "dbo.SRC_InputTable")
        table_name_only = self._extract_table_name(ssis_table)
        for ssis_table_key, mapped_table in table_mappings.items():
            if self._extract_table_name(ssis_table_key) == table_name_only:
                if '.' in mapped_table:
                    return mapped_table
                return f"{databricks_schema}.{mapped_table}" if databricks_schema else mapped_table
        
        # No table-level mapping, use connection-level schema
        if databricks_schema:
            # Extract table name and apply schema
            table_name = self._extract_table_name(ssis_table)
            # Optionally convert to lowercase/snake_case for Databricks convention
            table_name_normalized = self._normalize_table_name(table_name)
            return f"{databricks_schema}.{table_name_normalized}"
        
        return ssis_table
    
    def find_connection_for_table(self, table_name: str) -> Optional[str]:
        """
        Find the connection that contains a mapping for the given table name.
        
        Args:
            table_name: Table name to search for (e.g., "dbo.SRC_GenericTable" or "SRC_GenericTable")
            
        Returns:
            Connection name if found, None otherwise
        """
        if not self.active:
            return None
        
        # Normalize table name
        table_name_normalized = table_name.replace('[', '').replace(']', '')
        table_name_only = self._extract_table_name(table_name_normalized)
        
        # Search through all connections
        for conn_name, conn_mapping in self.connection_mappings.items():
            table_mappings = conn_mapping.get('table_mappings', {})
            
            # Check exact match
            if table_name_normalized in table_mappings:
                return conn_name
            
            # Check table name only (without schema)
            for mapped_table in table_mappings.keys():
                if self._extract_table_name(mapped_table).lower() == table_name_only.lower():
                    return conn_name
        
        return None
    
    def apply_mapping_to_sql(self, sql_query: str, connection_name: Optional[str] = None) -> str:
        """
        Apply schema mappings to a SQL query string.
        
        Replaces schema.table references in SQL queries with mapped Databricks names.
        If connection_name is not provided, it will be inferred from table names in the SQL.
        
        Args:
            sql_query: Original SQL query string
            connection_name: SSIS connection name (optional, will be inferred if not provided)
            
        Returns:
            SQL query with schema/table names replaced
        """
        if not self.active:
            return sql_query
        
        # Build a global replacement map from all connections
        global_replacement_map = {}
        
        # If connection is provided, only use that connection's mappings
        if connection_name:
            conn_key = self._find_connection_key(connection_name)
            if not conn_key:
                return sql_query
            
            conn_mapping = self.connection_mappings[conn_key]
            databricks_schema = conn_mapping.get('databricks_schema', '')
            table_mappings = conn_mapping.get('table_mappings', {})
            
            # Build replacement map for this connection
            for ssis_table, databricks_table in table_mappings.items():
                ssis_table_normalized = ssis_table.replace('[', '').replace(']', '')
                # Build full Databricks table name with schema prefix
                if '.' in databricks_table:
                    # Already has schema, use as-is
                    full_databricks_name = databricks_table
                else:
                    # Prepend connection schema
                    full_databricks_name = f"{databricks_schema}.{databricks_table}" if databricks_schema else databricks_table
                
                global_replacement_map[ssis_table_normalized] = full_databricks_name
                
                # Also map without schema prefix
                table_name_only = self._extract_table_name(ssis_table)
                if table_name_only != ssis_table_normalized:
                    global_replacement_map[table_name_only] = full_databricks_name
        else:
            # No connection provided - search across ALL connections for table mappings
            # Extract table names from SQL (FROM, INTO, JOIN, UPDATE, TRUNCATE clauses)
            table_patterns = [
                r'(?:FROM|INTO|JOIN|UPDATE)\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?',
                r'TABLE\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?'
            ]
            
            # Search all connections for table mappings
            for conn_name, conn_mapping in self.connection_mappings.items():
                databricks_schema = conn_mapping.get('databricks_schema', '')
                table_mappings = conn_mapping.get('table_mappings', {})
                
                for ssis_table, databricks_table in table_mappings.items():
                    ssis_table_normalized = ssis_table.replace('[', '').replace(']', '')
                    
                    # Build full Databricks table name with schema prefix
                    if '.' in databricks_table:
                        # Already has schema, use as-is
                        full_databricks_name = databricks_table
                    else:
                        # Prepend connection schema
                        full_databricks_name = f"{databricks_schema}.{databricks_table}" if databricks_schema else databricks_table
                    
                    # Add to global map (use first match if table appears in multiple connections)
                    if ssis_table_normalized not in global_replacement_map:
                        global_replacement_map[ssis_table_normalized] = full_databricks_name
                    
                    # Also map without schema prefix
                    table_name_only = self._extract_table_name(ssis_table)
                    if table_name_only not in global_replacement_map:
                        global_replacement_map[table_name_only] = full_databricks_name
        
        # If no mappings found, return original SQL
        if not global_replacement_map:
            logger.debug("No table mappings found in schema mapping file")
            return sql_query
        
        # Apply replacements in SQL query
        result_sql = sql_query
        
        # Track fully qualified table names that were created in bracket pattern replacement
        # This prevents double replacement in the global replacement map section
        replaced_qualified_names = set()
        
        # Handle schema.table format with brackets [schema].[table] FIRST (before word boundary matching)
        # Search across all connections for bracket patterns
        for conn_name, conn_mapping in self.connection_mappings.items():
            databricks_schema = conn_mapping.get('databricks_schema', '')
            table_mappings = conn_mapping.get('table_mappings', {})
            
            for ssis_table, databricks_table in table_mappings.items():
                if '.' in ssis_table:
                    schema_part, table_part = ssis_table.split('.', 1)
                    
                    # Build full Databricks name
                    if '.' in databricks_table:
                        full_databricks_name = databricks_table
                    else:
                        full_databricks_name = f"{databricks_schema}.{databricks_table}" if databricks_schema else databricks_table
                    
                    # Track this qualified name
                    replaced_qualified_names.add(full_databricks_name.lower())
                    # Also track the table name part to prevent replacing it later
                    table_name_only = self._extract_table_name(databricks_table)
                    replaced_qualified_names.add(table_name_only.lower())
                    
                    # Match [schema].[table] pattern
                    bracket_pattern = rf'\[{re.escape(schema_part)}\]\.\[{re.escape(table_part)}\]'
                    result_sql = re.sub(bracket_pattern, full_databricks_name, result_sql, flags=re.IGNORECASE)
                    # Also match schema.[table] pattern
                    bracket_pattern2 = rf'{re.escape(schema_part)}\.\[{re.escape(table_part)}\]'
                    result_sql = re.sub(bracket_pattern2, full_databricks_name, result_sql, flags=re.IGNORECASE)
                    # Match [schema].table pattern
                    bracket_pattern3 = rf'\[{re.escape(schema_part)}\]\.{re.escape(table_part)}'
                    result_sql = re.sub(bracket_pattern3, full_databricks_name, result_sql, flags=re.IGNORECASE)
        
        # Replace table references using the global replacement map
        # Pattern: [schema].[table] or schema.table or table
        # IMPORTANT: Only replace standalone table names, not ones already in schema.table format
        for ssis_ref, databricks_ref in global_replacement_map.items():
            # Skip if this is a schema.table format (already handled above in bracket pattern section)
            if '.' in ssis_ref:
                continue
            
            # Skip if this table name is already part of a replaced qualified name
            if ssis_ref.lower() in replaced_qualified_names:
                continue
            
            # Escape special regex characters
            ssis_ref_escaped = re.escape(ssis_ref)
            
            # Pattern 1: Match standalone table name (not part of schema.table)
            # Use a callback to check if the match is already part of a qualified name
            def make_replacer(ref, replacement):
                def replacer(match):
                    matched_text = match.group(0)
                    start = match.start()
                    # Check if this match is part of a qualified name (preceded by a dot)
                    if start > 0:
                        # Check the character immediately before the match
                        char_before = result_sql[start - 1]
                        # If preceded by a dot, it's already part of a qualified name
                        if char_before == '.':
                            return matched_text  # Already qualified, don't replace
                    return replacement
                return replacer
            
            # Match table name with word boundaries
            pattern1 = rf'\b{ssis_ref_escaped}\b'
            result_sql = re.sub(pattern1, make_replacer(ssis_ref, databricks_ref), result_sql, flags=re.IGNORECASE)
            
            # Pattern 2: With brackets [table] - only standalone (not part of schema.[table])
            bracket_pattern = rf'\[{re.escape(ssis_ref)}\]'
            result_sql = re.sub(bracket_pattern, make_replacer(ssis_ref, databricks_ref), result_sql, flags=re.IGNORECASE)
        
        return result_sql
    
    def _find_connection_key(self, connection_name: str) -> Optional[str]:
        """
        Find connection key in mappings (case-insensitive).
        
        Args:
            connection_name: Connection name to find
            
        Returns:
            Matching key from connection_mappings, or None
        """
        connection_name_lower = connection_name.lower()
        for key in self.connection_mappings.keys():
            if key.lower() == connection_name_lower:
                return key
        return None
    
    def _extract_table_name(self, table_ref: str) -> str:
        """
        Extract table name from schema.table reference.
        
        Args:
            table_ref: Table reference (e.g., "dbo.SRC_InputTable" or "[dbo].[SRC_InputTable]")
            
        Returns:
            Table name only (e.g., "SRC_InputTable")
        """
        # Remove brackets
        table_ref = table_ref.replace('[', '').replace(']', '')
        
        # Split by dot and take last part
        parts = table_ref.split('.')
        return parts[-1] if parts else table_ref
    
    def _normalize_table_name(self, table_name: str) -> str:
        """
        Normalize table name for Databricks (optional: convert to lowercase/snake_case).
        
        Args:
            table_name: Original table name
            
        Returns:
            Normalized table name
        """
        # For now, just return as-is. Can be enhanced to convert to snake_case if needed
        return table_name
    
    def is_active(self) -> bool:
        """Check if schema mapper is active (has loaded mappings)."""
        return self.active


