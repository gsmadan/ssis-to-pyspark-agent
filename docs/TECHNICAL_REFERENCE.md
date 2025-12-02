# Technical Reference - SSIS to PySpark Converter

**Last Updated:** October 20, 2025

---

## Parser Implementation

### Data Engineering Parser

**File:** `parsing/data_engineering_parser.py`

#### Key Methods

##### 1. Connection Extraction
```python
def _extract_connections(self, root: ET.Element) -> Dict[str, Any]
```
- Parses `DTS:ConnectionManagers` section
- Extracts server, database, provider details
- Maps connection IDs to connection names

##### 2. SQL Task Extraction
```python
def _extract_sql_tasks(self, root: ET.Element, connections: Dict) -> List[Dict[str, Any]]
```
- Finds all `Microsoft.ExecuteSQLTask` executables
- Extracts task ID (DTSID), name, SQL statement
- Links to connection managers
- **NEW:** Filters out disabled tasks (`DTS:Disabled="True"`)

##### 3. Data Flow Extraction
```python
def _extract_data_flows(self, root: ET.Element, connections: Dict) -> List[Dict[str, Any]]
```
- Finds all `Microsoft.Pipeline` executables
- Extracts pipeline components (sources, transformations, destinations)
- **NEW:** Filters out disabled tasks
- **NEW:** Extracts AccessMode for sources

##### 4. Execution Order Extraction
```python
def _extract_execution_order(self, root: ET.Element) -> List[Dict[str, str]]
```
- Parses `DTS:PrecedenceConstraints`
- Maps refId paths to DTSIDs
- Extracts from_id and to_id for dependencies

##### 5. Source Information
```python
def _extract_source_info(..., component: ET.Element) -> Dict[str, Any]
```
- **NEW:** Extracts AccessMode (1=table, 2=SQL query)
- **NEW:** Distinguishes `sql_query` from `table_name`
- **NEW:** Links to connection via `connectionManagerID`
- Supports: OpenRowset, SqlCommand, TableName properties

##### 6. Destination Information
```python
def _extract_destination_info(..., component: ET.Element) -> Dict[str, Any]
```
- **NEW:** Links to connection via `connectionManagerID`
- Extracts table name from OpenRowset property
- Determines write mode (append, overwrite, truncate)

---

## Mapper Implementation

### Enhanced JSON Mapper

**File:** `mapping/enhanced_json_mapper.py`

#### Key Improvements (October 2025)

##### 1. Connection Preservation
```python
def _map_sources(self, sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]
```
**NEW Fields Preserved:**
- `connection` - Connection info dict
- `is_sql_query` - Boolean flag
- `sql_query` - Full SQL query text
- `access_mode` - AccessMode value
- `component_id` - Component identifier

##### 2. Source Code Generation
```python
def _generate_source_code(self, source: Dict, mapped_connections: List) -> str
```
**Logic:**
1. Check `is_sql_query` flag
2. If SQL query: use `.option("query", """...""")`
3. If table: use `.option("dbtable", "...")`
4. Use connection variables from mapped_connections
5. Format SQL query comment (first line only to avoid syntax errors)

##### 3. Destination Code Generation
```python
def _generate_destination_code(self, dest: Dict, mapped_connections: List) -> str
```
**Logic:**
1. Get last DataFrame from flow tracking
2. Link to connection via connection name
3. Generate active write statement (not commented)
4. Use connection variables

##### 4. Execution Order Generation
```python
def _generate_ordered_execution_code(..., mapped_connections: List) -> str
```
**Algorithm:** Topological Sort (Kahn's Algorithm)
1. Build task lookup by task_id (DTSID)
2. Create dependency graph from precedence constraints
3. Calculate in-degree for each task
4. Process tasks with zero in-degree
5. Remove processed tasks and update in-degrees
6. Repeat until all tasks ordered

**Benefits:**
- Correct dependency resolution
- Handles complex dependency graphs
- Detects circular dependencies
- Respects parallel execution opportunities

##### 5. DataFrame Flow Tracking
```python
self.df_flow = {}  # Maps component_id -> df_variable_name

def _get_or_create_df_name(self, component_id: str, component_name: str) -> str
def _get_last_df_in_flow(self) -> str
```
- Tracks DataFrame variables through transformations
- Ensures correct variable names in destination writes
- Maintains data lineage

---

## Expression Translation

### Expression Translator

**File:** `mapping/expression_translator.py`

#### Supported SSIS Functions

##### Date Functions
- `GETDATE()` → `current_timestamp()`
- `DATEADD()` → `date_add()`, `add_months()`
- `DATEDIFF()` → `datediff()`, `months_between()`
- `YEAR()`, `MONTH()`, `DAY()` → `year()`, `month()`, `dayofmonth()`

##### String Functions
- `LEN()` → `length()`
- `SUBSTRING()` → `substring()`
- `UPPER()`, `LOWER()` → `upper()`, `lower()`
- `REPLACE()` → `regexp_replace()`
- `TRIM()`, `LTRIM()`, `RTRIM()` → `trim()`, `ltrim()`, `rtrim()`

##### Conditional Logic
- `(condition) ? true_val : false_val` → `when(...).otherwise(...)`
- `ISNULL()` → `coalesce()`

##### Type Conversions
- `(DT_I4)column` → `.cast("int")`
- `(DT_STR)column` → `.cast("string")`
- `(DT_DATE)column` → `.cast("date")`

---

## Code Quality Features

### Generated Code Includes

1. **Proper Structure**
   - Header with package metadata
   - Organized imports
   - Spark initialization
   - Connection configurations
   - Execution flow with comments
   - Footer with cleanup

2. **Logging**
   - INFO level by default
   - Step-by-step execution logs
   - Row count logging
   - Error context

3. **Comments**
   - Task names and purposes
   - Table/query information
   - TODO markers for manual configuration
   - Dependency notes

4. **Best Practices**
   - Adaptive query execution enabled
   - Partition coalescing
   - Proper JDBC options
   - Error handling structure

---

## SQL Sanitization

### Databricks Compatibility Fixes

```python
def _sanitize_sql_for_databricks(self, sql: str) -> str
```

**Transformations:**
1. `[schema].[table]` → `schema.table` (remove brackets)
2. `INSERT INTO table (cols)` → `INSERT INTO table(cols) VALUES`
3. `EXEC` → `CALL` (stored procedures)
4. Remove `GO` statements
5. Handle T-SQL specific syntax

---

## Performance Considerations

### Parser Performance
- Streams XML parsing (doesn't load entire file)
- Efficient XPath queries
- Minimal memory footprint

### Mapper Performance
- Rule-based mapping (fast)
- LLM fallback only when needed
- Cached expression translations

### Generated Code Performance
- Adaptive query execution enabled
- Partition optimization
- JDBC connection pooling support
- Efficient DataFrame operations

---

## Extension Points

### Adding New Transformations

1. Add to transformation mappings:
```python
self.transformation_mappings = {
    'NEW_TYPE': {
        'pyspark_function': 'custom_transformation',
        'code_template': '...'
    }
}
```

2. Implement in code generation:
```python
elif trans_type == 'NEW_TYPE':
    return f'''# Custom transformation code'''
```

### Adding New Source Types

1. Add to source mappings
2. Implement extraction in parser
3. Add code generation template

### Custom Expression Translation

Add to `expression_translator.py`:
```python
def translate_custom_function(self, expr: str) -> str:
    # Custom logic
    return pyspark_expr
```

---

## Error Handling

### Parser Errors
- Invalid XML format
- Missing required attributes
- Unsupported SSIS versions

### Mapper Errors
- Unmapped component types (LLM fallback)
- Missing connection information
- Invalid expression syntax

### Code Generation Errors
- Syntax validation failures
- Missing dependencies
- Invalid configuration

All errors are logged with context for debugging.

---

## Testing Strategy

### Unit Tests
- Parser component extraction
- Mapper transformation logic
- Expression translation

### Integration Tests
- End-to-end conversion
- Execution order validation
- Syntax validation

### Validation
- Python syntax check (AST parsing)
- PySpark compatibility check
- Databricks execution test

---

**For implementation examples, see the actual code in `parsing/` and `mapping/` directories.**


