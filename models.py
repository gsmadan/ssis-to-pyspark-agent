"""
Data models for SSIS to PySpark conversion system.
"""
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field
from enum import Enum


class SSISTaskType(str, Enum):
    """SSIS task types."""
    DATA_FLOW_TASK = "DataFlowTask"
    EXECUTE_SQL_TASK = "ExecuteSQLTask"
    SCRIPT_TASK = "ScriptTask"
    FILE_SYSTEM_TASK = "FileSystemTask"
    FTP_TASK = "FTPTask"
    WEB_SERVICE_TASK = "WebServiceTask"
    EXPRESSION_TASK = "ExpressionTask"
    FOR_LOOP_CONTAINER = "ForLoopContainer"
    FOREACH_LOOP_CONTAINER = "ForeachLoopContainer"
    SEQUENCE_CONTAINER = "SequenceContainer"


class DataSourceType(str, Enum):
    """Data source types."""
    SQL_SERVER = "SqlServer"
    ORACLE = "Oracle"
    MYSQL = "MySQL"
    POSTGRESQL = "PostgreSQL"
    CSV = "CSV"
    EXCEL = "Excel"
    FLAT_FILE = "FlatFile"
    XML = "XML"
    JSON = "JSON"


class TransformationType(str, Enum):
    """Data transformation types."""
    LOOKUP = "Lookup"
    MERGE_JOIN = "MergeJoin"
    CONDITIONAL_SPLIT = "ConditionalSplit"
    DERIVED_COLUMN = "DerivedColumn"
    DATA_CONVERSION = "DataConversion"
    SORT = "Sort"
    AGGREGATE = "Aggregate"
    UNION_ALL = "UnionAll"
    MULTICAST = "Multicast"
    ROW_COUNT = "RowCount"
    OLE_DB_COMMAND = "OleDbCommand"
    # Additional transformation types found in SSIS packages
    CHECKSUM = "Checksum"
    TRASH_DESTINATION = "TrashDestination"
    RC_SELECT = "RC_SELECT"
    RC_UPDATE = "RC_UPDATE"
    RC_DELETE = "RC_DELETE"
    RC_INSERT = "RC_INSERT"
    RC_INTERMEDIATE = "RC_INTERMEDIATE"
    GENERIC_TRANSFORMATION = "GenericTransformation"


class SSISComponent(BaseModel):
    """Represents an SSIS component."""
    id: str
    name: str
    type: Union[SSISTaskType, DataSourceType, TransformationType]
    properties: Dict[str, Any] = Field(default_factory=dict)
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    expressions: Dict[str, str] = Field(default_factory=dict)


class DataFlow(BaseModel):
    """Represents a data flow within SSIS."""
    id: str
    name: str
    sources: List[SSISComponent] = Field(default_factory=list)
    transformations: List[SSISComponent] = Field(default_factory=list)
    destinations: List[SSISComponent] = Field(default_factory=list)
    paths: List[Dict[str, str]] = Field(default_factory=list)  # source_id -> destination_id


class ControlFlow(BaseModel):
    """Represents control flow logic."""
    tasks: List[SSISComponent] = Field(default_factory=list)
    precedence_constraints: List[Dict[str, str]] = Field(default_factory=list)
    variables: Dict[str, Any] = Field(default_factory=dict)
    connection_managers: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class SSISPackage(BaseModel):
    """Represents a parsed SSIS package."""
    name: str
    version: str
    data_flows: List[DataFlow] = Field(default_factory=list)
    control_flow: ControlFlow
    package_properties: Dict[str, Any] = Field(default_factory=dict)


class PySparkMapping(BaseModel):
    """Mapping from SSIS component to PySpark equivalent."""
    ssis_component: SSISComponent
    pyspark_function: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    imports: List[str] = Field(default_factory=list)


class GeneratedCode(BaseModel):
    """Generated PySpark code with metadata."""
    code: str
    imports: List[str] = Field(default_factory=list)
    dependencies: List[str] = Field(default_factory=list)
    mappings: List[PySparkMapping] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ValidationResult(BaseModel):
    """Result of code validation."""
    is_valid: bool
    syntax_errors: List[str] = Field(default_factory=list)
    logic_warnings: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    coverage_score: float = Field(default=0.0)


class ConversionResult(BaseModel):
    """Final result of SSIS to PySpark conversion."""
    input_file: str
    output_file: str
    generated_code: GeneratedCode
    validation_result: ValidationResult
    conversion_metadata: Dict[str, Any] = Field(default_factory=dict)
    success: bool = Field(default=False)
    error_message: Optional[str] = None
