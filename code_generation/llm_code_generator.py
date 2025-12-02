"""
LLM-based code generation for PySpark code from SSIS mappings.
"""
from typing import Dict, List, Any, Optional
import logging
import time
import openai
import google.generativeai as genai
from config import get_llm_config
from models import GeneratedCode, PySparkMapping, SSISComponent

logger = logging.getLogger(__name__)


class LLMCodeGenerator:
    """Generates PySpark code using Large Language Models."""
    
    def __init__(self):
        self.llm_config = get_llm_config()
        self.primary_provider = self.llm_config['provider']
        self.fallback_provider = 'gemini' if self.primary_provider == 'openai' else 'openai'
        self.current_provider = self.primary_provider
        self._initialize_llm_client()
    
    def _initialize_llm_client(self):
        """Initialize the LLM client based on configuration."""
        provider = self.llm_config['provider']
        
        if provider == 'none':
            logger.warning("No LLM provider available - LLM features disabled")
            self.client = None
            return
        
        if provider == 'openai':
            if not self.llm_config['api_key']:
                logger.warning("OpenAI API key not provided - LLM features disabled")
                self.client = None
                return
            openai.api_key = self.llm_config['api_key']
            self.openai_client = openai.OpenAI(api_key=self.llm_config['api_key'])
            self.client = self.openai_client
        elif provider == 'gemini':
            if not self.llm_config['api_key']:
                logger.warning("Gemini API key not provided - LLM features disabled")
                self.client = None
                return
            genai.configure(api_key=self.llm_config['api_key'])
            self.gemini_client = genai.GenerativeModel(self.llm_config['model'])
            self.client = self.gemini_client
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
        
        # Initialize fallback client
        self._initialize_fallback_client()
    
    def _initialize_fallback_client(self):
        """Initialize Gemini LLM client as the primary provider."""
        try:
            # Get Gemini API key from environment
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            gemini_key = os.getenv('GEMINI_API_KEY')
            
            if gemini_key:
                genai.configure(api_key=gemini_key)
                self.gemini_client = genai.GenerativeModel(self.llm_config.get('model', 'gemini-2.5-flash'))
                logger.info(f"Gemini client initialized with model: {self.llm_config.get('model', 'gemini-2.5-flash')}")
            else:
                self.gemini_client = None
                logger.warning("Gemini API key not found")
                
        except Exception as e:
            logger.warning(f"Failed to initialize Gemini client: {e}")
            self.gemini_client = None
    
    def generate_pyspark_code(self, 
                            mapping_result: Dict[str, Any],
                            ssis_package_info: Dict[str, Any]) -> GeneratedCode:
        """
        Generate PySpark code from SSIS mapping result.
        
        Args:
            mapping_result: Complete mapping result from SSIS to PySpark
            ssis_package_info: Original SSIS package information
            
        Returns:
            Generated PySpark code with metadata
        """
        # Check if LLM client is available
        if not self.client:
            logger.warning("LLM client not available - returning rule-based code only")
            return GeneratedCode(
                code=mapping_result.get('pyspark_code', ''),
                provider="none",
                model="none",
                success=True,
                metadata={
                    "llm_used": False,
                    "reason": "No LLM provider available"
                }
            )
        try:
            logger.info("Starting PySpark code generation using LLM")
            
            # Prepare context for LLM
            context = self._prepare_llm_context(mapping_result, ssis_package_info)
            
            # Generate code using LLM
            generated_code = self._call_llm_for_code_generation(context)
            
            # Process and structure the generated code
            structured_code = self._structure_generated_code(
                generated_code,
                mapping_result,
                ssis_package_info
            )
            
            logger.info("Successfully generated PySpark code using LLM")
            
            return structured_code
            
        except Exception as e:
            logger.error(f"Error generating PySpark code: {e}")
            raise
    
    def _prepare_llm_context(self, 
                            mapping_result: Dict[str, Any],
                            ssis_package_info: Dict[str, Any]) -> str:
        """Prepare context for LLM code generation."""
        
        context_parts = []
        
        # Package information
        package_info = mapping_result['package_info']
        context_parts.append(f"SSIS Package: {package_info['name']}")
        context_parts.append(f"Version: {package_info['version']}")
        context_parts.append("")
        
        # Control flow information
        control_flow = mapping_result['control_flow']
        if control_flow['tasks']:
            context_parts.append("Control Flow Tasks:")
            for i, task in enumerate(control_flow['tasks']):
                context_parts.append(f"  {i+1}. {task.ssis_component.name} ({task.ssis_component.type})")
                context_parts.append(f"     PySpark Function: {task.pyspark_function}")
                context_parts.append(f"     Description: {task.parameters.get('description', 'N/A')}")
            context_parts.append("")
        
        # Data flows information
        data_flows = mapping_result['data_flows']
        if data_flows:
            context_parts.append("Data Flows:")
            for i, df in enumerate(data_flows):
                context_parts.append(f"  Data Flow {i+1}: {df['data_flow_name']}")
                
                # Sources
                if df['sources']:
                    context_parts.append("    Sources:")
                    for source in df['sources']:
                        context_parts.append(f"      - {source.ssis_component.name} ({source.ssis_component.type})")
                        context_parts.append(f"        PySpark: {source.pyspark_function}")
                
                # Transformations
                if df['transformations']:
                    context_parts.append("    Transformations:")
                    for trans in df['transformations']:
                        context_parts.append(f"      - {trans.ssis_component.name} ({trans.ssis_component.type})")
                        context_parts.append(f"        PySpark: {trans.pyspark_function}")
                
                # Destinations
                if df['destinations']:
                    context_parts.append("    Destinations:")
                    for dest in df['destinations']:
                        context_parts.append(f"      - {dest.ssis_component.name} ({dest.ssis_component.type})")
                        context_parts.append(f"        PySpark: {dest.pyspark_function}")
                
                context_parts.append("")
        
        # Variables
        variables = control_flow.get('variables', {})
        if variables:
            context_parts.append("Variables:")
            for var_name, var_info in variables.items():
                context_parts.append(f"  {var_name}: {var_info['type']} = {var_info['value']}")
            context_parts.append("")
        
        # Connection managers
        connections = control_flow.get('connection_managers', {})
        if connections:
            context_parts.append("Connection Managers:")
            for conn_id, conn_info in connections.items():
                context_parts.append(f"  {conn_id}: {conn_info['type']}")
            context_parts.append("")
        
        # Statistics
        stats = mapping_result['statistics']
        context_parts.append("Mapping Statistics:")
        context_parts.append(f"  Total Components: {stats['total_components']}")
        context_parts.append(f"  Successfully Mapped: {stats['successfully_mapped']}")
        context_parts.append(f"  Fallback Mappings: {stats['fallback_mappings']}")
        context_parts.append("")
        
        return "\n".join(context_parts)
    
    def _call_llm_for_code_generation(self, context: str) -> str:
        """Call LLM to generate PySpark code."""
        
        prompt = self._create_code_generation_prompt(context)
        
        provider = self.llm_config['provider']
        
        if provider == 'openai':
            return self._call_openai(prompt)
        elif provider == 'gemini':
            return self._call_gemini(prompt)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
    
    def _create_code_generation_prompt(self, context: str) -> str:
        """Create prompt for LLM code generation."""
        
        return f"""
You are an expert PySpark developer tasked with converting SSIS packages to PySpark code for Databricks deployment.

Here is the SSIS package information and mappings:

{context}

Please generate a complete PySpark script that:
1. Implements all the control flow tasks
2. Handles all data flows with proper DataFrame operations
3. Includes proper error handling and logging
4. Uses best practices for Databricks deployment
5. Includes all necessary imports and configurations
6. Handles variables and connection managers appropriately
7. Is production-ready with proper exception handling

Requirements:
- Use PySpark 3.x syntax
- Include comprehensive error handling
- Add logging for monitoring
- Use best practices for performance
- Include comments explaining the conversion
- Make the code modular and maintainable
- Handle all data types properly
- Include proper resource cleanup

Generate the complete PySpark script:
"""
    
    def _call_openai(self, prompt: str, max_retries: int = 3) -> str:
        """Call OpenAI API for code generation with retry logic and Gemini fallback."""
        for attempt in range(max_retries):
            try:
                response = self.openai_client.chat.completions.create(
                    model=self.llm_config['model'],
                    messages=[
                        {"role": "system", "content": "You are an expert PySpark developer specializing in SSIS to PySpark conversions."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=self.llm_config['max_tokens'],
                    temperature=self.llm_config['temperature']
                )
                
                return response.choices[0].message.content
                
            except openai.RateLimitError as e:
                error_msg = str(e)
                logger.warning(f"OpenAI rate limit hit (attempt {attempt + 1}/{max_retries})")
                
                # Check if it's a quota error (insufficient_quota) - these need longer waits
                if 'insufficient_quota' in error_msg:
                    logger.warning("Insufficient quota detected - needs longer wait time for quota to reset")
                    wait_time = (attempt + 1) * 300  # 5 minutes, 10 minutes, 15 minutes for quota issues
                else:
                    wait_time = (attempt + 1) * 60  # 60s, 120s, 180s for standard rate limits
                
                if attempt < max_retries - 1:
                    logger.warning(f"Waiting {wait_time}s before retry {attempt + 2}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"OpenAI rate limit exceeded after {max_retries} attempts")
                    raise
            except Exception as e:
                logger.error(f"Error calling OpenAI API: {e}")
                raise
    
    def _call_gemini_fallback(self, prompt: str) -> str:
        """Call Gemini as fallback when primary provider fails."""
        try:
            # Configure generation parameters for Gemini 2.0 Flash
            generation_config = genai.types.GenerationConfig(
                max_output_tokens=8000,  # Gemini 2.0 Flash has higher limits
                temperature=self.llm_config['temperature']
            )
            
            # Create the full prompt with system context
            full_prompt = f"""You are an expert PySpark developer specializing in SSIS to PySpark conversions.

{prompt}

Please generate clean, production-ready PySpark code with proper error handling and comments."""
            
            response = self.gemini_fallback_client.generate_content(
                full_prompt,
                generation_config=generation_config
            )
            
            self.current_provider = 'gemini'
            logger.info("✅ Successfully used Gemini 2.0 Flash as fallback")
            return response.text
            
        except Exception as e:
            logger.error(f"Error calling Gemini fallback: {e}")
            raise
    
    
    def _call_gemini(self, prompt: str, max_retries: int = 3) -> str:
        """Call Gemini API for code generation with retry logic."""
        for attempt in range(max_retries):
            try:
                # Configure generation parameters
                generation_config = genai.types.GenerationConfig(
                    max_output_tokens=self.llm_config['max_tokens'],
                    temperature=self.llm_config['temperature']
                )
                
                # Create the full prompt with system context
                full_prompt = f"""You are an expert PySpark developer specializing in SSIS to PySpark conversions.

{prompt}

Please generate clean, production-ready PySpark code with proper error handling and comments."""
                
                response = self.client.generate_content(
                    full_prompt,
                    generation_config=generation_config
                )
                
                return response.text
                
            except Exception as e:
                error_str = str(e)
                
                # Handle HTTP 499 errors (client closed connection)
                if "499" in error_str or "finish_reason" in error_str.lower():
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5  # Shorter wait for connection issues
                        logger.warning(f"API connection issue. Waiting {wait_time}s before retry {attempt + 2}/{max_retries}...")
                        time.sleep(wait_time)
                    else:
                        logger.warning(f"API connection failed after {max_retries} attempts. Using fallback.")
                        return "output_df = input_df"  # Return simple fallback
                
                # Handle rate limits
                elif "429" in error_str or "quota" in error_str.lower() or "rate" in error_str.lower():
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 10  # Exponential backoff: 10s, 20s, 30s
                        logger.warning(f"Rate limit hit. Waiting {wait_time}s before retry {attempt + 2}/{max_retries}...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} attempts")
                        raise
                else:
                    logger.error(f"Error calling Gemini API: {e}")
                    raise
    
    def _structure_generated_code(self,
                                 generated_code: str,
                                 mapping_result: Dict[str, Any],
                                 ssis_package_info: Dict[str, Any]) -> GeneratedCode:
        """Structure the generated code into a GeneratedCode object."""
        
        # Extract imports from generated code
        imports = self._extract_imports(generated_code)
        
        # Extract dependencies
        dependencies = self._extract_dependencies(generated_code)
        
        # Create mappings list (simplified)
        mappings = []
        for mapping in mapping_result.get('all_mappings', []):
            mappings.append(mapping)
        
        # Create metadata
        metadata = {
            'ssis_package_name': mapping_result['package_info']['name'],
            'ssis_package_version': mapping_result['package_info']['version'],
            'generation_timestamp': self._get_current_timestamp(),
            'llm_provider': self.llm_config['provider'],
            'llm_model': self.llm_config['model'],
            'mapping_statistics': mapping_result['statistics'],
            'total_components': mapping_result['statistics']['total_components'],
            'successfully_mapped': mapping_result['statistics']['successfully_mapped'],
            'fallback_mappings': mapping_result['statistics']['fallback_mappings']
        }
        
        return GeneratedCode(
            code=generated_code,
            imports=imports,
            dependencies=dependencies,
            mappings=mappings,
            metadata=metadata
        )
    
    def _extract_imports(self, code: str) -> List[str]:
        """Extract import statements from generated code."""
        imports = []
        lines = code.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('import ') or line.startswith('from '):
                imports.append(line)
        
        return imports
    
    def _extract_dependencies(self, code: str) -> List[str]:
        """Extract dependencies from generated code."""
        dependencies = set()
        
        # Common PySpark dependencies
        if 'pyspark' in code.lower():
            dependencies.add('pyspark')
        
        if 'pandas' in code.lower():
            dependencies.add('pandas')
        
        if 'numpy' in code.lower():
            dependencies.add('numpy')
        
        if 'requests' in code.lower():
            dependencies.add('requests')
        
        if 'openpyxl' in code.lower():
            dependencies.add('openpyxl')
        
        if 'xmltodict' in code.lower():
            dependencies.add('xmltodict')
        
        return list(dependencies)
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def generate_code_for_component(self, mapping: PySparkMapping) -> str:
        """Generate PySpark code for a single component mapping."""
        
        component = mapping.ssis_component
        function = mapping.pyspark_function
        parameters = mapping.parameters
        
        # Create a more detailed prompt for better LLM understanding
        prompt = self._create_component_mapping_prompt(component, parameters)
        
        provider = self.llm_config['provider']
        
        if provider == 'openai':
            return self._call_openai(prompt)
        elif provider == 'gemini':
            return self._call_gemini(prompt)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
    
    def _create_component_mapping_prompt(self, component: SSISComponent, parameters: Dict[str, Any]) -> str:
        """Create a detailed prompt for component mapping."""
        
        prompt = f"""
You are an expert PySpark developer tasked with converting an SSIS component to PySpark code.

SSIS Component Details:
- Component ID: {component.id}
- Component Name: {component.name}
- Component Type: {component.type}
- Properties: {component.properties}
- Expressions: {component.expressions}

Task: Generate a complete PySpark function that implements the equivalent functionality of this SSIS component.

Requirements:
1. Create a Python function with a descriptive name based on the component
2. Include proper imports for PySpark
3. Add comprehensive error handling with try-except blocks
4. Include detailed comments explaining the conversion
5. Use PySpark best practices for performance
6. Handle all component properties and expressions appropriately
7. Make the function production-ready

Component Type Analysis:
"""
        
        # Add specific guidance based on component type
        if isinstance(component.type, str):
            component_type_str = str(component.type)
            
            if 'Task' in component_type_str:
                prompt += f"""
This is an SSIS Control Flow Task. Convert it to a PySpark function that:
- Executes the task logic
- Handles success/failure scenarios
- Returns appropriate status
- Includes logging for monitoring
"""
            elif 'Source' in component_type_str or 'DataSource' in component_type_str:
                prompt += f"""
This is an SSIS Data Source. Convert it to a PySpark function that:
- Reads data from the source system
- Returns a DataFrame
- Handles connection parameters
- Includes data validation
"""
            elif 'Transformation' in component_type_str:
                prompt += f"""
This is an SSIS Data Transformation. Convert it to a PySpark function that:
- Takes input DataFrame(s) as parameters
- Applies the transformation logic
- Returns transformed DataFrame
- Handles data type conversions
"""
            elif 'Destination' in component_type_str:
                prompt += f"""
This is an SSIS Data Destination. Convert it to a PySpark function that:
- Takes a DataFrame as input
- Writes data to the destination
- Handles write modes and options
- Includes error handling for write operations
"""
            else:
                prompt += f"""
This is a generic SSIS component. Convert it to a PySpark function that:
- Implements the component's core functionality
- Follows PySpark best practices
- Includes proper error handling
- Is well-documented
"""
        
        prompt += f"""

Generate the complete PySpark function:

```python
def [function_name]([parameters]):
    \"\"\"
    [Function description]
    
    Args:
        [Parameter descriptions]
    
    Returns:
        [Return description]
    \"\"\"
    try:
        # Implementation here
        pass
    except Exception as e:
        logger.error(f"Error in [function_name]: {{e}}")
        raise
```

Focus on creating production-ready PySpark code that accurately represents the SSIS component's functionality.
"""
        
        return prompt
    
    def validate_generated_code(self, code: str) -> Dict[str, Any]:
        """Validate generated PySpark code."""
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'suggestions': []
        }
        
        # Basic syntax validation
        try:
            compile(code, '<generated_code>', 'exec')
        except SyntaxError as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"Syntax error: {e}")
        
        # Check for common issues
        if 'TODO' in code:
            validation_result['warnings'].append("Code contains TODO comments - manual review required")
        
        if 'placeholder' in code.lower():
            validation_result['warnings'].append("Code contains placeholder text - manual implementation required")
        
        # Check for required imports
        required_imports = ['pyspark', 'SparkSession']
        for req_import in required_imports:
            if req_import not in code:
                validation_result['warnings'].append(f"Missing required import: {req_import}")
        
        return validation_result
