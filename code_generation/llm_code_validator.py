"""
LLM-based code validator and refiner for PySpark code.
Validates and improves generated PySpark code to ensure Databricks compatibility
and maintain SSIS package control flow and data flow logic.
"""

from typing import Dict, List, Any, Optional
import logging
import time
import openai
import google.generativeai as genai
from config import get_llm_config
from code_generation.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class LLMCodeValidator:
    """Validates and refines PySpark code using Large Language Models."""
    
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
            logger.warning("No LLM provider available - validation will be disabled")
            self.client = None
            return
        
        if provider == 'gemini':
            if not self.llm_config['api_key']:
                logger.warning("Gemini API key not provided - validation will be disabled")
                self.client = None
                return
            genai.configure(api_key=self.llm_config['api_key'])
            self.gemini_client = genai.GenerativeModel(self.llm_config['model'])
            self.client = self.gemini_client
        elif provider == 'openai':
            if not self.llm_config['api_key']:
                logger.warning("OpenAI API key not provided - validation will be disabled")
                self.client = None
                return
            openai.api_key = self.llm_config['api_key']
            self.openai_client = openai.OpenAI(api_key=self.llm_config['api_key'])
            self.client = self.openai_client
        elif provider == 'databricks':
            try:
                self.databricks_client = DatabricksClient()
                self.client = self.databricks_client
                logger.info(f"Initialized Databricks client with endpoint: {self.databricks_client.endpoint_name}")
            except Exception as e:
                logger.warning(f"Failed to initialize Databricks client: {e} - validation will be disabled")
                self.client = None
        else:
            logger.warning(f"Unsupported LLM provider: {provider} - validation will be disabled")
            self.client = None
    
    def validate_and_refine_code(self, 
                                  pyspark_code: str, 
                                  package_name: str,
                                  parsed_json: Dict[str, Any] = None,
                                  mapping_details: Dict[str, Any] = None,
                                  databricks_mode: bool = True) -> str:
        """
        Validate and refine PySpark code using LLM.
        
        Args:
            pyspark_code: The generated PySpark code to validate
            package_name: Name of the SSIS package
            parsed_json: Parsed SSIS package JSON with extracted components
            mapping_details: Mapping details showing how components were mapped
            databricks_mode: Whether the code is for Databricks
            
        Returns:
            Refined PySpark code
        """
        if not self.client:
            logger.warning("LLM client not available - returning original code without validation")
            return pyspark_code
        
        try:
            # Prepare the prompt for code validation and refinement
            prompt = self._prepare_validation_prompt(pyspark_code, package_name, parsed_json, mapping_details, databricks_mode)
            
            # Call LLM for validation and refinement
            if self.current_provider == 'gemini':
                refined_code = self._call_gemini_validation(prompt)
            elif self.current_provider == 'databricks':
                refined_code = self._call_databricks_validation(prompt)
            else:
                refined_code = self._call_openai_validation(prompt)
            
            logger.info("Code validation and refinement completed successfully")
            return refined_code
            
        except Exception as e:
            logger.error(f"Error during LLM code validation: {e}")
            # Return original code if validation fails
            return pyspark_code
    
    def _prepare_validation_prompt(self, 
                                   pyspark_code: str, 
                                   package_name: str,
                                   parsed_json: Dict[str, Any] = None,
                                   mapping_details: Dict[str, Any] = None,
                                   databricks_mode: bool = True) -> str:
        """Prepare the prompt for LLM code validation."""
        
        import json
        
        # Include parsed JSON context
        parsed_context = ""
        if parsed_json:
            parsed_context = f"""
**Parsed SSIS Package Details:**
```json
{json.dumps(parsed_json, indent=2)}
```
"""
        
        # Include mapping details context
        mapping_context = ""
        if mapping_details:
            # Remove pyspark_code from mapping_details if present to avoid duplication
            mapping_copy = {k: v for k, v in mapping_details.items() if k != 'pyspark_code'}
            mapping_context = f"""
**Mapping Details:**
```json
{json.dumps(mapping_copy, indent=2)}
```
"""
        
        prompt = f"""You are an expert in PySpark analysis and code refinement for SSIS→Databricks conversions.

**Your mission**
1. Keep the converter's structure. Only fix syntax gaps or obvious mapping misses.
2. Verify the DataFrame lineage matches the SSIS data flow (no dropped assignments, keep variable names).
3. Enforce Databricks compatibility rules.

**CRITICAL: Preserve Schema and Table Names**
- **DO NOT** change any table names, schema names, or database references in the code.
- **DO NOT** revert table names back to original SSIS format (e.g., do NOT change `test_bronze.source.src_inputtable` back to `dbo.SRC_InputTable`).
- **DO NOT** change schema-qualified table names (e.g., keep `test_silver.destination.dst_generictable` exactly as is).
- All table references in `spark.sql()` calls, `spark.table()` calls, and `saveAsTable()` calls must remain EXACTLY as provided.
- If the input code has Databricks schema-qualified names (e.g., `schema.table`), keep them exactly as written.

**Always keep**
- Leading docstring `\"\"\"PySpark conversion...\"\"\"`.
- Comment block headed `# Connection Configurations`.
- The Connection Configurations should always be commented out.
- Every `# Step N:` section and its `logger.info` call.
- Existing `spark.sql(...)` calls for SQL tasks (do NOT rewrite as DataFrame code).
- **The exact SQL query structure and table names within `spark.sql()` calls.**
- DataFrame outputs coming from Conditional Splits, Lookups, OLE DB Commands, etc.

**SQL Query Preservation**
- Keep all SQL queries in `spark.sql()` calls EXACTLY as provided - do NOT reformat or restructure them.
- Do NOT change table names in SQL queries (e.g., FROM clauses, INTO clauses, JOIN clauses).
- Do NOT add or remove brackets around table names unless fixing a syntax error.
- Do NOT change the format of table references (e.g., if it's `schema.table`, keep it as `schema.table`).
- Preserve the exact whitespace and formatting of SQL queries.

**Databricks rules**
- Do **not** import or build `SparkSession`.
- Do **not** call `spark.stop()`.
- Use `spark.sql`, `spark.table`, PySpark Column APIs.

{parsed_context}

{mapping_context}

**Generated PySpark code**
```python
{pyspark_code}
```

**Validation checklist**
- Fix PySpark syntax issues only with Column APIs (e.g. use `.eqNullSafe`, `.isNull`, `.cast`).
- Keep Delta writes exactly as produced unless obviously wrong.
- Ensure every DataFrame produced is either used later or logged the same way as the converter output.
- Remove extra blank lines and redundant comments, but keep short transformation labels.
- **PRESERVE ALL TABLE NAMES AND SCHEMA REFERENCES EXACTLY AS PROVIDED.**

**Stored procedure handling**
- For OLE DB Commands: retain format
  ```
  # OLE DB Command: <name>
  # EXEC schema.proc ?, ...
  # @param = column
  output_df = input_df  # optional guard stays
  ```
  No extra commentary, no MERGE/Delta rewrites.
- For SQL tasks that are `EXEC` statements: emit exactly
  ```
  # EXEC schema.proc ...
  # TODO: Implement stored procedure call
  ```
- For variables fed by stored procs (e.g. `task_work_history_id`): reduce to at most
  ```
  # EXEC schema.proc ...
  task_work_history_id = <value or None>  # TODO: Populate with actual value
  ```

**Do not**
- Remove the header.
- Change execution order or rename outputs without cause.
- Introduce `MERGE INTO` or other write logic for stored procedure placeholders.
- Replace `spark.sql` tasks with manual DataFrame construction.
- **Change any table names, schema names, or database references.**
- **Reformat or restructure SQL queries in `spark.sql()` calls.**
- **Revert schema-qualified table names to original SSIS format.**

Return only the final PySpark code (no markdown, no narration)."""

        return prompt
    
    def _extract_transformations(self, pyspark_code: str) -> List[Dict[str, Any]]:
        """Extract individual transformations from the code for validation."""
        transformations = []
        lines = pyspark_code.split('\n')
        
        current_transformation = None
        current_comment = ""
        current_code = []
        
        for i, line in enumerate(lines):
            stripped_line = line.strip()
            
            # Check if this is a transformation comment
            if stripped_line.startswith('# Transformation:'):
                # Save previous transformation if exists
                if current_transformation:
                    transformations.append({
                        'name': current_transformation,
                        'comment': current_comment,
                        'code': '\n'.join(current_code),
                        'line': i
                    })
                
                # Start new transformation
                current_transformation = stripped_line.replace('# Transformation:', '').strip()
                current_comment = ""
                current_code = [line]
            
            # Collect lines for current transformation
            elif current_transformation and not stripped_line.startswith('#'):
                current_code.append(line)
            elif current_transformation:
                if stripped_line.startswith('#') and len(stripped_line) > 1:
                    current_comment += stripped_line + '\n'
        
        # Add last transformation
        if current_transformation:
            transformations.append({
                'name': current_transformation,
                'comment': current_comment,
                'code': '\n'.join(current_code),
                'line': len(lines)
            })
        
        return transformations
    
    def _call_gemini_validation(self, prompt: str, max_retries: int = 3) -> str:
        """Call Gemini API for code validation and refinement."""
        import threading
        
        def api_call_with_timeout(prompt, timeout_seconds, result_container, exception_container):
            """Make API call in a thread with timeout."""
            try:
                response = self.gemini_client.generate_content(prompt)
                result_container['response'] = response
            except Exception as e:
                exception_container['error'] = e
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Calling Gemini for code validation (attempt {attempt}/{max_retries})...")
                
                # Use threading to add timeout (180 seconds = 3 minutes)
                result_container = {}
                exception_container = {}
                
                api_thread = threading.Thread(
                    target=api_call_with_timeout,
                    args=(prompt, 180, result_container, exception_container),
                    daemon=True
                )
                api_thread.start()
                api_thread.join(timeout=180)  # Wait up to 3 minutes
                
                # Check if thread is still running (timeout occurred)
                if api_thread.is_alive():
                    logger.error("Gemini API call timed out after 180 seconds")
                    if attempt < max_retries:
                        logger.info(f"Retrying... (attempt {attempt + 1}/{max_retries})")
                        continue
                    else:
                        raise TimeoutError("Gemini API call timed out after multiple attempts")
                
                # Check for exceptions
                if 'error' in exception_container:
                    raise exception_container['error']
                
                # Get response
                if 'response' not in result_container:
                    raise RuntimeError("No response received from Gemini API")
                
                response = result_container['response']
                
                # Extract the code from response
                refined_code = response.text.strip()
                
                # Remove markdown code blocks if present
                if refined_code.startswith('```python'):
                    refined_code = refined_code[9:]
                if refined_code.startswith('```'):
                    refined_code = refined_code[3:]
                if refined_code.endswith('```'):
                    refined_code = refined_code[:-3]
                
                refined_code = refined_code.strip()
                
                logger.info("Successfully received refined code from Gemini")
                return refined_code
                
            except Exception as e:
                error_msg = str(e)
                
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 10)
                    logger.warning(f"Gemini API call failed (attempt {attempt}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Gemini API call failed after {max_retries} attempts: {e}")
                    raise
    
    def _call_openai_validation(self, prompt: str, max_retries: int = 3) -> str:
        """Call OpenAI API for code validation and refinement."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Calling OpenAI for code validation (attempt {attempt}/{max_retries})...")
                
                response = self.openai_client.chat.completions.create(
                    model=self.llm_config['model'],
                    messages=[
                        {"role": "system", "content": "You are an expert PySpark code reviewer specializing in SSIS to PySpark conversions. Return only the refined code without explanations."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=8000
                )
                
                refined_code = response.choices[0].message.content.strip()
                
                # Remove markdown code blocks if present
                if refined_code.startswith('```python'):
                    refined_code = refined_code[9:]
                if refined_code.startswith('```'):
                    refined_code = refined_code[3:]
                if refined_code.endswith('```'):
                    refined_code = refined_code[:-3]
                
                refined_code = refined_code.strip()
                
                logger.info("Successfully received refined code from OpenAI")
                return refined_code
                
            except Exception as e:
                error_msg = str(e)
                
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 10)
                    logger.warning(f"OpenAI API call failed (attempt {attempt}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

                else:
                    logger.error(f"OpenAI API call failed after {max_retries} attempts: {e}")
                    raise

    def _call_databricks_validation(self, prompt: str, max_retries: int = 3) -> str:
        """Call Databricks Model Serving for code validation and refinement."""
        system_instruction = "You are an expert PySpark code reviewer specializing in SSIS to PySpark conversions. Return only the refined code without explanations."
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Calling Databricks for code validation (attempt {attempt}/{max_retries})...")
                
                refined_code = self.databricks_client.generate_content(prompt, system_instruction)
                
                # Remove markdown code blocks if present
                if refined_code.startswith('```python'):
                    refined_code = refined_code[9:]
                if refined_code.startswith('```'):
                    refined_code = refined_code[3:]
                if refined_code.endswith('```'):
                    refined_code = refined_code[:-3]
                
                refined_code = refined_code.strip()
                
                logger.info("Successfully received refined code from Databricks")
                return refined_code
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 10)
                    logger.warning(f"Databricks API call failed (attempt {attempt}): {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Databricks API call failed after {max_retries} attempts: {e}")
                    raise
