import os
import requests
import json
import logging
from typing import Dict, Any, Optional
from config import settings

logger = logging.getLogger(__name__)

class DatabricksClient:
    """Client for interacting with Databricks Model Serving endpoints."""
    
    def __init__(self, host: str = None, token: str = None, endpoint_name: str = None):
        self.host = host or settings.databricks_host
        self.token = token or settings.databricks_token
        self.endpoint_name = endpoint_name or settings.databricks_endpoint
        
        if not self.host or not self.token:
            # Try to get from environment variables directly if not in settings
            self.host = self.host or os.environ.get("DATABRICKS_HOST")
            self.token = self.token or os.environ.get("DATABRICKS_TOKEN")
            
        if not self.host or not self.token:
            raise ValueError("Databricks Host and Token are required. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
            
        # Ensure host starts with https://
        if not self.host.startswith("http"):
            self.host = f"https://{self.host}"
            
        self.base_url = f"{self.host.rstrip('/')}/serving-endpoints/{self.endpoint_name}/invocations"
        
    def generate_content(self, prompt: str, system_instruction: str = None) -> str:
        """
        Generate content using the Databricks Model Serving endpoint.
        
        Args:
            prompt: The user prompt
            system_instruction: Optional system instruction (prepended to prompt for some models)
            
        Returns:
            Generated text content
        """
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Construct messages based on common chat format
        messages = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            "messages": messages,
            "max_tokens": settings.max_tokens,
            "temperature": settings.temperature
        }
        
        try:
            logger.info(f"Sending request to Databricks endpoint: {self.endpoint_name}")
            response = requests.post(self.base_url, headers=headers, json=payload, timeout=120)
            response.raise_for_status()
            
            result = response.json()
            
            # Extract content based on OpenAI-compatible response format (common for Databricks serving)
            if "choices" in result and len(result["choices"]) > 0:
                return result["choices"][0]["message"]["content"]
            else:
                logger.error(f"Unexpected response format from Databricks: {result}")
                return str(result)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling Databricks API: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise
