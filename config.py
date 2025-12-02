"""
Configuration management for SSIS to PySpark conversion system.
"""
import os
import getpass
from typing import Dict, Any, Optional
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Application settings and configuration."""
    
    # LLM Configuration
    openai_api_key: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    gemini_api_key: Optional[str] = Field(default=None, env="GEMINI_API_KEY")
    default_llm_provider: str = Field(default="auto", env="DEFAULT_LLM_PROVIDER")  # Changed to "auto" for dynamic detection
    default_model: str = Field(default="gemini-2.5-flash", env="DEFAULT_MODEL")
    
    # Code Generation Settings
    max_tokens: int = Field(default=4000, env="MAX_TOKENS")
    temperature: float = Field(default=0.1, env="TEMPERATURE")
    
    # System Settings
    output_directory: str = Field(default="./output", env="OUTPUT_DIRECTORY")
    temp_directory: str = Field(default="./temp", env="TEMP_DIRECTORY")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Validation Settings
    enable_syntax_check: bool = Field(default=True, env="ENABLE_SYNTAX_CHECK")
    enable_logic_validation: bool = Field(default=True, env="ENABLE_LOGIC_VALIDATION")
    
    # Databricks Integration
    databricks_host: Optional[str] = Field(default=None, env="DATABRICKS_HOST")
    databricks_token: Optional[str] = Field(default=None, env="DATABRICKS_TOKEN")
    databricks_endpoint: str = Field(default="databricks-dbrx-instruct", env="DATABRICKS_ENDPOINT")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()


def detect_available_provider() -> str:
    """Detect which LLM provider has a valid API key."""
    if settings.openai_api_key and settings.gemini_api_key:
        # Both available - prefer the one specified in DEFAULT_LLM_PROVIDER
        if settings.default_llm_provider in ["openai", "gemini"]:
            return settings.default_llm_provider
        else:
            # Default to Gemini if both available and no preference
            return "gemini"
    elif settings.openai_api_key:
        return "openai"
    elif settings.gemini_api_key:
        return "gemini"
    else:
        return "none"

def get_llm_config() -> Dict[str, Any]:
    """Get LLM configuration based on available API keys."""
    # Auto-detect provider if set to "auto"
    if settings.default_llm_provider == "auto":
        provider = detect_available_provider()
    else:
        provider = settings.default_llm_provider
    
    if provider == "openai":
        if not settings.openai_api_key:
            raise ValueError("OpenAI API key not provided")
        return {
            "provider": "openai",
            "model": "gpt-4",  # Use GPT-4 for OpenAI
            "api_key": settings.openai_api_key,
            "max_tokens": settings.max_tokens,
            "temperature": settings.temperature
        }
    
    elif provider == "gemini":
        if not settings.gemini_api_key:
            raise ValueError("Gemini API key not provided")
        return {
            "provider": "gemini",
            "model": settings.default_model,
            "api_key": settings.gemini_api_key,
            "max_tokens": settings.max_tokens,
            "temperature": settings.temperature
        }
    
    elif provider == "none":
        return {
            "provider": "none",
            "model": "none",
            "api_key": None,
            "max_tokens": 0,
            "temperature": 0.0
        }
    
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")

def prompt_for_api_key(provider: str) -> str:
    """Prompt user for API key in a secure way."""
    print(f"\n🔑 {provider.upper()} API Key Required")
    print(f"The converter uses {provider} for advanced code validation and refinement.")
    print(f"Get your API key from:")
    if provider == "gemini":
        print("  https://makersuite.google.com/app/apikey")
    else:
        print("  https://platform.openai.com/api-keys")
    print()
    
    api_key = getpass.getpass(f"Enter your {provider.upper()} API key: ").strip()
    
    if not api_key:
        print(f"❌ No API key provided. {provider.upper()} features will be disabled.")
        return None
    
    print(f"✅ {provider.upper()} API key received.")
    return api_key

def save_api_key_to_env(provider: str, api_key: str):
    """Save API key to .env file."""
    env_file = Path('.env')
    
    # Read existing .env file
    existing_content = ""
    if env_file.exists():
        with open(env_file, 'r') as f:
            existing_content = f.read()
    
    # Update or add API key
    lines = existing_content.split('\n') if existing_content else []
    updated_lines = []
    key_added = False
    provider_updated = False
    
    for line in lines:
        if line.startswith(f'{provider.upper()}_API_KEY='):
            updated_lines.append(f'{provider.upper()}_API_KEY={api_key}')
            key_added = True
        elif line.startswith('DEFAULT_LLM_PROVIDER='):
            updated_lines.append(f'DEFAULT_LLM_PROVIDER={provider}')
            provider_updated = True
        else:
            updated_lines.append(line)
    
    if not key_added:
        updated_lines.append(f'{provider.upper()}_API_KEY={api_key}')
    if not provider_updated:
        updated_lines.append(f'DEFAULT_LLM_PROVIDER={provider}')
    
    # Write updated .env file
    with open(env_file, 'w') as f:
        f.write('\n'.join(updated_lines))

def ensure_api_keys():
    """Ensure API keys are available, prompt if missing."""
    # Check current status
    provider = detect_available_provider()
    
    if provider == "none":
        # No API keys available - prompt user to choose
        print("\n🤖 AI Features Setup")
        print("The converter can use AI for advanced code validation and refinement.")
        print("Choose your preferred AI provider:")
        print("1. OpenAI (GPT-4) - Paid service")
        print("2. Google Gemini - Free tier available")
        print("3. Skip - Use rule-based conversion only")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == "1":
            api_key = prompt_for_api_key("openai")
            if api_key:
                settings.openai_api_key = api_key
                settings.default_llm_provider = "openai"
                # Save to .env file
                save_api_key_to_env("openai", api_key)
        elif choice == "2":
            api_key = prompt_for_api_key("gemini")
            if api_key:
                settings.gemini_api_key = api_key
                settings.default_llm_provider = "gemini"
                # Save to .env file
                save_api_key_to_env("gemini", api_key)
        elif choice == "3":
            print("✅ Skipping AI setup. Using rule-based conversion only.")
            settings.default_llm_provider = "none"
        else:
            print("❌ Invalid choice. Using rule-based conversion only.")
            settings.default_llm_provider = "none"
    
    return settings

def ensure_directories():
    """Ensure required directories exist."""
    os.makedirs(settings.output_directory, exist_ok=True)
    os.makedirs(settings.temp_directory, exist_ok=True)
