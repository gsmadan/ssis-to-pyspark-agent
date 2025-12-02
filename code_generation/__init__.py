"""
Code Generation Module.

This module provides LLM fallback functionality for complex SSIS expressions
that cannot be translated using rule-based methods.

Note: The main code generation is now handled by mapping.enhanced_json_mapper.
This module only provides LLM fallback support.
"""
from .llm_code_generator import LLMCodeGenerator

__all__ = ["LLMCodeGenerator"]
