"""
LLM client abstraction layer.
Currently implements Gemini, but designed to easily swap providers.
"""
import os
from abc import ABC, abstractmethod
from typing import Optional
import google.generativeai as genai


class LLMClient(ABC):
    """Base class for LLM providers."""
    
    @abstractmethod
    def generate(self, prompt: str, temperature: float = 0.0) -> str:
        """Generate text from a prompt."""
        pass


class GeminiClient(LLMClient):
    """Google Gemini implementation."""
    
    def __init__(self, api_key: Optional[str] = None, model_name: str = "gemini-2.0-flash"):
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(model_name)
    
    def generate(self, prompt: str, temperature: float = 0.0) -> str:
        """Generate text using Gemini."""
        generation_config = genai.types.GenerationConfig(
            temperature=temperature,
        )
        response = self.model.generate_content(prompt, generation_config=generation_config)
        return response.text


# Factory function for easy provider switching
def create_llm_client(provider: str = "gemini", **kwargs) -> LLMClient:
    """
    Create an LLM client based on provider name.
    
    Args:
        provider: One of 'gemini', 'openai', 'anthropic' (only gemini implemented for now)
        **kwargs: Provider-specific arguments
    
    Returns:
        LLMClient instance
    """
    if provider.lower() == "gemini":
        return GeminiClient(**kwargs)
    # To add OpenAI later:
    # elif provider.lower() == "openai":
    #     return OpenAIClient(**kwargs)
    else:
        raise ValueError(f"Unknown LLM provider: {provider}")
