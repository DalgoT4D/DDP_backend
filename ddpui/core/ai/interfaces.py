"""
AI Provider interfaces and abstract base classes for Dalgo platform.
This module defines the contracts for different AI service providers.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Union, AsyncGenerator
from enum import Enum
from dataclasses import dataclass


class AIProviderType(Enum):
    """Supported AI provider types."""

    OPENAI = "openai"
    CLAUDE = "claude"
    OLLAMA = "ollama"


@dataclass
class AIMessage:
    """Standardized message format for AI interactions."""

    role: str  # "user", "assistant", "system"
    content: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class AIResponse:
    """Standardized response format from AI providers."""

    content: str
    usage: Optional[Dict[str, int]] = None  # tokens used
    model: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    provider: Optional[str] = None


@dataclass
class StreamingAIResponse:
    """Response format for streaming AI interactions."""

    content: str
    is_complete: bool
    usage: Optional[Dict[str, int]] = None
    metadata: Optional[Dict[str, Any]] = None


class AIProviderInterface(ABC):
    """
    Abstract base class defining the interface for all AI providers.
    This ensures consistent behavior across different AI services.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the AI provider with configuration.

        Args:
            config: Provider-specific configuration including API keys, endpoints, etc.
        """
        self.config = config
        self._validate_config(config)

    @abstractmethod
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate provider-specific configuration.

        Args:
            config: Configuration to validate

        Raises:
            ValueError: If configuration is invalid
        """
        pass

    @abstractmethod
    def chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """
        Generate a chat completion response.

        Args:
            messages: List of messages in conversation
            model: Model name to use (provider-specific)
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            **kwargs: Additional provider-specific parameters

        Returns:
            AIResponse containing the generated content

        Raises:
            Exception: If the API call fails
        """
        pass

    @abstractmethod
    def stream_chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AsyncGenerator[StreamingAIResponse, None]:
        """
        Generate a streaming chat completion response.

        Args:
            messages: List of messages in conversation
            model: Model name to use (provider-specific)
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            **kwargs: Additional provider-specific parameters

        Yields:
            StreamingAIResponse objects containing partial content

        Raises:
            Exception: If the API call fails
        """
        pass

    @abstractmethod
    def completion(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """
        Generate a completion response for a prompt.

        Args:
            prompt: Input prompt text
            model: Model name to use (provider-specific)
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            **kwargs: Additional provider-specific parameters

        Returns:
            AIResponse containing the generated content

        Raises:
            Exception: If the API call fails
        """
        pass

    @abstractmethod
    def get_available_models(self) -> List[str]:
        """
        Get list of available models for this provider.

        Returns:
            List of model names

        Raises:
            Exception: If unable to fetch models
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the AI provider is accessible and healthy.

        Returns:
            True if provider is healthy, False otherwise
        """
        pass

    def get_provider_type(self) -> AIProviderType:
        """
        Get the provider type.

        Returns:
            AIProviderType enum value
        """
        return self._provider_type

    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration.

        Returns:
            Current configuration dictionary
        """
        return self.config.copy()


class AIProviderException(Exception):
    """Base exception for AI provider related errors."""

    def __init__(self, message: str, provider: str, original_error: Optional[Exception] = None):
        self.provider = provider
        self.original_error = original_error
        super().__init__(f"[{provider}] {message}")


class AIProviderConfigurationError(AIProviderException):
    """Raised when provider configuration is invalid."""

    pass


class AIProviderConnectionError(AIProviderException):
    """Raised when unable to connect to AI provider."""

    pass


class AIProviderRateLimitError(AIProviderException):
    """Raised when rate limit is exceeded."""

    pass


class AIProviderModelNotFoundError(AIProviderException):
    """Raised when requested model is not available."""

    pass
