"""
AI Provider Factory for managing different AI service providers.
Enables easy switching between OpenAI, Claude, Ollama, etc.
"""

import os
from typing import Optional, Dict, Any, Union
from enum import Enum

from ddpui.core.ai.interfaces import (
    AIProviderInterface,
    AIProviderType,
    AIProviderConfigurationError,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.ai.factory")


class AIProviderFactory:
    """
    Factory class for creating and managing AI providers.
    Supports configuration via environment variables or explicit config.
    """

    _providers = {}  # Registry of available providers
    _instances = {}  # Cache of provider instances

    @classmethod
    def register_provider(cls, provider_type: AIProviderType, provider_class: type):
        """
        Register a provider class for a given provider type.

        Args:
            provider_type: The provider type enum
            provider_class: The provider class that implements AIProviderInterface
        """
        cls._providers[provider_type] = provider_class
        logger.info(f"Registered AI provider: {provider_type.value}")

    @classmethod
    def create_provider(
        cls,
        provider_type: Union[str, AIProviderType],
        config: Optional[Dict[str, Any]] = None,
        use_env: bool = True,
        force_new: bool = False,
    ) -> AIProviderInterface:
        """
        Create an AI provider instance.

        Args:
            provider_type: Type of provider to create (string or enum)
            config: Optional configuration dictionary
            use_env: Whether to use environment variables for configuration
            force_new: Whether to force creation of new instance (ignore cache)

        Returns:
            Configured AI provider instance

        Raises:
            AIProviderConfigurationError: If provider type is not supported or configuration is invalid
        """
        # Convert string to enum if needed
        if isinstance(provider_type, str):
            try:
                provider_type = AIProviderType(provider_type.lower())
            except ValueError:
                raise AIProviderConfigurationError(
                    f"Unsupported provider type: {provider_type}", str(provider_type)
                )

        # Check if provider is registered
        if provider_type not in cls._providers:
            raise AIProviderConfigurationError(
                f"Provider {provider_type.value} is not registered", provider_type.value
            )

        # Use cached instance if available and not forcing new
        cache_key = f"{provider_type.value}_{hash(str(config))}"
        if not force_new and cache_key in cls._instances:
            logger.info(f"Using cached provider instance: {provider_type.value}")
            return cls._instances[cache_key]

        # Create configuration
        final_config = config or {}
        if use_env:
            env_config = cls._get_env_config(provider_type)
            final_config = {**env_config, **final_config}

        # Create provider instance
        provider_class = cls._providers[provider_type]
        try:
            provider = provider_class(final_config)
            cls._instances[cache_key] = provider
            logger.info(f"Created new AI provider instance: {provider_type.value}")
            return provider
        except Exception as e:
            raise AIProviderConfigurationError(
                f"Failed to create provider {provider_type.value}: {str(e)}", provider_type.value, e
            )

    @classmethod
    def create_default_provider(
        cls, config: Optional[Dict[str, Any]] = None, force_new: bool = False
    ) -> AIProviderInterface:
        """
        Create a provider using the default provider type from environment.

        Args:
            config: Optional configuration dictionary
            force_new: Whether to force creation of new instance

        Returns:
            Configured AI provider instance
        """
        default_provider = os.getenv("AI_PROVIDER_DEFAULT", "openai")
        return cls.create_provider(default_provider, config, use_env=True, force_new=force_new)

    @classmethod
    def get_available_providers(cls) -> list[str]:
        """
        Get list of available provider types.

        Returns:
            List of provider type names
        """
        return [provider_type.value for provider_type in cls._providers.keys()]

    @classmethod
    def clear_cache(cls):
        """Clear the provider instance cache."""
        cls._instances.clear()
        logger.info("Cleared AI provider instance cache")

    @classmethod
    def health_check_all(cls) -> Dict[str, bool]:
        """
        Perform health check on all available providers.

        Returns:
            Dictionary mapping provider names to health status
        """
        results = {}
        for provider_type in cls._providers.keys():
            try:
                provider = cls.create_provider(provider_type, use_env=True)
                results[provider_type.value] = provider.health_check()
            except Exception as e:
                logger.error(f"Health check failed for {provider_type.value}: {e}")
                results[provider_type.value] = False

        return results

    @classmethod
    def _get_env_config(cls, provider_type: AIProviderType) -> Dict[str, Any]:
        """
        Get configuration from environment variables for a provider type.

        Args:
            provider_type: The provider type

        Returns:
            Configuration dictionary from environment variables
        """
        if provider_type == AIProviderType.OPENAI:
            return cls._get_openai_env_config()
        elif provider_type == AIProviderType.CLAUDE:
            return cls._get_claude_env_config()
        elif provider_type == AIProviderType.OLLAMA:
            return cls._get_ollama_env_config()
        else:
            return {}

    @classmethod
    def _get_openai_env_config(cls) -> Dict[str, Any]:
        """Get OpenAI configuration from environment variables."""
        config = {}

        if os.getenv("OPENAI_API_KEY"):
            config["api_key"] = os.getenv("OPENAI_API_KEY")
        if os.getenv("OPENAI_BASE_URL"):
            config["base_url"] = os.getenv("OPENAI_BASE_URL")
        if os.getenv("OPENAI_ORGANIZATION"):
            config["organization"] = os.getenv("OPENAI_ORGANIZATION")
        if os.getenv("OPENAI_DEFAULT_MODEL"):
            config["default_model"] = os.getenv("OPENAI_DEFAULT_MODEL")

        return config

    @classmethod
    def _get_claude_env_config(cls) -> Dict[str, Any]:
        """Get Claude configuration from environment variables."""
        config = {}

        api_key = os.getenv("CLAUDE_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
        if api_key:
            config["api_key"] = api_key
        if os.getenv("CLAUDE_BASE_URL"):
            config["base_url"] = os.getenv("CLAUDE_BASE_URL")
        if os.getenv("CLAUDE_DEFAULT_MODEL"):
            config["default_model"] = os.getenv("CLAUDE_DEFAULT_MODEL")
        if os.getenv("CLAUDE_MAX_TOKENS_DEFAULT"):
            config["max_tokens_default"] = int(os.getenv("CLAUDE_MAX_TOKENS_DEFAULT"))

        return config

    @classmethod
    def _get_ollama_env_config(cls) -> Dict[str, Any]:
        """Get Ollama configuration from environment variables."""
        config = {}

        if os.getenv("OLLAMA_BASE_URL"):
            config["base_url"] = os.getenv("OLLAMA_BASE_URL")
        if os.getenv("OLLAMA_DEFAULT_MODEL"):
            config["default_model"] = os.getenv("OLLAMA_DEFAULT_MODEL")
        if os.getenv("OLLAMA_TIMEOUT"):
            config["timeout"] = int(os.getenv("OLLAMA_TIMEOUT"))
        if os.getenv("OLLAMA_API_KEY"):
            config["api_key"] = os.getenv("OLLAMA_API_KEY")

        return config


# Register default providers
def register_default_providers():
    """Register all default AI providers."""
    try:
        from ddpui.core.ai.providers.openai_provider import OpenAIProvider

        AIProviderFactory.register_provider(AIProviderType.OPENAI, OpenAIProvider)
    except ImportError:
        logger.warning("OpenAI provider not available (missing openai package)")

    try:
        from ddpui.core.ai.providers.claude_provider import ClaudeProvider

        AIProviderFactory.register_provider(AIProviderType.CLAUDE, ClaudeProvider)
    except ImportError:
        logger.warning("Claude provider not available (missing anthropic package)")

    try:
        from ddpui.core.ai.providers.ollama_provider import OllamaProvider

        AIProviderFactory.register_provider(AIProviderType.OLLAMA, OllamaProvider)
    except ImportError:
        logger.warning("Ollama provider not available")


# Auto-register providers on import
register_default_providers()


# Convenience functions
def get_ai_provider(
    provider_type: Optional[Union[str, AIProviderType]] = None,
    config: Optional[Dict[str, Any]] = None,
    force_new: bool = False,
) -> AIProviderInterface:
    """
    Convenience function to get an AI provider.

    Args:
        provider_type: Type of provider (if None, uses default from env)
        config: Optional configuration
        force_new: Whether to force new instance

    Returns:
        AI provider instance
    """
    if provider_type is None:
        return AIProviderFactory.create_default_provider(config, force_new)
    else:
        return AIProviderFactory.create_provider(
            provider_type, config, use_env=True, force_new=force_new
        )


def get_default_ai_provider(force_new: bool = False) -> AIProviderInterface:
    """
    Get the default AI provider based on environment configuration.

    Args:
        force_new: Whether to force new instance

    Returns:
        Default AI provider instance
    """
    return AIProviderFactory.create_default_provider(force_new=force_new)
