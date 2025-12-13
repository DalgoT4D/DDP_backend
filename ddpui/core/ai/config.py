"""
AI configuration management for Dalgo platform.
Centralizes AI-related settings and environment variable handling.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.ai.config")


@dataclass
class AIProviderConfig:
    """Configuration for a specific AI provider."""

    api_key: Optional[str] = None
    base_url: Optional[str] = None
    default_model: Optional[str] = None
    timeout: int = 30
    additional_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AIGlobalConfig:
    """Global AI configuration settings."""

    default_provider: str = "openai"
    streaming_enabled: bool = True
    rate_limit_requests_per_minute: int = 60
    max_tokens_default: int = 2048
    temperature_default: float = 0.7
    log_requests: bool = True
    log_responses: bool = False
    api_rate_limit_enabled: bool = True
    api_auth_required: bool = True


class AIConfigManager:
    """
    Manages AI configuration for the Dalgo platform.
    Handles environment variables, defaults, and validation.
    """

    def __init__(self):
        """Initialize the AI configuration manager."""
        self._global_config = None
        self._provider_configs = {}
        self._load_config()

    def _load_config(self):
        """Load configuration from environment variables."""
        # Load global AI configuration
        self._global_config = AIGlobalConfig(
            default_provider=os.getenv("AI_PROVIDER_DEFAULT", "openai"),
            streaming_enabled=os.getenv("AI_STREAMING_ENABLED", "true").lower() == "true",
            rate_limit_requests_per_minute=int(
                os.getenv("AI_RATE_LIMIT_REQUESTS_PER_MINUTE", "60")
            ),
            max_tokens_default=int(os.getenv("AI_MAX_TOKENS_DEFAULT", "2048")),
            temperature_default=float(os.getenv("AI_TEMPERATURE_DEFAULT", "0.7")),
            log_requests=os.getenv("AI_LOG_REQUESTS", "true").lower() == "true",
            log_responses=os.getenv("AI_LOG_RESPONSES", "false").lower() == "true",
            api_rate_limit_enabled=os.getenv("AI_API_RATE_LIMIT_ENABLED", "true").lower() == "true",
            api_auth_required=os.getenv("AI_API_AUTH_REQUIRED", "true").lower() == "true",
        )

        # Load provider-specific configurations
        self._load_provider_configs()

        logger.info(
            f"AI configuration loaded. Default provider: {self._global_config.default_provider}"
        )

    def _load_provider_configs(self):
        """Load configurations for all AI providers."""
        # OpenAI configuration
        openai_config = AIProviderConfig(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL"),
            default_model=os.getenv("OPENAI_DEFAULT_MODEL", "gpt-3.5-turbo"),
            timeout=int(os.getenv("OPENAI_TIMEOUT", "30")),
            additional_config={"organization": os.getenv("OPENAI_ORGANIZATION")},
        )
        self._provider_configs["openai"] = openai_config

        # Claude configuration
        claude_api_key = os.getenv("CLAUDE_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
        claude_config = AIProviderConfig(
            api_key=claude_api_key,
            base_url=os.getenv("CLAUDE_BASE_URL"),
            default_model=os.getenv("CLAUDE_DEFAULT_MODEL", "claude-3-sonnet-20240229"),
            timeout=int(os.getenv("CLAUDE_TIMEOUT", "30")),
            additional_config={
                "max_tokens_default": int(os.getenv("CLAUDE_MAX_TOKENS_DEFAULT", "4096"))
            },
        )
        self._provider_configs["claude"] = claude_config

        # Ollama configuration
        ollama_config = AIProviderConfig(
            api_key=os.getenv("OLLAMA_API_KEY"),  # Optional for Ollama
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            default_model=os.getenv("OLLAMA_DEFAULT_MODEL", "llama2"),
            timeout=int(os.getenv("OLLAMA_TIMEOUT", "30")),
        )
        self._provider_configs["ollama"] = ollama_config

    def get_global_config(self) -> AIGlobalConfig:
        """Get the global AI configuration."""
        return self._global_config

    def get_provider_config(self, provider: str) -> Optional[AIProviderConfig]:
        """
        Get configuration for a specific provider.

        Args:
            provider: Provider name (openai, claude, ollama)

        Returns:
            Provider configuration or None if not found
        """
        return self._provider_configs.get(provider.lower())

    def get_provider_dict_config(self, provider: str) -> Dict[str, Any]:
        """
        Get provider configuration as a dictionary for use with AI providers.

        Args:
            provider: Provider name

        Returns:
            Configuration dictionary
        """
        config = self.get_provider_config(provider)
        if not config:
            return {}

        result = {}

        if config.api_key:
            result["api_key"] = config.api_key
        if config.base_url:
            result["base_url"] = config.base_url
        if config.default_model:
            result["default_model"] = config.default_model
        if config.timeout:
            result["timeout"] = config.timeout

        # Add additional provider-specific config
        result.update(config.additional_config)

        # Remove None values
        return {k: v for k, v in result.items() if v is not None}

    def is_provider_configured(self, provider: str) -> bool:
        """
        Check if a provider is properly configured.

        Args:
            provider: Provider name

        Returns:
            True if provider is configured
        """
        config = self.get_provider_config(provider)
        if not config:
            return False

        # Check provider-specific requirements
        if provider.lower() == "openai":
            return config.api_key is not None
        elif provider.lower() == "claude":
            return config.api_key is not None
        elif provider.lower() == "ollama":
            # Ollama only needs base_url (API key is optional)
            return config.base_url is not None

        return False

    def get_configured_providers(self) -> list[str]:
        """
        Get list of properly configured providers.

        Returns:
            List of configured provider names
        """
        return [
            provider
            for provider in self._provider_configs.keys()
            if self.is_provider_configured(provider)
        ]

    def reload_config(self):
        """Reload configuration from environment variables."""
        self._load_config()
        logger.info("AI configuration reloaded")

    def validate_config(self) -> Dict[str, Any]:
        """
        Validate the current configuration.

        Returns:
            Dictionary with validation results
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "configured_providers": self.get_configured_providers(),
        }

        # Check if at least one provider is configured
        if not results["configured_providers"]:
            results["valid"] = False
            results["errors"].append("No AI providers are properly configured")

        # Check if default provider is configured
        default_provider = self._global_config.default_provider
        if not self.is_provider_configured(default_provider):
            results["valid"] = False
            results["errors"].append(
                f"Default provider '{default_provider}' is not properly configured"
            )

        # Validate temperature range
        if not 0.0 <= self._global_config.temperature_default <= 2.0:
            results["warnings"].append("Temperature default is outside recommended range (0.0-2.0)")

        # Validate rate limits
        if self._global_config.rate_limit_requests_per_minute <= 0:
            results["warnings"].append("Rate limit should be greater than 0")

        return results

    def get_environment_template(self) -> str:
        """
        Get a template for environment variables.

        Returns:
            String containing environment variable template
        """
        template = """
# AI Provider Configuration

# Default AI Provider (openai, claude, ollama)
AI_PROVIDER_DEFAULT={default_provider}

# Global AI Settings
AI_STREAMING_ENABLED={streaming_enabled}
AI_RATE_LIMIT_REQUESTS_PER_MINUTE={rate_limit}
AI_MAX_TOKENS_DEFAULT={max_tokens}
AI_TEMPERATURE_DEFAULT={temperature}

# OpenAI Configuration
OPENAI_API_KEY=your-openai-api-key-here
OPENAI_DEFAULT_MODEL=gpt-3.5-turbo

# Claude Configuration
CLAUDE_API_KEY=your-claude-api-key-here
CLAUDE_DEFAULT_MODEL=claude-3-sonnet-20240229

# Ollama Configuration
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_DEFAULT_MODEL=llama2
""".strip()

        return template.format(
            default_provider=self._global_config.default_provider,
            streaming_enabled=self._global_config.streaming_enabled,
            rate_limit=self._global_config.rate_limit_requests_per_minute,
            max_tokens=self._global_config.max_tokens_default,
            temperature=self._global_config.temperature_default,
        )


# Global instance
ai_config = AIConfigManager()


# Convenience functions
def get_ai_global_config() -> AIGlobalConfig:
    """Get global AI configuration."""
    return ai_config.get_global_config()


def get_ai_provider_config(provider: str) -> Optional[AIProviderConfig]:
    """Get provider-specific configuration."""
    return ai_config.get_provider_config(provider)


def is_ai_provider_configured(provider: str) -> bool:
    """Check if AI provider is configured."""
    return ai_config.is_provider_configured(provider)


def get_configured_ai_providers() -> list[str]:
    """Get list of configured AI providers."""
    return ai_config.get_configured_providers()


def validate_ai_config() -> Dict[str, Any]:
    """Validate AI configuration."""
    return ai_config.validate_config()
