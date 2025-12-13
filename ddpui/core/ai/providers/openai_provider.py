"""
OpenAI provider implementation for Dalgo AI wrapper.
"""

import os
import asyncio
from typing import Optional, Dict, Any, List, AsyncGenerator
from ddpui.core.ai.interfaces import (
    AIProviderInterface,
    AIMessage,
    AIResponse,
    StreamingAIResponse,
    AIProviderType,
    AIProviderConfigurationError,
    AIProviderConnectionError,
    AIProviderRateLimitError,
    AIProviderModelNotFoundError,
)
from ddpui.utils.custom_logger import CustomLogger

try:
    import openai
    from openai import OpenAI, AsyncOpenAI
except ImportError:
    openai = None
    OpenAI = None
    AsyncOpenAI = None

logger = CustomLogger("ddpui.ai.openai")


class OpenAIProvider(AIProviderInterface):
    """
    OpenAI API provider implementation.
    Supports both OpenAI and OpenAI-compatible APIs.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OpenAI provider.

        Args:
            config: Configuration dictionary containing:
                - api_key: OpenAI API key
                - base_url: Optional base URL for OpenAI-compatible APIs
                - organization: Optional organization ID
                - default_model: Default model to use
        """
        if not openai:
            raise AIProviderConfigurationError(
                "openai package not installed. Install with: pip install openai", "openai"
            )

        super().__init__(config)
        self._provider_type = AIProviderType.OPENAI

        # Initialize OpenAI clients
        client_kwargs = {"api_key": self.config["api_key"]}

        if self.config.get("base_url"):
            client_kwargs["base_url"] = self.config["base_url"]
        if self.config.get("organization"):
            client_kwargs["organization"] = self.config["organization"]

        self.client = OpenAI(**client_kwargs)
        self.async_client = AsyncOpenAI(**client_kwargs)

        self.default_model = self.config.get("default_model", "gpt-3.5-turbo")

        logger.info(f"OpenAI provider initialized with model: {self.default_model}")

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate OpenAI provider configuration."""
        required_fields = ["api_key"]
        for field in required_fields:
            if not config.get(field):
                raise AIProviderConfigurationError(
                    f"Missing required configuration field: {field}", "openai"
                )

    def _convert_messages(self, messages: List[AIMessage]) -> List[Dict[str, str]]:
        """Convert AIMessage objects to OpenAI format."""
        return [{"role": msg.role, "content": msg.content} for msg in messages]

    def _handle_openai_error(self, error: Exception) -> Exception:
        """Convert OpenAI errors to our custom exceptions."""
        error_message = str(error)

        if "rate limit" in error_message.lower():
            return AIProviderRateLimitError(
                f"Rate limit exceeded: {error_message}", "openai", error
            )
        elif "model" in error_message.lower() and (
            "not found" in error_message.lower() or "does not exist" in error_message.lower()
        ):
            return AIProviderModelNotFoundError(
                f"Model not found: {error_message}", "openai", error
            )
        elif "connection" in error_message.lower() or "timeout" in error_message.lower():
            return AIProviderConnectionError(f"Connection error: {error_message}", "openai", error)
        else:
            return AIProviderConnectionError(f"OpenAI API error: {error_message}", "openai", error)

    def chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """Generate a chat completion using OpenAI API."""
        try:
            model = model or self.default_model
            openai_messages = self._convert_messages(messages)

            completion_kwargs = {
                "model": model,
                "messages": openai_messages,
                "temperature": temperature,
                **kwargs,
            }

            if max_tokens:
                completion_kwargs["max_tokens"] = max_tokens

            response = self.client.chat.completions.create(**completion_kwargs)

            return AIResponse(
                content=response.choices[0].message.content,
                usage={
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                }
                if response.usage
                else None,
                model=response.model,
                provider="openai",
                metadata={
                    "finish_reason": response.choices[0].finish_reason,
                    "response_id": response.id,
                },
            )

        except Exception as e:
            logger.error(f"OpenAI chat completion error: {e}")
            raise self._handle_openai_error(e)

    async def stream_chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AsyncGenerator[StreamingAIResponse, None]:
        """Generate a streaming chat completion using OpenAI API."""
        try:
            model = model or self.default_model
            openai_messages = self._convert_messages(messages)

            completion_kwargs = {
                "model": model,
                "messages": openai_messages,
                "temperature": temperature,
                "stream": True,
                **kwargs,
            }

            if max_tokens:
                completion_kwargs["max_tokens"] = max_tokens

            stream = await self.async_client.chat.completions.create(**completion_kwargs)

            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    yield StreamingAIResponse(
                        content=chunk.choices[0].delta.content,
                        is_complete=chunk.choices[0].finish_reason is not None,
                        metadata={
                            "chunk_id": chunk.id,
                            "finish_reason": chunk.choices[0].finish_reason,
                        },
                    )

        except Exception as e:
            logger.error(f"OpenAI streaming error: {e}")
            raise self._handle_openai_error(e)

    def completion(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """Generate a completion using OpenAI API."""
        messages = [AIMessage(role="user", content=prompt)]
        return self.chat_completion(messages, model, temperature, max_tokens, **kwargs)

    def get_available_models(self) -> List[str]:
        """Get list of available OpenAI models."""
        try:
            models = self.client.models.list()
            return [model.id for model in models.data]
        except Exception as e:
            logger.error(f"Error fetching OpenAI models: {e}")
            raise self._handle_openai_error(e)

    def health_check(self) -> bool:
        """Check if OpenAI API is accessible."""
        try:
            # Simple API call to check connectivity
            models = self.client.models.list()
            return len(models.data) > 0
        except Exception as e:
            logger.error(f"OpenAI health check failed: {e}")
            return False


def create_openai_provider_from_env() -> OpenAIProvider:
    """
    Create OpenAI provider using environment variables.

    Environment variables:
        - OPENAI_API_KEY: Required API key
        - OPENAI_BASE_URL: Optional base URL for compatible APIs
        - OPENAI_ORGANIZATION: Optional organization ID
        - OPENAI_DEFAULT_MODEL: Optional default model

    Returns:
        Configured OpenAIProvider instance
    """
    config = {
        "api_key": os.getenv("OPENAI_API_KEY"),
        "base_url": os.getenv("OPENAI_BASE_URL"),
        "organization": os.getenv("OPENAI_ORGANIZATION"),
        "default_model": os.getenv("OPENAI_DEFAULT_MODEL", "gpt-3.5-turbo"),
    }

    # Remove None values
    config = {k: v for k, v in config.items() if v is not None}

    return OpenAIProvider(config)
