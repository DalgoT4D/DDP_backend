"""
Claude (Anthropic) provider implementation for Dalgo AI wrapper.
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
    import anthropic
    from anthropic import Anthropic, AsyncAnthropic
except ImportError:
    anthropic = None
    Anthropic = None
    AsyncAnthropic = None

logger = CustomLogger("ddpui.ai.claude")


class ClaudeProvider(AIProviderInterface):
    """
    Anthropic Claude API provider implementation.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Claude provider.

        Args:
            config: Configuration dictionary containing:
                - api_key: Anthropic API key
                - base_url: Optional base URL for API
                - default_model: Default model to use
                - max_tokens_default: Default max tokens for responses
        """
        if not anthropic:
            raise AIProviderConfigurationError(
                "anthropic package not installed. Install with: pip install anthropic", "claude"
            )

        super().__init__(config)
        self._provider_type = AIProviderType.CLAUDE

        # Initialize Anthropic clients
        client_kwargs = {"api_key": self.config["api_key"]}

        if self.config.get("base_url"):
            client_kwargs["base_url"] = self.config["base_url"]

        self.client = Anthropic(**client_kwargs)
        self.async_client = AsyncAnthropic(**client_kwargs)

        self.default_model = self.config.get("default_model", "claude-3-sonnet-20240229")
        self.max_tokens_default = self.config.get("max_tokens_default", 4096)

        logger.info(f"Claude provider initialized with model: {self.default_model}")

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate Claude provider configuration."""
        required_fields = ["api_key"]
        for field in required_fields:
            if not config.get(field):
                raise AIProviderConfigurationError(
                    f"Missing required configuration field: {field}", "claude"
                )

    def _convert_messages(self, messages: List[AIMessage]) -> List[Dict[str, str]]:
        """
        Convert AIMessage objects to Claude format.
        Claude requires alternating user/assistant messages and doesn't support system messages in the messages array.
        """
        claude_messages = []
        system_message = None

        for msg in messages:
            if msg.role == "system":
                # Claude handles system messages separately
                system_message = msg.content
            else:
                claude_messages.append({"role": msg.role, "content": msg.content})

        return claude_messages, system_message

    def _handle_claude_error(self, error: Exception) -> Exception:
        """Convert Claude errors to our custom exceptions."""
        error_message = str(error)

        if hasattr(error, "status_code"):
            status_code = error.status_code
            if status_code == 429:
                return AIProviderRateLimitError(
                    f"Rate limit exceeded: {error_message}", "claude", error
                )
            elif status_code == 404:
                return AIProviderModelNotFoundError(
                    f"Model not found: {error_message}", "claude", error
                )
            elif status_code >= 500:
                return AIProviderConnectionError(
                    f"Claude API server error: {error_message}", "claude", error
                )

        if "rate limit" in error_message.lower():
            return AIProviderRateLimitError(
                f"Rate limit exceeded: {error_message}", "claude", error
            )
        elif "model" in error_message.lower() and (
            "not found" in error_message.lower() or "not supported" in error_message.lower()
        ):
            return AIProviderModelNotFoundError(
                f"Model not found: {error_message}", "claude", error
            )
        elif "connection" in error_message.lower() or "timeout" in error_message.lower():
            return AIProviderConnectionError(f"Connection error: {error_message}", "claude", error)
        else:
            return AIProviderConnectionError(f"Claude API error: {error_message}", "claude", error)

    def chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """Generate a chat completion using Claude API."""
        try:
            model = model or self.default_model
            max_tokens = max_tokens or self.max_tokens_default

            claude_messages, system_message = self._convert_messages(messages)

            completion_kwargs = {
                "model": model,
                "messages": claude_messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                **kwargs,
            }

            if system_message:
                completion_kwargs["system"] = system_message

            response = self.client.messages.create(**completion_kwargs)

            # Extract content from response
            content = ""
            if response.content and len(response.content) > 0:
                content = (
                    response.content[0].text
                    if hasattr(response.content[0], "text")
                    else str(response.content[0])
                )

            return AIResponse(
                content=content,
                usage={
                    "prompt_tokens": response.usage.input_tokens,
                    "completion_tokens": response.usage.output_tokens,
                    "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
                }
                if response.usage
                else None,
                model=response.model,
                provider="claude",
                metadata={"stop_reason": response.stop_reason, "response_id": response.id},
            )

        except Exception as e:
            logger.error(f"Claude chat completion error: {e}")
            raise self._handle_claude_error(e)

    async def stream_chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AsyncGenerator[StreamingAIResponse, None]:
        """Generate a streaming chat completion using Claude API."""
        try:
            model = model or self.default_model
            max_tokens = max_tokens or self.max_tokens_default

            claude_messages, system_message = self._convert_messages(messages)

            completion_kwargs = {
                "model": model,
                "messages": claude_messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "stream": True,
                **kwargs,
            }

            if system_message:
                completion_kwargs["system"] = system_message

            async with self.async_client.messages.stream(**completion_kwargs) as stream:
                async for event in stream:
                    if hasattr(event, "delta") and hasattr(event.delta, "text"):
                        yield StreamingAIResponse(
                            content=event.delta.text,
                            is_complete=False,
                            metadata={"event_type": type(event).__name__},
                        )
                    elif hasattr(event, "type") and event.type == "message_stop":
                        yield StreamingAIResponse(
                            content="",
                            is_complete=True,
                            usage={
                                "total_tokens": getattr(event, "usage", {}).get("output_tokens", 0)
                            },
                            metadata={"event_type": "message_stop"},
                        )

        except Exception as e:
            logger.error(f"Claude streaming error: {e}")
            raise self._handle_claude_error(e)

    def completion(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """Generate a completion using Claude API."""
        messages = [AIMessage(role="user", content=prompt)]
        return self.chat_completion(messages, model, temperature, max_tokens, **kwargs)

    def get_available_models(self) -> List[str]:
        """
        Get list of available Claude models.
        Note: Claude API doesn't provide a models endpoint, so we return known models.
        """
        return [
            "claude-3-opus-20240229",
            "claude-3-sonnet-20240229",
            "claude-3-haiku-20240307",
            "claude-2.1",
            "claude-2.0",
            "claude-instant-1.2",
        ]

    def health_check(self) -> bool:
        """Check if Claude API is accessible."""
        try:
            # Simple API call to check connectivity
            test_messages = [AIMessage(role="user", content="Hi")]
            response = self.chat_completion(test_messages, max_tokens=10)
            return len(response.content) > 0
        except Exception as e:
            logger.error(f"Claude health check failed: {e}")
            return False


def create_claude_provider_from_env() -> ClaudeProvider:
    """
    Create Claude provider using environment variables.

    Environment variables:
        - CLAUDE_API_KEY or ANTHROPIC_API_KEY: Required API key
        - CLAUDE_BASE_URL: Optional base URL for API
        - CLAUDE_DEFAULT_MODEL: Optional default model
        - CLAUDE_MAX_TOKENS_DEFAULT: Optional default max tokens

    Returns:
        Configured ClaudeProvider instance
    """
    api_key = os.getenv("CLAUDE_API_KEY") or os.getenv("ANTHROPIC_API_KEY")

    config = {
        "api_key": api_key,
        "base_url": os.getenv("CLAUDE_BASE_URL"),
        "default_model": os.getenv("CLAUDE_DEFAULT_MODEL", "claude-3-sonnet-20240229"),
        "max_tokens_default": int(os.getenv("CLAUDE_MAX_TOKENS_DEFAULT", "4096")),
    }

    # Remove None values
    config = {k: v for k, v in config.items() if v is not None}

    return ClaudeProvider(config)
