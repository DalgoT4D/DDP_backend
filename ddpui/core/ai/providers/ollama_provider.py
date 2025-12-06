"""
Ollama provider implementation for Dalgo AI wrapper.
Supports local and remote Ollama instances.
"""

import os
import json
import asyncio
import aiohttp
import requests
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

logger = CustomLogger("ddpui.ai.ollama")


class OllamaProvider(AIProviderInterface):
    """
    Ollama API provider implementation for local and remote instances.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Ollama provider.

        Args:
            config: Configuration dictionary containing:
                - base_url: Ollama server URL (default: http://localhost:11434)
                - default_model: Default model to use
                - timeout: Request timeout in seconds
                - api_key: Optional API key for authentication
        """
        super().__init__(config)
        self._provider_type = AIProviderType.OLLAMA

        self.base_url = self.config.get("base_url", "http://localhost:11434")
        self.default_model = self.config.get("default_model", "llama2")
        self.timeout = self.config.get("timeout", 30)
        self.api_key = self.config.get("api_key")

        # Ensure base URL doesn't end with slash
        self.base_url = self.base_url.rstrip("/")

        # Setup headers
        self.headers = {"Content-Type": "application/json"}
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"

        logger.info(
            f"Ollama provider initialized with base_url: {self.base_url}, model: {self.default_model}"
        )

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate Ollama provider configuration."""
        # Ollama is quite flexible, only base_url is really needed
        base_url = config.get("base_url", "http://localhost:11434")
        if not base_url or not isinstance(base_url, str):
            raise AIProviderConfigurationError("base_url must be a valid URL string", "ollama")

    def _convert_messages(self, messages: List[AIMessage]) -> List[Dict[str, str]]:
        """Convert AIMessage objects to Ollama format."""
        return [{"role": msg.role, "content": msg.content} for msg in messages]

    def _handle_ollama_error(
        self, error: Exception, status_code: Optional[int] = None
    ) -> Exception:
        """Convert Ollama errors to our custom exceptions."""
        error_message = str(error)

        if status_code == 404:
            if "model" in error_message.lower():
                return AIProviderModelNotFoundError(
                    f"Model not found: {error_message}", "ollama", error
                )
        elif status_code == 429:
            return AIProviderRateLimitError(
                f"Rate limit exceeded: {error_message}", "ollama", error
            )
        elif status_code and status_code >= 500:
            return AIProviderConnectionError(
                f"Ollama server error: {error_message}", "ollama", error
            )
        elif "connection" in error_message.lower() or "timeout" in error_message.lower():
            return AIProviderConnectionError(f"Connection error: {error_message}", "ollama", error)
        else:
            return AIProviderConnectionError(f"Ollama API error: {error_message}", "ollama", error)

    def chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """Generate a chat completion using Ollama API."""
        try:
            model = model or self.default_model
            ollama_messages = self._convert_messages(messages)

            payload = {
                "model": model,
                "messages": ollama_messages,
                "stream": False,
                "options": {"temperature": temperature, **kwargs},
            }

            if max_tokens:
                payload["options"]["num_predict"] = max_tokens

            response = requests.post(
                f"{self.base_url}/api/chat",
                json=payload,
                headers=self.headers,
                timeout=self.timeout,
            )

            response.raise_for_status()
            result = response.json()

            return AIResponse(
                content=result.get("message", {}).get("content", ""),
                usage={
                    "prompt_tokens": result.get("prompt_eval_count", 0),
                    "completion_tokens": result.get("eval_count", 0),
                    "total_tokens": result.get("prompt_eval_count", 0)
                    + result.get("eval_count", 0),
                },
                model=result.get("model", model),
                provider="ollama",
                metadata={
                    "done": result.get("done", False),
                    "total_duration": result.get("total_duration"),
                    "load_duration": result.get("load_duration"),
                    "prompt_eval_duration": result.get("prompt_eval_duration"),
                    "eval_duration": result.get("eval_duration"),
                },
            )

        except requests.RequestException as e:
            logger.error(f"Ollama chat completion error: {e}")
            status_code = (
                getattr(e.response, "status_code", None) if hasattr(e, "response") else None
            )
            raise self._handle_ollama_error(e, status_code)
        except Exception as e:
            logger.error(f"Ollama chat completion error: {e}")
            raise self._handle_ollama_error(e)

    async def stream_chat_completion(
        self,
        messages: List[AIMessage],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AsyncGenerator[StreamingAIResponse, None]:
        """Generate a streaming chat completion using Ollama API."""
        try:
            model = model or self.default_model
            ollama_messages = self._convert_messages(messages)

            payload = {
                "model": model,
                "messages": ollama_messages,
                "stream": True,
                "options": {"temperature": temperature, **kwargs},
            }

            if max_tokens:
                payload["options"]["num_predict"] = max_tokens

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as session:
                async with session.post(
                    f"{self.base_url}/api/chat", json=payload, headers=self.headers
                ) as response:
                    response.raise_for_status()

                    async for line in response.content:
                        if line:
                            try:
                                chunk = json.loads(line.decode("utf-8"))
                                message_content = chunk.get("message", {}).get("content", "")
                                is_done = chunk.get("done", False)

                                if message_content or is_done:
                                    usage = None
                                    if is_done:
                                        usage = {
                                            "prompt_tokens": chunk.get("prompt_eval_count", 0),
                                            "completion_tokens": chunk.get("eval_count", 0),
                                            "total_tokens": chunk.get("prompt_eval_count", 0)
                                            + chunk.get("eval_count", 0),
                                        }

                                    yield StreamingAIResponse(
                                        content=message_content,
                                        is_complete=is_done,
                                        usage=usage,
                                        metadata={
                                            "done": is_done,
                                            "total_duration": chunk.get("total_duration"),
                                            "eval_count": chunk.get("eval_count"),
                                        },
                                    )

                                    if is_done:
                                        break

                            except json.JSONDecodeError:
                                continue

        except aiohttp.ClientError as e:
            logger.error(f"Ollama streaming error: {e}")
            raise self._handle_ollama_error(e)
        except Exception as e:
            logger.error(f"Ollama streaming error: {e}")
            raise self._handle_ollama_error(e)

    def completion(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AIResponse:
        """Generate a completion using Ollama API."""
        messages = [AIMessage(role="user", content=prompt)]
        return self.chat_completion(messages, model, temperature, max_tokens, **kwargs)

    def get_available_models(self) -> List[str]:
        """Get list of available Ollama models."""
        try:
            response = requests.get(
                f"{self.base_url}/api/tags", headers=self.headers, timeout=self.timeout
            )
            response.raise_for_status()

            result = response.json()
            models = result.get("models", [])
            return [model.get("name", "") for model in models if model.get("name")]

        except requests.RequestException as e:
            logger.error(f"Error fetching Ollama models: {e}")
            status_code = (
                getattr(e.response, "status_code", None) if hasattr(e, "response") else None
            )
            raise self._handle_ollama_error(e, status_code)
        except Exception as e:
            logger.error(f"Error fetching Ollama models: {e}")
            raise self._handle_ollama_error(e)

    def health_check(self) -> bool:
        """Check if Ollama server is accessible."""
        try:
            response = requests.get(
                f"{self.base_url}/api/tags",
                headers=self.headers,
                timeout=5,  # Short timeout for health check
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Ollama health check failed: {e}")
            return False

    def pull_model(self, model: str) -> bool:
        """
        Pull a model from Ollama registry.

        Args:
            model: Name of the model to pull

        Returns:
            True if model was pulled successfully
        """
        try:
            payload = {"name": model}
            response = requests.post(
                f"{self.base_url}/api/pull",
                json=payload,
                headers=self.headers,
                timeout=300,  # Longer timeout for model pulling
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error pulling model {model}: {e}")
            return False

    def delete_model(self, model: str) -> bool:
        """
        Delete a model from local Ollama instance.

        Args:
            model: Name of the model to delete

        Returns:
            True if model was deleted successfully
        """
        try:
            payload = {"name": model}
            response = requests.delete(
                f"{self.base_url}/api/delete",
                json=payload,
                headers=self.headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error deleting model {model}: {e}")
            return False


def create_ollama_provider_from_env() -> OllamaProvider:
    """
    Create Ollama provider using environment variables.

    Environment variables:
        - OLLAMA_BASE_URL: Ollama server URL (default: http://localhost:11434)
        - OLLAMA_DEFAULT_MODEL: Default model to use
        - OLLAMA_TIMEOUT: Request timeout in seconds
        - OLLAMA_API_KEY: Optional API key for authentication

    Returns:
        Configured OllamaProvider instance
    """
    config = {
        "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        "default_model": os.getenv("OLLAMA_DEFAULT_MODEL", "llama2"),
        "timeout": int(os.getenv("OLLAMA_TIMEOUT", "30")),
        "api_key": os.getenv("OLLAMA_API_KEY"),
    }

    # Remove None values
    config = {k: v for k, v in config.items() if v is not None}

    return OllamaProvider(config)
