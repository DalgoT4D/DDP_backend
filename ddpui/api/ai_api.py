"""
AI API endpoints for Dalgo platform.
Provides generic AI capabilities that work with any configured provider.
"""

import json
import asyncio
from typing import Optional, List, Dict, Any
from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
from ninja import Router, Schema
from ninja.security import HttpBearer

from ddpui.auth import has_permission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.ai.factory import get_ai_provider, get_default_ai_provider, AIProviderFactory
from ddpui.core.ai.interfaces import (
    AIMessage,
    AIResponse,
    AIProviderType,
    AIProviderConfigurationError,
    AIProviderConnectionError,
    AIProviderRateLimitError,
)

logger = CustomLogger("ddpui.api.ai")

# Ninja router for AI API
router = Router()


# Pydantic schemas for API
class ChatMessageSchema(Schema):
    role: str
    content: str
    metadata: Optional[Dict[str, Any]] = None


class ChatCompletionRequest(Schema):
    messages: List[ChatMessageSchema]
    model: Optional[str] = None
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    provider_type: Optional[str] = None
    stream: bool = False


class CompletionRequest(Schema):
    prompt: str
    model: Optional[str] = None
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    provider_type: Optional[str] = None


class ProviderConfigRequest(Schema):
    provider_type: str
    config: Dict[str, Any]


class AIResponseSchema(Schema):
    content: str
    usage: Optional[Dict[str, int]] = None
    model: Optional[str] = None
    provider: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# Bearer token auth
class AIApiAuth(HttpBearer):
    def authenticate(self, request, token):
        # You can integrate this with your existing auth system
        # For now, we'll use the existing has_permission decorator
        return token


auth = AIApiAuth()


def handle_ai_error(error: Exception) -> JsonResponse:
    """Convert AI provider errors to appropriate HTTP responses."""
    if isinstance(error, AIProviderConfigurationError):
        return JsonResponse(
            {"error": "Configuration Error", "message": str(error), "provider": error.provider},
            status=400,
        )
    elif isinstance(error, AIProviderConnectionError):
        return JsonResponse(
            {"error": "Connection Error", "message": str(error), "provider": error.provider},
            status=502,
        )
    elif isinstance(error, AIProviderRateLimitError):
        return JsonResponse(
            {"error": "Rate Limit Exceeded", "message": str(error), "provider": error.provider},
            status=429,
        )
    else:
        logger.error(f"Unexpected AI error: {error}")
        return JsonResponse(
            {"error": "Internal Error", "message": "An unexpected error occurred"}, status=500
        )


@router.post("/chat/completions")
@has_permission("can_use_ai")
def chat_completions(request, payload: ChatCompletionRequest):
    """
    Generate chat completions using the configured AI provider.

    This endpoint provides a unified interface for chat completions
    regardless of the underlying AI provider (OpenAI, Claude, Ollama).
    """
    try:
        # Get AI provider
        provider = (
            get_ai_provider(payload.provider_type)
            if payload.provider_type
            else get_default_ai_provider()
        )

        # Convert schema messages to AI messages
        messages = [
            AIMessage(role=msg.role, content=msg.content, metadata=msg.metadata)
            for msg in payload.messages
        ]

        # Handle streaming vs non-streaming
        if payload.stream:
            return StreamingHttpResponse(
                _stream_chat_completion(provider, messages, payload),
                content_type="text/event-stream",
            )
        else:
            # Generate completion
            response = provider.chat_completion(
                messages=messages,
                model=payload.model,
                temperature=payload.temperature,
                max_tokens=payload.max_tokens,
            )

            return JsonResponse(
                {
                    "id": f"chatcmpl-{response.metadata.get('response_id', 'unknown')}"
                    if response.metadata
                    else "chatcmpl-unknown",
                    "object": "chat.completion",
                    "created": int(asyncio.get_event_loop().time()),
                    "model": response.model,
                    "provider": response.provider,
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": response.content},
                            "finish_reason": response.metadata.get("finish_reason", "stop")
                            if response.metadata
                            else "stop",
                        }
                    ],
                    "usage": response.usage or {},
                }
            )

    except Exception as e:
        return handle_ai_error(e)


async def _stream_chat_completion(provider, messages, payload):
    """Generate streaming chat completion response."""
    try:
        async for chunk in provider.stream_chat_completion(
            messages=messages,
            model=payload.model,
            temperature=payload.temperature,
            max_tokens=payload.max_tokens,
        ):
            data = {
                "id": f"chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": int(asyncio.get_event_loop().time()),
                "model": payload.model or "unknown",
                "provider": provider.get_provider_type().value,
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": chunk.content},
                        "finish_reason": "stop" if chunk.is_complete else None,
                    }
                ],
            }

            if chunk.usage:
                data["usage"] = chunk.usage

            yield f"data: {json.dumps(data)}\n\n"

            if chunk.is_complete:
                yield "data: [DONE]\n\n"
                break

    except Exception as e:
        error_data = {"error": {"message": str(e), "type": type(e).__name__}}
        yield f"data: {json.dumps(error_data)}\n\n"


@router.post("/completions")
@has_permission("can_use_ai")
def completions(request, payload: CompletionRequest):
    """
    Generate text completions using the configured AI provider.

    This endpoint provides a unified interface for text completions.
    """
    try:
        # Get AI provider
        provider = (
            get_ai_provider(payload.provider_type)
            if payload.provider_type
            else get_default_ai_provider()
        )

        # Generate completion
        response = provider.completion(
            prompt=payload.prompt,
            model=payload.model,
            temperature=payload.temperature,
            max_tokens=payload.max_tokens,
        )

        return JsonResponse(
            {
                "id": f"cmpl-{response.metadata.get('response_id', 'unknown')}"
                if response.metadata
                else "cmpl-unknown",
                "object": "text_completion",
                "created": int(asyncio.get_event_loop().time()),
                "model": response.model,
                "provider": response.provider,
                "choices": [
                    {
                        "text": response.content,
                        "index": 0,
                        "finish_reason": response.metadata.get("finish_reason", "stop")
                        if response.metadata
                        else "stop",
                    }
                ],
                "usage": response.usage or {},
            }
        )

    except Exception as e:
        return handle_ai_error(e)


@router.get("/providers")
@has_permission("can_use_ai")
def list_providers(request):
    """
    List available AI providers and their status.
    """
    try:
        providers = AIProviderFactory.get_available_providers()
        health_status = AIProviderFactory.health_check_all()

        return JsonResponse(
            {
                "providers": [
                    {
                        "name": provider,
                        "available": provider in health_status,
                        "healthy": health_status.get(provider, False),
                    }
                    for provider in providers
                ]
            }
        )

    except Exception as e:
        return handle_ai_error(e)


@router.get("/providers/{provider_type}/models")
@has_permission("can_use_ai")
def list_models(request, provider_type: str):
    """
    List available models for a specific provider.
    """
    try:
        provider = get_ai_provider(provider_type)
        models = provider.get_available_models()

        return JsonResponse({"provider": provider_type, "models": models})

    except Exception as e:
        return handle_ai_error(e)


@router.get("/providers/{provider_type}/health")
@has_permission("can_use_ai")
def provider_health(request, provider_type: str):
    """
    Check health of a specific provider.
    """
    try:
        provider = get_ai_provider(provider_type)
        is_healthy = provider.health_check()

        return JsonResponse(
            {
                "provider": provider_type,
                "healthy": is_healthy,
                "timestamp": int(asyncio.get_event_loop().time()),
            }
        )

    except Exception as e:
        return handle_ai_error(e)


@router.post("/providers/configure")
@has_permission("can_manage_ai_providers")
def configure_provider(request, payload: ProviderConfigRequest):
    """
    Configure a specific AI provider.
    Note: This creates a new instance with the given config.
    """
    try:
        provider = AIProviderFactory.create_provider(
            payload.provider_type, payload.config, use_env=False, force_new=True
        )

        # Test the configuration
        is_healthy = provider.health_check()

        return JsonResponse(
            {"provider": payload.provider_type, "configured": True, "healthy": is_healthy}
        )

    except Exception as e:
        return handle_ai_error(e)


# Legacy Django views for backward compatibility
@method_decorator(csrf_exempt, name="dispatch")
@method_decorator(has_permission("can_use_ai"), name="dispatch")
class ChatCompletionView(View):
    """Legacy Django view for chat completions."""

    def post(self, request):
        try:
            data = json.loads(request.body)

            # Convert to schema format
            payload = ChatCompletionRequest(**data)

            # Reuse the router logic
            return chat_completions(request, payload)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            return handle_ai_error(e)


@method_decorator(csrf_exempt, name="dispatch")
@method_decorator(has_permission("can_use_ai"), name="dispatch")
class CompletionView(View):
    """Legacy Django view for completions."""

    def post(self, request):
        try:
            data = json.loads(request.body)

            # Convert to schema format
            payload = CompletionRequest(**data)

            # Reuse the router logic
            return completions(request, payload)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            return handle_ai_error(e)
