from typing import Optional, List, Dict, Any

from ninja import Schema


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
