"""Shared Chroma HTTP client helpers."""

from functools import lru_cache

from chromadb import ClientAPI, HttpClient


@lru_cache(maxsize=8)
def get_shared_chroma_http_client(host: str, port: int, ssl: bool) -> ClientAPI:
    """Return a shared Chroma HTTP client for one host/port/ssl tuple."""
    return HttpClient(host=host, port=port, ssl=ssl)
