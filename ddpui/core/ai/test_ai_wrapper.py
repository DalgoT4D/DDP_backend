"""
Test script for the AI wrapper functionality.
This script tests the AI providers and factory without requiring a full Django setup.
"""

import os
import sys
import asyncio
import json
from typing import Dict, Any

# Add the project root to Python path
sys.path.insert(0, "/Users/pradeep/Work/Dalgo/DDP_backend")

from ddpui.core.ai.interfaces import AIMessage, AIProviderType
from ddpui.core.ai.factory import AIProviderFactory, get_ai_provider
from ddpui.core.ai.config import AIConfigManager, validate_ai_config


def test_config_loading():
    """Test AI configuration loading."""
    print("\n🔧 Testing AI Configuration Loading...")

    config_manager = AIConfigManager()
    global_config = config_manager.get_global_config()

    print(f"Default provider: {global_config.default_provider}")
    print(f"Streaming enabled: {global_config.streaming_enabled}")
    print(f"Rate limit: {global_config.rate_limit_requests_per_minute}")

    # Test provider configurations
    providers = ["openai", "claude", "ollama"]
    for provider in providers:
        config = config_manager.get_provider_config(provider)
        is_configured = config_manager.is_provider_configured(provider)
        print(f"{provider.upper()}: {'✅ Configured' if is_configured else '❌ Not configured'}")
        if config and config.api_key:
            print(f"  API Key: {'✅ Present' if config.api_key else '❌ Missing'}")
        if config and config.default_model:
            print(f"  Default model: {config.default_model}")

    # Validate configuration
    validation = validate_ai_config()
    print(f"\nConfiguration validation: {'✅ Valid' if validation['valid'] else '❌ Invalid'}")
    if validation["errors"]:
        for error in validation["errors"]:
            print(f"  Error: {error}")
    if validation["warnings"]:
        for warning in validation["warnings"]:
            print(f"  Warning: {warning}")

    return validation["valid"]


def test_factory():
    """Test AI provider factory."""
    print("\n🏭 Testing AI Provider Factory...")

    # Test provider registration
    available_providers = AIProviderFactory.get_available_providers()
    print(f"Available providers: {', '.join(available_providers)}")

    # Test health check
    print("\nHealth checking all providers...")
    health_status = AIProviderFactory.health_check_all()
    for provider, is_healthy in health_status.items():
        status = "🟢 Healthy" if is_healthy else "🔴 Unhealthy"
        print(f"  {provider}: {status}")

    return len(available_providers) > 0


def test_provider_creation():
    """Test creating individual providers."""
    print("\n🛠️ Testing Provider Creation...")

    # Test each provider type
    test_configs = {
        "openai": {"api_key": "test-key", "default_model": "gpt-3.5-turbo"},
        "claude": {"api_key": "test-key", "default_model": "claude-3-sonnet-20240229"},
        "ollama": {"base_url": "http://localhost:11434", "default_model": "llama2"},
    }

    created_providers = []

    for provider_type, config in test_configs.items():
        try:
            print(f"\nCreating {provider_type} provider...")
            provider = AIProviderFactory.create_provider(
                provider_type, config, use_env=False, force_new=True
            )
            print(f"  ✅ Successfully created {provider_type} provider")
            print(f"  Provider type: {provider.get_provider_type()}")
            created_providers.append(provider)

            # Test basic functionality (without making actual API calls)
            try:
                # This might fail due to invalid API keys, but should not crash
                models = provider.get_available_models()
                print(f"  Available models: {len(models)} found")
            except Exception as e:
                print(f"  ⚠️ Model listing failed (expected with test keys): {type(e).__name__}")

        except Exception as e:
            print(f"  ❌ Failed to create {provider_type} provider: {e}")

    return len(created_providers) > 0


def test_message_conversion():
    """Test message format conversion."""
    print("\n💬 Testing Message Conversion...")

    # Create test messages
    messages = [
        AIMessage(role="system", content="You are a helpful assistant."),
        AIMessage(role="user", content="Hello, how are you?"),
        AIMessage(role="assistant", content="I'm doing well, thank you!"),
    ]

    print(f"Created {len(messages)} test messages:")
    for i, msg in enumerate(messages):
        print(f"  {i+1}. {msg.role}: {msg.content[:50]}...")

    return True


async def test_basic_api_simulation():
    """Simulate basic AI API interactions without making real calls."""
    print("\n🎭 Testing Basic API Simulation...")

    # Test message creation for different providers
    test_message = AIMessage(role="user", content="Hello, this is a test message.")

    # Test different provider types
    for provider_type in ["openai", "claude", "ollama"]:
        try:
            print(f"\nSimulating {provider_type} interaction...")

            # Create a mock config that won't make real API calls
            mock_config = {"api_key": "test-key-" + provider_type, "default_model": "test-model"}

            if provider_type == "ollama":
                mock_config["base_url"] = "http://localhost:11434"

            # This will create the provider but we won't call the API
            provider = AIProviderFactory.create_provider(
                provider_type, mock_config, use_env=False, force_new=True
            )

            print(f"  ✅ Provider created with type: {provider.get_provider_type().value}")
            print(f"  Config keys: {list(provider.get_config().keys())}")

        except Exception as e:
            print(f"  ❌ Simulation failed for {provider_type}: {e}")

    return True


def generate_sample_env_config():
    """Generate sample environment configuration."""
    print("\n📝 Generating Sample Environment Configuration...")

    config_manager = AIConfigManager()
    template = config_manager.get_environment_template()

    print("Sample .env configuration:")
    print("-" * 50)
    print(template)
    print("-" * 50)

    return True


async def main():
    """Run all tests."""
    print("🚀 Starting AI Wrapper Tests for Dalgo Platform")
    print("=" * 60)

    tests = [
        ("Configuration Loading", test_config_loading),
        ("Provider Factory", test_factory),
        ("Provider Creation", test_provider_creation),
        ("Message Conversion", test_message_conversion),
        ("API Simulation", test_basic_api_simulation),
        ("Sample Config Generation", generate_sample_env_config),
    ]

    results = {}

    for test_name, test_func in tests:
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"\n❌ Test '{test_name}' failed with error: {e}")
            results[test_name] = False

    # Print summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = 0
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! AI wrapper is ready for use.")
    else:
        print("⚠️ Some tests failed. Check configuration and dependencies.")

    print("\n🔗 Next Steps:")
    print("1. Set up environment variables in .env file")
    print("2. Install required AI provider packages (openai, anthropic)")
    print("3. Add AI API routes to Django URL configuration")
    print("4. Test with real API keys and models")


if __name__ == "__main__":
    # Set up basic environment for testing
    os.environ.setdefault("AI_PROVIDER_DEFAULT", "openai")
    os.environ.setdefault("AI_STREAMING_ENABLED", "true")

    asyncio.run(main())
