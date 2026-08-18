"""Dalgo's AI features and the infrastructure they share.

agent/ holds one module per agent plus the shared loop pieces (middleware,
run context, checkpointer). chat/ is the Chat with Data turn pipeline,
llm_calls/ are one-shot model calls, tools/ + guards/ + messages/ +
tracing.py are shared infrastructure.
"""
