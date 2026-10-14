"""Single one-shot LLM calls. Each file asks the model ONE question and returns —
no tools, no loops. Cheap (Haiku) and disposable; if one fails the turn
continues (fail-open)."""
