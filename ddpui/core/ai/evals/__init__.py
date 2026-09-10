"""Offline evals for Chat with Data (evals-plan.md Phase 1).

A golden dataset of questions runs through the real TurnGraph; hard metrics
(routing equality, execution-based SQL comparison) gate, autoevals LLM judges
inform. All scores land in self-hosted Langfuse as dataset runs.
"""
