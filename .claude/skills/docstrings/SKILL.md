---
name: docstrings
description: Docstring style — skip the docstring when the function name already explains what it does; when you do write one, keep it to a single line focused on the non-obvious WHY. Never write multi-line Args / Returns / Raises blocks. Apply when authoring new functions or classes, reviewing a diff, or judging whether an existing docstring should stay.
---

# docstrings

## 1. Skip the docstring when the name says it

If a reader can predict what the function does from its name + signature, don't paraphrase it in prose. Delete the docstring.

```python
# ✗ BAD
def get_user_by_email(email: str) -> User:
    """Get a user by email."""
    return User.objects.get(email=email)
```

```python
# ✓ GOOD
def get_user_by_email(email: str) -> User:
    return User.objects.get(email=email)
```

Same rule applies to classes, methods, and test functions.

## 2. When you do write one, keep it to a single line

A docstring earns its place by answering *what would surprise a reader*: a hidden invariant, a subtle side effect, a workaround for a specific bug, a constraint the caller must respect. Not what the code does — the reader can see that.

```python
# ✗ BAD — 4 lines of what the code already says
def normalize_email(email: str) -> str:
    """Normalize an email address.

    Lowercases the address and strips whitespace, then returns
    the cleaned string.
    """
    return email.strip().lower()
```

```python
# ✓ GOOD — name says it, no docstring
def normalize_email(email: str) -> str:
    return email.strip().lower()
```

```python
# ✓ GOOD — the WHY isn't obvious from the name
def _resolve_pending_emails(org, orguser, emails, invite_role_uuid, group_name=None) -> dict[str, int]:
    """Emails that already belong to an active orguser are ignored — the caller resolves them via orguser_ids."""
    ...
```

## 3. Never write Args / Returns / Raises blocks

Types live in the signature. If a parameter needs a prose explanation, rename it — don't document around a bad name.

```python
# ✗ BAD
def notify_share_recipients(sender, rtype, resource, classified):
    """Fire share notifications.

    Args:
        sender: The user sharing the resource.
        rtype: The resource type as a string.
        resource: The resource object.
        classified: Dict with "new" and "upgrade" keys mapping level → orguser ids.

    Returns:
        None.
    """
    ...
```

```python
# ✓ GOOD — one line, focused on the invariant a reader can't infer
def notify_share_recipients(sender, rtype, resource, classified):
    """Fires one create_notification per (class, level) bucket. Notification failure is logged but never fails the API call."""
    ...
```

## Reviewing existing docstrings

When editing a file, apply the same rules to what's already there — delete a docstring whose name-and-signature already say the same thing. Trim multi-line ones down to the single line that carries the WHY, or delete them if there's no non-obvious WHY.
