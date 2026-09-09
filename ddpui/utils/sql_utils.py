"""SQL expression utilities."""

import re


def safe_division_expression(expression: str) -> str:
    """Wrap division denominators with NULLIF(..., 0) to prevent DivisionByZero errors.

    Scans a raw SQL expression for the ``/`` operator and wraps each
    denominator so that ``a / b`` becomes ``a / NULLIF(b, 0)``.  The
    denominator may be a parenthesised sub-expression, a function call
    (e.g. ``SUM(col)``), or a plain identifier/number.
    """
    result: list[str] = []
    i = 0
    length = len(expression)

    while i < length:
        ch = expression[i]

        # Skip string literals so we don't touch '/' inside quoted values
        if ch in ("'", '"'):
            quote = ch
            result.append(ch)
            i += 1
            while i < length and expression[i] != quote:
                if expression[i] == "\\" and i + 1 < length:
                    result.append(expression[i])
                    i += 1
                result.append(expression[i])
                i += 1
            if i < length:
                result.append(expression[i])
                i += 1
            continue

        if ch != "/":
            result.append(ch)
            i += 1
            continue

        # Found a division operator
        result.append("/")
        i += 1

        # Preserve whitespace between / and the denominator
        while i < length and expression[i] == " ":
            result.append(" ")
            i += 1

        if i >= length:
            break

        denominator, end_pos = _extract_term(expression, i)
        result.append(f"NULLIF({denominator}, 0)")
        i = end_pos

    return "".join(result)


def _extract_term(expression: str, start: int) -> tuple[str, int]:
    """Extract the next SQL term starting at *start*.

    A term is one of:
    * a parenthesised sub-expression ``(…)``
    * a function call ``FUNC_NAME(…)``
    * a plain identifier, qualified name, or numeric literal
    """
    i = start

    # Parenthesised sub-expression
    if expression[i] == "(":
        end = _find_matching_paren(expression, i)
        return expression[i:end], end

    # Function call — identifier followed (possibly with spaces) by '('
    m = re.match(r"[A-Za-z_]\w*", expression[i:])
    if m:
        after_name = i + m.end()
        # Skip optional whitespace between name and '('
        j = after_name
        while j < len(expression) and expression[j] == " ":
            j += 1
        if j < len(expression) and expression[j] == "(":
            end = _find_matching_paren(expression, j)
            return expression[i:end], end
        # Plain identifier (no parens)
        return m.group(), after_name

    # Numeric literal (including decimals like 3.14)
    m = re.match(r"\d+(?:\.\d+)?", expression[i:])
    if m:
        return m.group(), i + m.end()

    # Fallback — single character
    return expression[i], i + 1


def _find_matching_paren(expression: str, start: int) -> int:
    """Return the index *after* the closing paren that matches ``expression[start]``."""
    depth = 1
    i = start + 1
    length = len(expression)
    while i < length and depth > 0:
        ch = expression[i]
        if ch in ("'", '"'):
            # Skip string literals inside parens
            quote = ch
            i += 1
            while i < length and expression[i] != quote:
                if expression[i] == "\\" and i + 1 < length:
                    i += 1
                i += 1
            # Move past the closing quote
            if i < length:
                i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        i += 1
    return i
