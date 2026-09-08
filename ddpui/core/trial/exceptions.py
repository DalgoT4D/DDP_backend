class TrialAccountExistsError(Exception):
    """raised when a trial is requested for an email that already has a Dalgo account"""


class TrialCloneError(RuntimeError):
    """raised when any step of a template→trial clone fails.

    Subclasses RuntimeError so pre-existing callers/tests catching RuntimeError keep
    working, while giving the API/task layer a feature-specific type to map distinctly
    from unrelated RuntimeErrors.
    """
