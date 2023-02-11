"""Exceptions for Synpp."""


class PipelineError(Exception):
    """Exception class for general pipeline error."""

    pass


class PipelineParallelError(Exception):
    """Exception class for error related to parallel execution."""

    pass
