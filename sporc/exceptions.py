"""
Custom exceptions for the SPORC package.
"""


class SPORCError(Exception):
    """Base exception for SPORC package errors."""
    pass


class DatasetAccessError(SPORCError):
    """Raised when there's an error accessing the SPORC dataset."""
    pass


class AuthenticationError(SPORCError):
    """Raised when Hugging Face authentication fails."""
    pass


class NotFoundError(SPORCError):
    """Raised when a requested resource is not found."""
    pass


class ValidationError(SPORCError):
    """Raised when data validation fails."""
    pass


class ConfigurationError(SPORCError):
    """Raised when there's a configuration error."""
    pass