class TEDError(Exception):
    """Generic exception for TED errors"""


class NotAMondayError(ValueError, TEDError):
    """The specified date is not a Monday"""
