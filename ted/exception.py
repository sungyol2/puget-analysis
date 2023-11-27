class TEDError(Exception):
    """Generic exception for TED errors"""


class NotAMondayError(ValueError, TEDError):
    """The specified date is not a Monday"""


class FareNotFoundError(ValueError, TEDError):
    """A fare was not found"""


class NoExistingFareError(ValueError, TEDError):
    """An existing fare was not found"""
