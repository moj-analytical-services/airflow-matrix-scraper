import logging


class ContextFilter(logging.Filter):
    """
    This is just overkill to apply a default context param to the log.
    But it does mean I don't have to define extra everytime I wanna log.
    So keeping it.
    """

    def filter(self, record):
        if not getattr(record, "context", None):
            record.context = "PROCESSING"
        return True
