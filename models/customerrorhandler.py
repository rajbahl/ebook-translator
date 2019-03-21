from sanic.handlers import ErrorHandler
import logging


logger = logging.getLogger("customerrorhandler")


class CustomErrorHandler(ErrorHandler):
    def default(self, request, exception):
        # Handles errors that have no error handlers assigned
        # You custom error handling logic...
        logger.error("Custom Error from " + str(request) + " with " + str(exception))
        raise Exception(exception)
