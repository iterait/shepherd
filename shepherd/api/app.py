import logging
import os
from aiohttp import web
from aiohttp.web_exceptions import HTTPError

from functools import partial

from ..errors.api import ClientActionError, AppError, NameConflictError, StorageInaccessibleError, UnknownJobError, \
    UnknownSheepError
from .responses import ErrorResponse
from .swagger import swagger


def internal_error_handler(error: Exception):
    """
    Handles internal server errors
    :param error: an exception object
    :return: a Flask response
    """
    logging.exception(error)
    return (ErrorResponse({"message": 'Internal server error ({})'.format(str(error))})), 500


def error_handler(http_code, error: AppError):
    """
    Handles errors derived from AppError
    :param http_code: the HTTP status code to be returned when the error happens
    :param error: an exception object
    :return: a Flask response
    """
    return (ErrorResponse({"message": str(error)})), http_code


def http_error_handler(error: HTTPError):
    """
    Handles HTTP errors
    :param error: an exception object
    :return: a Flask response
    """
    return (ErrorResponse({"message": error.text})), error.status_code


def create_app(debug=None) -> web.Application:
    """
    Create the AioHTTP app.

    :param debug: If set, determines whether the app should run in debug mode.
                  If not set, the `DEBUG` environment variable is used (with False as default).
    :return: a new application object
    """

    app = web.Application(debug=debug if debug is not None else os.getenv('DEBUG', False), client_max_size=10*1024**3)

    swagger.error_middleware.add_handler(NameConflictError, partial(error_handler, 409))
    swagger.error_middleware.add_handler(ClientActionError, partial(error_handler, 400))
    swagger.error_middleware.add_handler(UnknownJobError, partial(error_handler, 404))
    swagger.error_middleware.add_handler(UnknownSheepError, partial(error_handler, 404))
    swagger.error_middleware.add_handler(StorageInaccessibleError, partial(error_handler, 503))
    swagger.error_middleware.add_handler(HTTPError, http_error_handler)
    swagger.error_middleware.add_handler(AppError, partial(error_handler, 500))
    swagger.error_middleware.add_handler(Exception, internal_error_handler)

    swagger.init_app(app)

    return app
