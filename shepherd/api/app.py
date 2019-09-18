import os
import logging
from aiohttp import web
from aiohttp.web_exceptions import HTTPError
from apistrap.errors import InvalidFieldsError

from .openapi import oapi
from .responses import ErrorResponse
from ..errors.api import ApiClientError, ApiServerError, NameConflictError, StorageInaccessibleError, UnknownJobError, \
    UnknownSheepError


def internal_error_handler(error: Exception):
    """
    Handles internal server errors

    :param error: an exception object
    :return: a Flask response
    """
    logging.exception(error)
    return (ErrorResponse({"message": 'Internal server error ({})'.format(str(error))})), 500


def error_handler(error: ApiServerError):
    """
    Handles errors derived from apistrap ApiServerError

    :param error: an exception object
    :return: a Flask response
    """
    return ErrorResponse({"message": str(error)})


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

    oapi.add_error_handler(NameConflictError, 409, error_handler)
    oapi.add_error_handler(ApiClientError, 400, error_handler)
    oapi.add_error_handler(UnknownJobError, 404, error_handler)
    oapi.add_error_handler(UnknownSheepError, 404, error_handler)
    oapi.add_error_handler(InvalidFieldsError, 400, error_handler)
    oapi.add_error_handler(StorageInaccessibleError, 503, error_handler)
    oapi.add_error_handler(HTTPError, None, http_error_handler)
    oapi.add_error_handler(ApiServerError, 500, error_handler)
    oapi.add_error_handler(Exception, None, internal_error_handler)

    oapi.init_app(app)

    return app
