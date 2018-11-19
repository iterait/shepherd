import json
import logging
import os
from aiohttp import web

from functools import partial

from ..errors.api import ClientActionError, AppError, NameConflictError, StorageInaccessibleError
from .responses import ErrorResponse
from .swagger import swagger


def internal_error_handler(error: Exception):
    """
    Handles internal server errors
    :param error: an exception object
    :return: a Flask response
    """

    response = ErrorResponse({"message": 'Internal server error ({})'.format(str(error))})
    logging.exception(error)
    return web.Response(text=json.dumps(response.to_primitive()), content_type='application/json'), 500


def error_handler(http_code, error: AppError):
    """
    Handles errors derived from AppError
    :param http_code: the HTTP status code to be returned when the error happens
    :param error: an exception object
    :return: a Flask response
    """

    response = ErrorResponse({"message": str(error)})
    return web.Response(text=json.dumps(response.to_primitive()), content_type='application/json'), http_code


def create_app():
    app = web.Application(debug=os.getenv('DEBUG', False))
    # TODO CORS(app, expose_headers=["Content-Disposition"], send_wildcard=True, origins=[])

    swagger.error_middleware.add_handler(NameConflictError, partial(error_handler, 409))
    swagger.error_middleware.add_handler(ClientActionError, partial(error_handler, 400))
    swagger.error_middleware.add_handler(StorageInaccessibleError, partial(error_handler, 503))
    swagger.error_middleware.add_handler(AppError, partial(error_handler, 500))
    swagger.error_middleware.add_handler(Exception, internal_error_handler)

    swagger.init_app(app)

    return app
