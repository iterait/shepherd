from flask import Flask, jsonify
from flask_cors import CORS
from functools import partial
import logging

from .errors import ClientActionError, AppError, NameConflictError, StorageInaccessibleError
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
    return jsonify(response.to_primitive()), 500


def error_handler(http_code, error: AppError):
    """
    Handles errors derived from AppError
    :param http_code: the HTTP status code to be returned when the error happens
    :param error: an exception object
    :return: a Flask response
    """

    response = ErrorResponse({"message": str(error)})
    return jsonify(response.to_primitive()), http_code


def create_app(name: str):
    app = Flask(name)
    CORS(app, expose_headers=["Content-Disposition"], send_wildcard=True, origins=[])

    app.register_error_handler(NameConflictError, partial(error_handler, 409))
    app.register_error_handler(ClientActionError, partial(error_handler, 400))
    app.register_error_handler(StorageInaccessibleError, partial(error_handler, 503))
    app.register_error_handler(AppError, partial(error_handler, 500))
    app.register_error_handler(Exception, internal_error_handler)

    swagger.init_app(app)

    return app
