from flask import Flask, jsonify
from functools import partial
import logging

from .errors import ClientActionError, AppError
from .responses import ErrorResponse


def internal_error_handler(error: Exception):
    """
    Handles internal server errors
    :param error: an exception object
    :return: a Flask response
    """

    response = ErrorResponse('Internal server error ({})'.format(str(error)))
    logging.exception(error)
    return jsonify(response.dump()), 500


def error_handler(http_code, error: AppError):
    """
    Handles errors derived from AppError
    :param http_code: the HTTP status code to be returned when the error happens
    :param error: an exception object
    :return: a Flask response
    """

    response = ErrorResponse(str(error))
    return jsonify(response.dump()), http_code


def create_app(name: str):
    app = Flask(name)

    app.register_error_handler(ClientActionError, partial(error_handler, 400))
    app.register_error_handler(AppError, partial(error_handler, 500))
    app.register_error_handler(Exception, internal_error_handler)

    return app
