from flask import Flask, jsonify
from functools import partial
import logging

from minio import Minio

from ..manager.registry import ContainerRegistry
from .errors import ClientActionError, AppError
from .responses import ErrorResponse
from .views import create_worker_blueprint


def internal_error_handler(error: Exception):
    """
    Handles internal server errors
    :param error: an exception object
    :return: a Flask response
    """

    response = ErrorResponse('Internal server error')
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


def create_app():
    app = Flask(__name__)

    app.register_error_handler(ClientActionError, partial(error_handler, 400))
    app.register_error_handler(AppError, partial(error_handler, 500))
    app.register_error_handler(Exception, internal_error_handler)

    return app
