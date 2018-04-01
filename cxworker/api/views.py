from flask import Blueprint, request, jsonify, Response
from minio import Minio
from minio.error import MinioError
from schematics import Model
from schematics.exceptions import DataError, FieldError
from typing import TypeVar, Type

from cxworker.manager.shepherd import Shepherd
from .requests import StartJobRequest, InterruptJobRequest, ReconfigureRequest
from .responses import StartJobResponse, InterruptJobResponse, StatusResponse, ReconfigureResponse
from .errors import ClientActionError, StorageError


T = TypeVar('T', bound=Model)


def load_request(schema_class: Type[T]) -> T:
    """
    Load request data according to given schema.
    This function must be called from a request context.
    """

    json = request.get_json()
    if json is None:
        raise ClientActionError('The body must be a valid JSON document')

    try:
        result = schema_class.__new__(schema_class)
        result.__init__(json, validate=True)
        return result
    except DataError as exception:
        raise ClientActionError("There are errors in the following fields: {}".format(exception.errors)) from exception
    except FieldError as exception:
        raise ClientActionError("There are errors in the following fields: {}".format(exception.errors)) from exception


def serialize_response(response: Model) -> Response:
    response.validate()
    return jsonify(response.to_primitive())


def create_worker_blueprint(shepherd: Shepherd, minio: Minio):
    worker = Blueprint('worker', __name__)

    @worker.route('/start-job', methods=['POST'])
    def start_job():
        start_job_request = load_request(StartJobRequest)

        try:
            if not minio.bucket_exists(start_job_request.job_id):
                raise StorageError('Minio bucket `{}` does not exist'.format(start_job_request.job_id))
        except MinioError as me:
            raise StorageError('Failed to check minio bucket `{}`'.format(start_job_request.job_id)) from me

        if start_job_request.refresh_model:
            shepherd.refresh_model(start_job_request.sheep_id)

        shepherd.enqueue_job(start_job_request.sheep_id, start_job_request.job_id)

        return serialize_response(StartJobResponse())

    @worker.route('/interrupt-job', methods=['POST'])
    def interrupt_job():
        interrupt_job_request = load_request(InterruptJobRequest)

        shepherd.slaughter_sheep(interrupt_job_request.sheep_id)

        return serialize_response(InterruptJobResponse())

    @worker.route('/reconfigure', methods=['POST'])
    def reconfigure():
        reconfigure_request = load_request(ReconfigureRequest)

        shepherd.start_sheep(reconfigure_request.sheep_id, reconfigure_request.model.name,
                             reconfigure_request.model.version)

        return serialize_response(ReconfigureResponse())

    @worker.route('/status', methods=['GET'])
    def get_status():
        response = StatusResponse()
        response.containers = dict(shepherd.get_status())
        return serialize_response(response)

    return worker
