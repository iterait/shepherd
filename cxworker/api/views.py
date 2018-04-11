from io import StringIO, BytesIO

from flask import Blueprint, request, jsonify, Response
from minio import Minio
from minio.error import MinioError
from schematics import Model
from schematics.exceptions import DataError, FieldError
from typing import TypeVar, Type

from cxworker.shepherd.shepherd import Shepherd
from .requests import StartJobRequest
from .responses import StartJobResponse, StatusResponse, JobStatusResponse
from .errors import ClientActionError, StorageError, UnknownJobError


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


def check_job_exists(minio, job_id):
    try:
        if not minio.bucket_exists(job_id):
            raise ClientActionError('Minio bucket `{}` does not exist'.format(job_id))
    except MinioError as me:
        raise StorageError('Failed to check minio bucket `{}`'.format(job_id)) from me


def serialize_response(response: Model) -> Response:
    response.validate()
    return jsonify(response.to_primitive())


def create_worker_blueprint(shepherd: Shepherd, minio: Minio):
    worker = Blueprint('worker', __name__)

    @worker.route('/start-job', methods=['POST'])
    def start_job():
        """Start a new job."""
        start_job_request = load_request(StartJobRequest)

        if not start_job_request.payload:
            check_job_exists(minio, start_job_request.job_id)
        else:
            minio.make_bucket(start_job_request.job_id)
            payload_data = start_job_request.payload.encode()
            payload = BytesIO(payload_data)
            minio.put_object(start_job_request.job_id, start_job_request.payload_name,
                             payload, len(start_job_request.payload))

        try:
            shepherd.is_job_done(start_job_request.job_id)
            # if the call didn't throw, the job is either done or being computed, no need to enqueue it
            return serialize_response(StartJobResponse())
        except UnknownJobError:
            pass
        shepherd.enqueue_job(start_job_request.job_id, start_job_request.model, start_job_request.sheep_id)
        return serialize_response(StartJobResponse())

    @worker.route("/jobs/<job_id>/ready", methods=["GET"])
    def is_ready(job_id):
        """
        Check if a job has already been processed.
        A job that ended with an error is considered ready.

        :param job_id: An identifier of the queried job
        """
        check_job_exists(minio, job_id)
        return serialize_response(JobStatusResponse({'ready': shepherd.is_job_done(job_id)}))

    @worker.route("/jobs/<job_id>/wait_ready", methods=["GET"])
    def wait_ready(job_id):
        """
        Wait until the specified job is ready.

        :param job_id: An identifier of the queried job
        """
        check_job_exists(minio, job_id)
        shepherd.notifier.wait_for(lambda: shepherd.is_job_done(job_id))
        return serialize_response(JobStatusResponse({'ready': shepherd.is_job_done(job_id)}))

    @worker.route('/status', methods=['GET'])
    def get_status():
        """Get status of all the sheep available."""
        response = StatusResponse()
        response.containers = dict(shepherd.get_status())
        return serialize_response(response)

    return worker
