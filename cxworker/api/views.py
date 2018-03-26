from flask import Blueprint, request, jsonify
from minio import Minio
from minio.error import MinioError

from cxworker.manager.registry import ContainerRegistry
from .requests import StartJobRequest, InterruptJobRequest, ReconfigureRequest
from .responses import StartJobResponse, InterruptJobResponse, StatusResponse, ReconfigureResponse
from .schemas import StartJobRequestSchema, InterruptJobRequestSchema, ReconfigureRequestSchema
from .errors import ClientActionError, StorageError


def load_request(schema):
    """
    Load request data according to given schema.
    This function must be called from a request context.
    """

    json = request.get_json()
    if json is None:
        raise ClientActionError('The body must be a valid JSON document')

    result, errors = schema.load(json)
    if errors:
        raise ClientActionError('Invalid value in fields {}'.format(', '.join(errors.keys())))

    return result


def create_worker_blueprint(registry: ContainerRegistry, minio: Minio):
    worker = Blueprint('worker', __name__)

    @worker.route('/start-job', methods=['POST'])
    def start_job():
        request_data = load_request(StartJobRequestSchema())
        start_job_request = StartJobRequest(**request_data)

        try:
            payload = minio.get_object(start_job_request.id, start_job_request.source_url).read()
        except MinioError as me:
            raise StorageError('Can not obtain job payload from minio storage {}:{} ({})'
                               .format(start_job_request.id, start_job_request.source_url, str(me))) from me

        if start_job_request.refresh_model:
            registry.refresh_model(start_job_request.container_id)

        registry.send_input(start_job_request.container_id, start_job_request, payload)

        return jsonify(StartJobResponse().dump())

    @worker.route('/interrupt-job', methods=['POST'])
    def interrupt_job():
        request_data = load_request(InterruptJobRequestSchema())
        interrupt_job_request = InterruptJobRequest(**request_data)

        registry.kill_container(interrupt_job_request.container_id)

        return jsonify(InterruptJobResponse().dump())

    @worker.route('/reconfigure', methods=['POST'])
    def reconfigure():
        request_data = load_request(ReconfigureRequestSchema())
        reconfigure_request = ReconfigureRequest(request_data)

        registry.start_container(reconfigure_request.container_id, reconfigure_request.model_name,
                                 reconfigure_request.model_version)

        return jsonify(ReconfigureResponse().dump())

    @worker.route('/status', methods=['GET'])
    def get_status():
        return jsonify(StatusResponse(registry.get_status()).dump())

    return worker
