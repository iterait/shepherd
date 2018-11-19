from aiohttp import web
from aiohttp.web_request import Request
from apistrap.types import FileResponse
from io import BytesIO
from datetime import datetime
import calendar

import mimetypes

from minio import Minio
from minio.error import MinioError, BucketAlreadyExists, BucketAlreadyOwnedByYou

from ..constants import DONE_FILE, ERROR_FILE, DEFAULT_OUTPUT_FILE, OUTPUT_DIR, DEFAULT_PAYLOAD_PATH
from ..shepherd import Shepherd
from ..utils import minio_object_exists
from .requests import StartJobRequest
from .responses import StartJobResponse, StatusResponse, JobStatusResponse, ErrorResponse, \
    JobErrorResponse, JobReadyResponse
from ..errors.api import ClientActionError, StorageError, UnknownJobError, NameConflictError
from .swagger import swagger


def check_job_exists(minio, job_id):
    try:
        if not minio.bucket_exists(job_id):
            raise ClientActionError('Minio bucket `{}` does not exist'.format(job_id))
    except MinioError as me:
        raise StorageError('Failed to check minio bucket `{}`'.format(job_id)) from me


def create_shepherd_routes(shepherd: Shepherd, minio: Minio):
    api = web.RouteTableDef()

    @api.post('/start-job')
    @swagger.autodoc()
    @swagger.accepts(StartJobRequest)
    @swagger.responds_with(StartJobResponse)
    async def start_job(request: Request, start_job_request: StartJobRequest):
        """Start a new job."""
        if not start_job_request.payload:
            check_job_exists(minio, start_job_request.job_id)
        else:
            try:
                minio.make_bucket(start_job_request.job_id)
            except (BucketAlreadyExists, BucketAlreadyOwnedByYou) as e:
                raise NameConflictError("A job with this ID was already submitted") from e

            payload_data = start_job_request.payload.encode()
            payload = BytesIO(payload_data)
            minio.put_object(start_job_request.job_id, DEFAULT_PAYLOAD_PATH,
                             payload, len(start_job_request.payload))

        try:
            shepherd.is_job_done(start_job_request.job_id)
            # if the call didn't throw, the job is either done or being computed, no need to enqueue it
            return StartJobResponse()
        except UnknownJobError:
            pass

        await shepherd.enqueue_job(start_job_request.job_id, start_job_request.model, start_job_request.sheep_id)

        return StartJobResponse()

    @api.get("/jobs/{job_id}/ready")
    @swagger.autodoc()
    @swagger.responds_with(JobReadyResponse)
    async def is_ready(request: Request):
        """
        Check if a job has already been processed.
        A job that ended with an error is considered ready.

        :param job_id: An identifier of the queried job
        """
        job_id = request.match_info['job_id']

        check_job_exists(minio, job_id)
        ready = shepherd.is_job_done(job_id)
        if ready:
            timestamp = minio.stat_object(job_id, 'done').last_modified
            formatted_timestamp = datetime.fromtimestamp(calendar.timegm(timestamp))
        else:
            formatted_timestamp = None
        return JobReadyResponse({'ready': ready,
                                 'finished_at': formatted_timestamp})

    @api.get("/jobs/{job_id}/wait_ready")
    @swagger.autodoc()
    @swagger.responds_with(JobStatusResponse)
    async def wait_ready(request: Request):
        """
        Wait until the specified job is ready.

        :param job_id: An identifier of the queried job
        """
        job_id = request.match_info['job_id']

        check_job_exists(minio, job_id)
        async with shepherd.job_done_condition:
            await shepherd.job_done_condition.wait_for(lambda: shepherd.is_job_done(job_id))
        return JobStatusResponse({'ready': shepherd.is_job_done(job_id)})

    @api.get("/jobs/{job_id}/result/{result_file}")
    @api.get("/jobs/{job_id}/result")
    @swagger.autodoc()
    @swagger.responds_with(JobStatusResponse, code=202)
    @swagger.responds_with(ErrorResponse, code=404)
    @swagger.responds_with(JobErrorResponse, code=500)
    @swagger.responds_with(FileResponse, code=200)
    async def get_job_result(request: Request):
        """
        Get the result of the specified job.

        :param job_id: An identifier of the job
        :param result_file: Name of the requested file
        """
        job_id = request.match_info['job_id']
        result_file = request.match_info.get('result_file', DEFAULT_OUTPUT_FILE)

        check_job_exists(minio, job_id)

        if minio_object_exists(minio, job_id, ERROR_FILE):
            message = minio.get_object(job_id, ERROR_FILE)
            return JobErrorResponse(dict(message=message.read()))

        if not minio_object_exists(minio, job_id, DONE_FILE):
            return JobStatusResponse(dict(ready=False))

        output_path = OUTPUT_DIR + "/" + result_file
        if not minio_object_exists(minio, job_id, output_path):
            return ErrorResponse(dict(message="Requested file does not exist"))

        mime = mimetypes.guess_type(result_file)[0] or "application/octet-stream"
        return FileResponse(minio.get_object(job_id, output_path), mimetype=mime)

    @api.get('/status')
    @swagger.autodoc()
    @swagger.responds_with(StatusResponse)
    async def get_status(request: Request):
        """Get status of all the sheep available."""
        response = StatusResponse()
        response.containers = dict(shepherd.get_status())
        return response

    return api
