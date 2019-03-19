from aiohttp import web
from aiohttp.web_request import Request
from apistrap.types import FileResponse
from io import BytesIO

import mimetypes

from ..storage import Storage
from ..constants import DEFAULT_OUTPUT_FILE, OUTPUT_DIR, DEFAULT_PAYLOAD_PATH, DEFAULT_PAYLOAD_FILE
from ..api.models import JobStatus
from ..shepherd import Shepherd
from .requests import StartJobRequest
from .responses import StartJobResponse, StatusResponse, JobStatusResponse, ErrorResponse, \
    JobErrorResponse, JobNotReadyResponse
from ..errors.api import UnknownJobError
from .swagger import swagger


async def check_job_dir_exists(storage: Storage, job_id: str) -> None:
    """
    Check if a job dir/bucket exists and raise an error if it doesn't.

    :param storage: a storage adapter to be checked
    :param job_id: an identifier of the job
    """
    if not await storage.job_dir_exists(job_id):
        raise UnknownJobError('Data for job `{}` does not exist'.format(job_id))


def create_shepherd_routes(shepherd: Shepherd, storage: Storage) -> web.RouteTableDef:
    """
    Create shepherd API endpoint handlers.

    :param shepherd: the shepherd exposed by the API
    :param storage: the storage used by the shepherd
    :return: a route table containing the API endpoint handlers
    """

    api = web.RouteTableDef()

    @api.post('/start-job')
    @swagger.autodoc()
    @swagger.accepts(StartJobRequest)
    @swagger.responds_with(StartJobResponse)
    async def start_job(request: Request, start_job_request: StartJobRequest):
        """
        Start a new job.

        :raises NameConflictError: a job with given id was already submitted
        """
        if not start_job_request.payload:
            await check_job_dir_exists(storage, start_job_request.job_id)
        else:
            await storage.init_job(start_job_request.job_id)

            payload_data = start_job_request.payload.encode()
            payload = BytesIO(payload_data)
            await storage.put_file(start_job_request.job_id, DEFAULT_PAYLOAD_PATH,
                                   payload, len(start_job_request.payload))

        await shepherd.enqueue_job(start_job_request.job_id, start_job_request.model, start_job_request.sheep_id)

        return StartJobResponse()

    @api.get("/jobs/{job_id}/status")
    @swagger.autodoc()
    @swagger.responds_with(JobStatusResponse)
    async def get_job_status(request: Request):
        """
        Get status information for a job.

        :param job_id: An identifier of the queried job
        """
        job_id = request.match_info['job_id']

        status = shepherd.get_job_status(job_id)
        if status is not None:
            return status

        status = await storage.get_job_status(job_id)

        return status

    @api.get("/jobs/{job_id}/wait_ready")
    @swagger.autodoc()
    @swagger.responds_with(JobStatusResponse)
    async def wait_ready(request: Request):
        """
        Wait until the specified job is ready.

        :param job_id: An identifier of the queried job
        """
        job_id = request.match_info['job_id']

        await check_job_dir_exists(storage, job_id)
        async with shepherd.job_done_condition:
            while not await shepherd.is_job_done(job_id):
                await shepherd.job_done_condition.wait()

        return await storage.get_job_status(job_id)

    @api.get("/jobs/{job_id}/result/{result_file}")
    @api.get("/jobs/{job_id}/result")
    @swagger.autodoc()
    @swagger.responds_with(JobNotReadyResponse, code=202)
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

        await check_job_dir_exists(storage, job_id)
        status = await storage.get_job_status(job_id)

        if status is not None and status.status == JobStatus.FAILED:
            return JobErrorResponse(dict(message=status.error_details.message))

        if status is None or status.status != JobStatus.DONE:
            return JobNotReadyResponse()

        output_path = OUTPUT_DIR + "/" + result_file
        output = await storage.get_file(job_id, output_path)
        if output is None:
            return ErrorResponse(dict(message="Requested file does not exist"))

        mime = mimetypes.guess_type(result_file)[0] or "application/octet-stream"
        return FileResponse(output, mimetype=mime)

    @api.get("/jobs/{job_id}/input/{input_file}")
    @api.get("/jobs/{job_id}/input")
    @swagger.autodoc()
    @swagger.responds_with(ErrorResponse, code=404)
    @swagger.responds_with(FileResponse, code=200)
    async def get_job_input(request: Request):
        """
        Get the input of the specified job.

        :param job_id: An identifier of the job
        :param result_file: Name of the requested file
        """
        job_id = request.match_info['job_id']
        result_file = request.match_info.get('input_file', DEFAULT_PAYLOAD_FILE)

        await check_job_dir_exists(storage, job_id)

        input_path = OUTPUT_DIR + "/" + result_file
        input = await storage.get_file(job_id, input_path)
        if input is None:
            return ErrorResponse(dict(message="Requested file does not exist"))

        mime = mimetypes.guess_type(result_file)[0] or "application/octet-stream"
        return FileResponse(input, mimetype=mime)

    @api.get('/status')
    @swagger.autodoc()
    @swagger.responds_with(StatusResponse)
    async def get_status(request: Request):
        """Get status of all the sheep available."""
        response = StatusResponse()
        response.containers = dict(shepherd.get_status())
        return response

    return api
