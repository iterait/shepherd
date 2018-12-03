from aiohttp import web
from aiohttp.web_request import Request
from apistrap.types import FileResponse
from io import BytesIO

import mimetypes

from ..storage import Storage
from ..constants import DONE_FILE, ERROR_FILE, DEFAULT_OUTPUT_FILE, OUTPUT_DIR, DEFAULT_PAYLOAD_PATH
from ..shepherd import Shepherd
from .requests import StartJobRequest
from .responses import StartJobResponse, StatusResponse, JobStatusResponse, ErrorResponse, \
    JobErrorResponse, JobReadyResponse
from ..errors.api import ClientActionError, UnknownJobError
from .swagger import swagger


async def check_job_exists(storage: Storage, job_id: str):
    if not await storage.job_data_exists(job_id):
        raise ClientActionError('Data for job `{}` does not exist'.format(job_id))


def create_shepherd_routes(shepherd: Shepherd, storage: Storage):
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
            await check_job_exists(storage, start_job_request.job_id)
        else:
            await storage.init_job(start_job_request.job_id)

            payload_data = start_job_request.payload.encode()
            payload = BytesIO(payload_data)
            await storage.put_file(start_job_request.job_id, DEFAULT_PAYLOAD_PATH,
                                   payload, len(start_job_request.payload))

        try:
            await shepherd.is_job_done(start_job_request.job_id)
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

        await check_job_exists(storage, job_id)

        ready = await shepherd.is_job_done(job_id)
        formatted_timestamp = await storage.get_timestamp(job_id, DONE_FILE) if ready else None

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

        await check_job_exists(storage, job_id)
        async with shepherd.job_done_condition:
            while not await shepherd.is_job_done(job_id):
                await shepherd.job_done_condition.wait()

        return JobStatusResponse({'ready': await shepherd.is_job_done(job_id)})

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

        await check_job_exists(storage, job_id)

        if not await storage.is_job_done(job_id):
            return JobStatusResponse(dict(ready=False))

        error = await storage.get_file(job_id, ERROR_FILE)
        if error is not None:
            return JobErrorResponse(dict(message=error.read()))

        output_path = OUTPUT_DIR + "/" + result_file
        output = await storage.get_file(job_id, output_path)
        if output is None:
            return ErrorResponse(dict(message="Requested file does not exist"))

        mime = mimetypes.guess_type(result_file)[0] or "application/octet-stream"
        return FileResponse(output, mimetype=mime)

    @api.get('/status')
    @swagger.autodoc()
    @swagger.responds_with(StatusResponse)
    async def get_status(request: Request):
        """Get status of all the sheep available."""
        response = StatusResponse()
        response.containers = dict(shepherd.get_status())
        return response

    return api
