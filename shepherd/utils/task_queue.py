import asyncio


class TaskQueue:
    """
    A helper for running asyncio tasks in the background with a limit on the number of tasks running in parallel.
    """

    def __init__(self, worker_count: int = 1):
        self._queue = asyncio.Queue()
        self._workers = tuple(asyncio.create_task(self._consume_tasks()) for _ in range(worker_count))

    async def _consume_tasks(self) -> None:
        """
        Take tasks from the queue and process them.
        """
        future_result: asyncio.Future

        while True:
            task, future_result = await self._queue.get()

            try:
                result = await task
                future_result.set_result(result)
            except Exception as ex:
                future_result.set_exception(ex)

            self._queue.task_done()

    # TODO no suitable type definition for awaitable exists yet
    async def enqueue_task(self, awaitable) -> asyncio.Future:
        """
        Enqueue a task and return a future that resolves on its completion.

        :param awaitable: an awaitable object to be enqueued
        :return: a future object that will contain the result of the task
        """
        future_result = asyncio.get_event_loop().create_future()
        await self._queue.put((awaitable, future_result))
        return future_result

    async def close(self) -> None:
        """
        Wait for all tasks to complete and terminate the workers.
        """
        await self._queue.join()

        for worker in self._workers:
            worker.cancel()
