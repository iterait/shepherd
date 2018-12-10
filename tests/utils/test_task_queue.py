import asyncio
import pytest

from shepherd.utils.task_queue import TaskQueue


async def sleep_coro(time: float, result: int):
    await asyncio.sleep(time)
    return result


async def test_task_queue_sequential(loop):
    q = TaskQueue(1)

    f1: asyncio.Future = await q.enqueue_task(sleep_coro(0.5, 1))
    f2: asyncio.Future = await q.enqueue_task(sleep_coro(0.1, 2))

    result_1 = await f1
    assert result_1 == 1

    assert not f2.done()

    result_2 = await f2
    assert result_2 == 2

    await q.close()


async def test_task_queue_parallel(loop):
    q = TaskQueue(2)

    f1: asyncio.Future = await q.enqueue_task(sleep_coro(0.5, 1))
    f2: asyncio.Future = await q.enqueue_task(sleep_coro(0.1, 2))

    result_1 = await f1
    assert result_1 == 1

    assert f2.done()

    result_2 = await f2
    assert result_2 == 2

    await q.close()


async def exception_coro():
    raise ValueError("Boom!")


async def test_task_queue_exception(loop):
    q = TaskQueue(1)

    f1: asyncio.Future = await q.enqueue_task(exception_coro())

    with pytest.raises(ValueError):
        await f1

    await q.close()
