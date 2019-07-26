import time

from molotov import events, global_teardown

from tests.stress.loadtest import _SLEEP

_TIMES = {}
_ID = 0


@events()
async def record_time(event, **info):
    global _ID
    if event == 'scenario_start':
        scenario = info['scenario']
        index = (info['wid'], scenario['name'])
        _TIMES[index] = time.time()
    if event == 'scenario_success':
        scenario = info['scenario']
        index = (info['wid'], scenario['name'])
        start_time = _TIMES.pop(index, None)
        duration = time.time() - start_time - _SLEEP
        _TIMES[scenario['name'] + '_' + str(_ID)] = duration
        _ID += 1


@global_teardown()
def print_times():
    for scenario, time in _TIMES.items():
        print(f'Scenario `{scenario}` took {time}s to process.')
