

import asyncio
from functools import wraps
from typing import Any, Callable, Coroutine  #, get_type_hints


class Router:
    def __init__(self) -> None:
        self.endpoints: dict = {}

    def rpc(self, method):
        def _subscriber(func: Callable[..., Coroutine | Any]):
            subscribers: list = self.endpoints.setdefault(method, [])
            subscribers.append(func)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return _subscriber

    def route(self, method: str, *args, **kwargs):
        for callback in self.endpoints.get(method, []):
            # hints = get_type_hints(callback)
            # print(hints)
            # for arg, hint in zip(args, hints.values()):
            #     if not isinstance(arg, hint):
            #         raise TypeError(f'Got {type(arg)} but expected {hint}')
            if asyncio.iscoroutinefunction(callback):
                loop = asyncio.get_event_loop()
                task = loop.create_task(callback(*args, **kwargs))
                task.add_done_callback(self.done_callback)
            else:
                callback(*args, **kwargs)

    def done_callback(self, task: asyncio.Task):
        print('task done:', task)
