

import asyncio
from functools import wraps
from typing import Any, Callable, Coroutine  #, get_type_hints


class Router:
    def __init__(self) -> None:
        self.endpoints: dict = {}
        self._parent = None

    def rpc(self, method):
        def _subscriber(func: Callable[..., Coroutine | Any]):
            subscribers: list = self.endpoints.setdefault(method, [])
            subscribers.append(func)
            @wraps(func)
            def wrapper(*args, **kwargs):
                if self._parent:
                    return func(self._parent, *args, **kwargs)
                return func(*args, **kwargs)
            return wrapper
        return _subscriber

    def route(self, method: str, *args, **kwargs):
        for callback in self.endpoints.get(method, []):
            if asyncio.iscoroutinefunction(callback):
                loop = asyncio.get_event_loop()
                if self._parent:
                    task = loop.create_task(callback(self._parent, *args, **kwargs))
                else:
                    task = loop.create_task(callback(*args, **kwargs))
                task.add_done_callback(self.done_callback)
            else:
                if self._parent:
                    callback(self._parent, *args, **kwargs)
                else:
                    callback(*args, **kwargs)

    def done_callback(self, task: asyncio.Task):
        print('task done:', task)

    def set_parent(self, parent):
        self._parent = parent



router2 = Router()
class FooBar:
    router = Router()
    def __init__(self) -> None:
        self.router.set_parent(self)
        ...

    @router.rpc('test')
    async def foo(self, var: int):
        await asyncio.sleep(0.02)
        print(var)

    @router.rpc('test2')
    def foo2(self, val: str):
        # await asyncio.sleep(0.02)
        print(val)


async def main():
    v = FooBar()
    got_msg = {'method': 'test', 'data': {'var': 1}}
    v.router.route(got_msg['method'], **got_msg['data'])
    got_msg = {'method': 'test2', 'data': {'val': 's'}}
    v.router.route(got_msg['method'], **got_msg['data'])
    await asyncio.sleep(0.1)


if __name__ == '__main__':
    asyncio.run(main())