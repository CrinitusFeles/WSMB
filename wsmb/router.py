from enum import Enum
from functools import wraps
from typing import Any, Callable, Coroutine


class Router:
    def __init__(self) -> None:
        self.endpoints: dict = {}

    def rpc(self, method):
        def _subscriber(func: Callable[..., Coroutine | Any]):
            self._subscribe(method, func)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return _subscriber

    def _subscribe(self, event: str | Enum,
                  handler: Callable[..., Coroutine]) -> None:
        if isinstance(event, Enum):
            event = event.name
        subscribers: list = self.endpoints.setdefault(event, [])
        subscribers.append(handler)

    def subscribe(self, event: str | Enum):
        def _subscriber(func: Callable[..., Coroutine | Any]):
            self._subscribe(event, func)
            @wraps(func)
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            return wrapper
        return _subscriber
