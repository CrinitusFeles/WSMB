from dataclasses import dataclass
from enum import Enum
from functools import partial, wraps
from typing import Any, Callable, Coroutine, Type


HANDLER = Callable[..., Coroutine | Any] | partial


@dataclass
class Endpoint:
    name: str
    handler: HANDLER
    postroute: Callable | None = None

class Router:
    def __init__(self) -> None:
        self.endpoints: dict[str, list[Endpoint]] = {}

    def rpc(self, method: str | Enum, postrouter: Callable | None = None) -> Callable:
        def _subscriber(func: HANDLER):
            self._subscribe(method, func, postrouter)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return _subscriber

    def _subscribe(self, event: str | Enum, handler: HANDLER,
                   postrouter: Callable | None = None) -> None:
        if isinstance(event, Enum):
            event = event.name
        endpoints: list[Endpoint] = self.endpoints.setdefault(event, [])
        endpoints.append(Endpoint(event, handler, postrouter))

    def subscribe(self, event: str | Enum,
                  postrouter: Callable | None = None):
        def _subscriber(func: Callable[..., Coroutine | Any]):
            self._subscribe(event, func, postrouter)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return _subscriber
