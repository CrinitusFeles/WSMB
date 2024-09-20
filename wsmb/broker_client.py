
import asyncio
from asyncio import Task
from functools import wraps
from typing import Callable, Coroutine, Hashable  # , get_type_hints
from uuid import UUID
from loguru import logger
from pydantic import ValidationError
from wsmb.router import Router
from wsmb.msg import Msg
from wsmb.ws_client import WebSocket


class BrokerClient:
    def __init__(self, client: WebSocket) -> None:
        self.ws: WebSocket = client
        self.endpoints: dict[Hashable, list[Callable[..., Coroutine]]] = {}
        self.name: str = ''
        self._waiting_tasks: dict[UUID, asyncio.Task] = {}
        self._tasks_result: dict[UUID, dict] = {}
        self.ws.on_received = self.route

    def include_router(self, router: Router) -> None:
        self.endpoints.update(router.endpoints)

    def subscribe(self, event, handler: Callable[..., Coroutine]):
        subscribers: list = self.endpoints.setdefault(event, [])
        subscribers.append(handler)

    def rpc(self, method):
        def _subscriber(func: Callable[..., Coroutine]):
            self.subscribe(method, func)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return _subscriber

    async def route(self, data: str) -> None:
        try:
            msg: Msg = Msg.model_validate_json(data)
        except ValidationError:
            logger.error(f'got incorrect message: {data}')
            return None

        answer: Task | None = self._waiting_tasks.get(msg.message_id, None)
        if answer:
            answer.cancel()
            self._tasks_result.update({msg.message_id: msg.data})

        msg_data = msg.data
        method = msg_data.get('method', '')
        handlers: list[Callable[..., Coroutine]] = self.endpoints.get(method, [])
        if len(handlers):
            for handler in handlers:
                # hints = get_type_hints(handler)
                # print(hints)
                asyncio.create_task(handler(msg))
        else:
            logger.warning(f'unsubscribed method: {data}')

    async def publish(self, dst: str, data: dict) -> Msg:
        msg = Msg(src=self.name, dst=dst, data=data)
        await self.ws.send(msg.model_dump_json())
        return msg

    async def _wait_message(self, msg_id: UUID, timeout: float) -> bool:
        task: Task = asyncio.create_task(asyncio.sleep(timeout))
        self._waiting_tasks.update({msg_id: task})
        try:
            await task
            logger.error('TIMEOUT')
            return False
        except asyncio.CancelledError:
            return True

    async def call(self, dst: str, data: dict,
                          timeout: float = 3) -> dict | None:
        msg: Msg = await self.publish(dst, data)
        if not await self._wait_message(msg.message_id, timeout):
            return None
        result: dict | None = self._tasks_result.get(msg.message_id, None)
        self._tasks_result.pop(msg.message_id)
        if not result:
            logger.error('got empty result')
        return result

