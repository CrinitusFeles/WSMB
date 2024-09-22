
import asyncio
from asyncio import Task
from enum import Enum
from functools import wraps
from typing import Any, Callable, Coroutine
from uuid import UUID
from loguru import logger
from pydantic import ValidationError
from wsmb.router import Router
from wsmb.msg import Msg
from wsmb.ws_client import WebSocket


class BrokerClient:
    def __init__(self) -> None:
        self.ws: WebSocket | None = None
        self.endpoints: dict[str, list[Callable[..., Coroutine]]] = {}
        self.name: str = ''
        self._waiting_tasks: dict[UUID, asyncio.Task] = {}
        self._tasks_result: dict[UUID, Any] = {}

    def set_ws(self, ws: WebSocket) -> None:
        self.ws = ws
        self.ws.on_received = self.route

    def include_router(self, router: Router) -> None:
        self.endpoints.update(router.endpoints)

    def subscribe(self, event: str | Enum,
                  handler: Callable[..., Coroutine]) -> None:
        if isinstance(event, Enum):
            event = event.name
        subscribers: list = self.endpoints.setdefault(event, [])
        subscribers.append(handler)

    def rpc(self, method: str | Enum) -> Callable:
        """decorator to `subscribe` method

        Args:
            method (str | Enum): endpoint string
        """
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

        if self._handle_answer(msg):
            return None
        handlers: list[Callable[..., Coroutine | Any]] = self.endpoints.get(msg.method, [])
        if len(handlers):
            for handler in handlers:
                await self._exec_handler(handler, msg)
        else:
            logger.warning(f'unsubscribed method: {data}')

    def _handle_answer(self, msg: Msg) -> bool:
        answer: Task | None = self._waiting_tasks.get(msg.msg_id, None)
        if answer:
            if msg.dst == self.name:
                answer.cancel()
                self._waiting_tasks.pop(msg.msg_id)
                if isinstance(msg.data, dict):
                    self._tasks_result.update({msg.msg_id: msg.data})  # type: ignore
                else:
                    logger.error('Incorrect answer format! Answer data must be dict')
                return True
            else:
                logger.error(f'Got msg with incorrect destination '\
                            f'({msg.src}->{msg.dst} (expected: {self.name}))!')
        return False

    async def _exec_handler(self, handler: Callable[..., Coroutine], msg: Msg):
        if asyncio.iscoroutinefunction(handler):
            await self._execute_coroutine(handler, msg)
        else:
            answer_msg: Msg | None = self._execute_func(handler, msg)
            if answer_msg:
                await self._send(answer_msg.model_dump_json())

    def _execute_func(self, handler: Callable, msg: Msg) -> Msg | None:
        exception: str = ''
        exception_type: str = ''
        try:
            if isinstance(msg.data, dict):
                result: Any = handler(**msg.data)
            else:
                result = handler(*msg.data)
        except Exception as err:
            logger.error(err)
            exception = str(err)
            exception_type = str(type(err))
        if msg.need_answer:
            if exception:
                answer = msg.exception(exception_type, exception)
            else:
                answer: Msg = msg.answer(result)
            return answer

    async def _execute_coroutine(self, handler: Callable[..., Coroutine],
                                 msg: Msg) -> None:
        if isinstance(msg.data, dict):
            task: Task = asyncio.create_task(handler(**msg.data),
                                             name=msg.model_dump_json())
        else:
            task = asyncio.create_task(handler(*msg.data),
                                        name=msg.model_dump_json())
        if msg.need_answer:
            task.add_done_callback(self._task_done)

    async def _send(self, data: str) -> None:
        if not self.ws:
            raise RuntimeError('WS Client not defined')
        await self.ws.send_text(data)

    def _task_done(self, task: Task) -> None:
        msg: Msg = Msg.model_validate_json(task.get_name())
        exception: BaseException | None = task.exception()
        if task.cancelled():
            answer = msg.exception('CancelledError', 'Task was cancelled')
        elif exception:
            answer = msg.exception(type(exception).__name__, str(exception))
        else:
            result: Any = task.result()
            answer: Msg = msg.answer(result)
        asyncio.create_task(self._send(answer.model_dump_json()))

    async def publish(self, dst: str, method: str | Enum, data: dict | tuple,
                      need_answer: bool = False) -> Msg:
        if isinstance(method, Enum):
            method = method.name
        msg = Msg(src=self.name, dst=dst, method=method, data=data,
                  need_answer=need_answer)
        await self._send(msg.model_dump_json())
        return msg

    async def _wait_message(self, msg_id: UUID, timeout: float) -> bool:
        task: Task = asyncio.create_task(asyncio.sleep(timeout))
        self._waiting_tasks.update({msg_id: task})
        try:
            await task
            self._waiting_tasks.pop(msg_id)
            return False
        except asyncio.CancelledError:
            return True

    async def call(self, dst: str, method: str | Enum, data: dict | tuple = {},
                   timeout: float = 3) -> Any | None:
        msg: Msg = await self.publish(dst, method, data, True)
        if not await self._wait_message(msg.msg_id, timeout):
            logger.error(f'{method} TIMEOUT ({timeout} sec)')
            return None
        result: dict | None = self._tasks_result.get(msg.msg_id, None)
        self._tasks_result.pop(msg.msg_id)
        if result:
            exception: str = result.get('exception', None)
            detailes: str = result.get('details', None)
            result = result.get('answer', None)
            if exception:
                logger.error(f'Got exception for {method}:\n'\
                             f'{exception}: {detailes}')
                return None
        return result
