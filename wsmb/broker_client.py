
import asyncio
from asyncio import Task
from enum import Enum
from functools import wraps, partial
import json
from typing import Any, Callable, Coroutine, get_type_hints
from uuid import UUID
from loguru import logger
from pydantic import BaseModel, ValidationError
from wsmb.router import Router
from wsmb.msg import Msg
from wsmb.ws_client import WebSocket
from pyvalidate import create_dyn_model
from pyvalidate.validator import args_to_kwargs


HANDLER = Callable[..., Coroutine | Any]


class BrokerClient:
    def __init__(self) -> None:
        self.ws: WebSocket | None = None
        self.endpoints: dict[str, list[HANDLER]] = {}
        self.name: str = ''
        self._waiting_tasks: dict[UUID, asyncio.Task] = {}
        self._tasks_result: dict[UUID, Any] = {}
        self.on_exception: Callable[[str, str], None] | None = None

    def include_router(self, router: Router, *args) -> None:
        if len(args) > 0:
            new_endpoints: dict = {}
            for ep, handlers in router.endpoints.items():
                new_handlers: list = [partial(handler, *args)
                                      for handler in handlers]
                new_endpoints.update({ep: new_handlers})
            router.endpoints = new_endpoints
        self.endpoints.update(router.endpoints)

    def set_ws(self, ws: WebSocket) -> None:
        self.ws = ws
        self.ws.on_received = self.route

    def subscribe(self, event: str | Enum,
                  handler: HANDLER) -> None:
        if isinstance(event, Enum):
            event = event.name
        subscribers: list = self.endpoints.setdefault(event, [])
        subscribers.append(handler)

    def rpc(self, method: str | Enum) -> Callable:
        def _subscriber(func: HANDLER):
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
        if msg.dst != self.name and msg.dst != 'BROADCAST':
            logger.error(f'Got msg with incorrect destination '\
                         f'({msg.src}->{msg.dst} '\
                         f'(expected: {self.name})\n{msg}')
            return None
        if msg.is_answer:
            self._handle_answer(msg)
            return None
        handlers: list[HANDLER] = self.endpoints.get(msg.method, [])
        if len(handlers):
            for handler in handlers:
                try:
                    args_model: type[BaseModel] = create_dyn_model(handler)
                    if isinstance(msg.data, tuple):
                        msg.data = args_to_kwargs(handler, *msg.data)
                    json_str: str = json.dumps(msg.data)
                    args_dict = args_model.model_validate_json(json_str).__dict__
                    msg.data = args_dict
                    await self._exec_handler(handler, msg)
                except ValidationError as err:
                    details: str = err.json(include_url=False)
                    logger.error(details)
                    if msg.need_answer and self.on_exception:
                        self.on_exception('ValidationError', details)
                    return

        else:
            logger.warning(f'unsubscribed method: {data}')

    def _handle_answer(self, msg: Msg) -> None:
        answer: Task | None = self._waiting_tasks.get(msg.msg_id, None)
        if not answer:
            logger.warning('Got answer but it\'s not in waiting list. '\
                           'Check timeout')
            return
        answer.cancel()
        self._waiting_tasks.pop(msg.msg_id)
        if isinstance(msg.data, dict):
            self._tasks_result.update({msg.msg_id: msg.data})  # type: ignore
        else:
            logger.error('Incorrect answer format! Answer data must be dict')

    async def _exec_handler(self, handler: HANDLER, msg: Msg) -> None:
        if asyncio.iscoroutinefunction(handler):
            await self._execute_coroutine(handler, msg)
        else:
            answer_msg: Msg | None = self._execute_func(handler, msg)
            if answer_msg:
                await self._send(answer_msg.model_dump_json())

    def _execute_func(self, handler: HANDLER, msg: Msg) -> Msg | None:
        exception: str = ''
        exception_type: str = ''
        result: Any = None
        try:
            if isinstance(msg.data, dict):
                result = handler(**msg.data)
            else:
                result = handler(*msg.data)
        except Exception as err:
            logger.error(err)
            exception = str(err)
            exception_type = type(err).__name__
        if msg.need_answer:
            if exception:
                answer = msg.exception(exception_type, exception)
            else:
                answer: Msg = msg.answer(result)
            return answer

    async def _execute_coroutine(self, handler: HANDLER, msg: Msg) -> None:
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
            logger.error(f'Got exception for input msg: {msg}')
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

    async def broadcast(self, method: str | Enum, data: dict | tuple):
        return await self.publish('BROADCAST', method, data)

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
            details: str = result.get('details', None)
            result = result.get('answer', None)
            if exception:
                logger.error(f'Got exception for {method}:\n'\
                             f'{exception}: {details}')
                if self.on_exception:
                    self.on_exception(exception, details)
        return result
