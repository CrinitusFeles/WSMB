
import asyncio
from asyncio import Task
from enum import Enum
from functools import wraps, partial
import json
from typing import Any, Callable
from uuid import UUID
from loguru import logger
from pydantic import BaseModel, ValidationError
from wsmb.router import HANDLER, Endpoint, Router
from wsmb.msg import Msg
from wsmb.ws_client import WebSocket
from pyvalidate import create_dyn_model
from pyvalidate.validator import args_to_kwargs


def execute_func(handler: HANDLER, msg: Msg) -> Msg | None:
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


async def execute_coroutine(handler: HANDLER, msg: Msg):
    if isinstance(msg.data, dict):
        task: Task = asyncio.create_task(handler(**msg.data),
                                         name=msg.model_dump_json())
    else:
        task = asyncio.create_task(handler(*msg.data),
                                   name=msg.model_dump_json())
    return task


class BrokerClient:
    def __init__(self) -> None:
        self.ws: WebSocket | None = None
        self.endpoints: dict[str, list[Endpoint]] = {}
        self.postrouters: dict[str, HANDLER] = {}
        self.name: str = ''
        self._waiting_tasks: dict[UUID, asyncio.Task] = {}
        self._tasks_result: dict[UUID, Any] = {}
        self.on_exception: Callable[[str, str], None] | None = None

    def include_router(self, router: Router, *args) -> None:
        if len(args) > 0:
            new_endpoints: dict = {}
            for ep_name, endpoints in router.endpoints.items():
                new_handlers: list = [Endpoint(ep_name,
                                               partial(ep.handler, *args),
                                               ep.postroute)
                                      for ep in endpoints]
                new_endpoints.update({ep_name: new_handlers})
            router.endpoints = new_endpoints
        self.endpoints.update(router.endpoints)

    def set_ws(self, ws: WebSocket) -> None:
        self.ws = ws
        self.ws.on_received = self.route

    def _postroute(self, event: str | Enum, handler: HANDLER):
        if isinstance(event, Enum):
            event = event.name
        self.postrouters.update({event: handler})

    def postroute(self, method: str | Enum) -> Callable:
        def _subscriber(func: HANDLER):
            self._postroute(method, func)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return _subscriber

    def subscribe(self, event: str | Enum, handler: HANDLER,
                  postrouter: Callable | None = None) -> None:
        if isinstance(event, Enum):
            event = event.name
        endpoints: list[Endpoint] = self.endpoints.setdefault(event, [])
        endpoints.append(Endpoint(event, handler, postrouter))

    def rpc(self, method: str | Enum,
            postrouter: Callable | None = None) -> Callable:
        def _subscriber(func: HANDLER):
            self.subscribe(method, func, postrouter)
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
        endpoints: list[Endpoint] = self.endpoints.get(msg.method, [])
        if len(endpoints):
            for endpoint in endpoints:
                await self._validate_exec(endpoint, msg)
        else:
            logger.warning(f'unsubscribed method: {data}')

    async def _validate_exec(self, endpoint: Endpoint, msg: Msg):
        try:
            args_model: type[BaseModel] = create_dyn_model(endpoint.handler)
            if isinstance(msg.data, tuple):
                msg.data = args_to_kwargs(endpoint.handler, *msg.data)
            json_str: str = json.dumps(msg.data)
            args_dict: dict = args_model.model_validate_json(json_str).__dict__
            msg.data = args_dict
            return await self._exec_handler(endpoint, msg)
        except ValidationError as err:
            details: str = err.json(include_url=False)
            logger.error(details)
            if msg.need_answer and self.on_exception:
                self.on_exception('ValidationError', details)

    def _handle_answer(self, msg: Msg) -> None:
        answer: Task | None = self._waiting_tasks.get(msg.msg_id, None)
        if not answer:
            logger.warning('Got answer but it\'s not in waiting list. '\
                           'Check timeout')
            return
        answer.cancel()
        self._waiting_tasks.pop(msg.msg_id)
        if isinstance(msg.data, dict):
            self._tasks_result.update({msg.msg_id: msg.data})
        else:
            logger.error('Incorrect answer format! Answer data must be dict')

    async def _exec_handler(self, endpoint: Endpoint, msg: Msg):
        if asyncio.iscoroutinefunction(endpoint.handler):
            task: Task = await execute_coroutine(endpoint.handler, msg)
            if msg.need_answer:
                task.add_done_callback(self._task_done)
                # await task
                # answer_msg = self._task_done(task)
        else:
            answer_msg: Msg | None = execute_func(endpoint.handler, msg)
            if answer_msg:
                await self._send(answer_msg.model_dump_json())
                postroute = self.postrouters.get(answer_msg.method, None)
                if postroute:
                    postroute(answer_msg)

    async def _send(self, data: str) -> None:
        if not self.ws:
            raise RuntimeError(f'Trying to send next data: {data}\n'\
                               f' but WS Client not defined')
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
        postroute = self.postrouters.get(answer.method)
        if postroute:
            postroute(answer)

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
