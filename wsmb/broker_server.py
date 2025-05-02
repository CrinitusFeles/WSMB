from enum import Enum, EnumType
from functools import wraps
from typing import Awaitable, Callable, Type  #, Coroutine
from pydantic import ValidationError
from loguru import logger
from wsmb.broker_client import BrokerClient
from wsmb.msg import Msg


class Channel:
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.publishers = set()
        self.subscribers = set()

    def pub_count(self) -> int:
        return len(self.publishers)

    def sub_count(self) -> int:
        return len(self.subscribers)

    def add_publisher(self, ws) -> None:
        self.publishers.add(ws)

    def subscribe(self, ws) -> None:
        self.subscribers.add(ws)

    async def to_publisher(self, msg: Msg) -> None:
        for p in self.publishers:
            await p.send_text(msg.model_dump_json())

    async def publish(self, msg: Msg) -> None:
        for s in self.subscribers:
            await s.send_text(msg.model_dump_json())


INSPECTOR = Callable[..., Awaitable[tuple[bool, Msg]]]


class BrokerServer:
    def __init__(self) -> None:
        self.channels: dict[str, Channel] = {}
        self.client: BrokerClient | None = None
        self._inspect_methods: dict[str, INSPECTOR] = {}

    def _inspect(self, endpoint: str | Enum | Type[Enum],
                 handler: INSPECTOR) -> None:
        if isinstance(endpoint, EnumType):
            self._inspect_methods.update({ep.name: handler for ep in endpoint})
            return
        if isinstance(endpoint, Enum):
            endpoint = endpoint.name
        self._inspect_methods.update({endpoint: handler})

    def inspect(self, endpoint: str | Enum | Type[Enum]) -> Callable:
        def _subscriber(func: INSPECTOR):# -> _Wrapped[Callable[[Msg], Any], Awaitable[tuple[bool, Msg]...:
            self._inspect(endpoint, func)
            @wraps(func)
            def wrapper(msg: Msg):
                return func(msg)
            return wrapper
        return _subscriber

    def remove_ws(self, ws):
        for ch in self.channels.values():
            ch.subscribers.discard(ws)
            ch.publishers.discard(ws)

    def add_channel(self, name: str, ws) -> None:
        ch: Channel | None = self.channels.get(name, None)
        if ch is not None:
            ch.add_publisher(ws)
        else:
            ch = Channel(name)
            ch.add_publisher(ws)
            self.channels.update({name: ch})

    def subscribe(self, name: str, ws) -> None:
        if self.client and name == self.client:
            raise ValueError('Channal\'s name must not be the same as the '\
                             'server\'s client')
        ch: Channel | None = self.channels.get(name, None)
        if ch is not None:
            ch.subscribe(ws)
        else:
            logger.error(f'Channel {name} not exists')

    def unsubscribe(self, ch_name: str, ws) -> None:
        ch: Channel | None = self.channels.get(ch_name, None)
        if ch is None:
            raise ValueError('Channel not exists!')
        ch.subscribers.discard(ws)

    async def route(self, data: str, ws) -> None:
        msg: Msg | None = await self._parse_msg(data, ws)
        if not msg:
            return
        if self.client:
            if msg.dst == self.client.name:
                await self.client.route(data, ws)
                return
        sender: Channel | None = self.channels.get(msg.src, None)
        if not sender:
            answer: Msg = msg.exception('ValueError',
                                        'SRC not in channels')
            await ws.send_text(answer.model_dump_json())
            return
        if msg.dst == 'BROADCAST':
            await sender.publish(msg)
            return
        # if ws not in sender.subscribers:
        #     answer: Msg = msg.exception('ValueError',
        #                                 'Attempt to access for alien channel')
        #     await ws.send_text(answer.model_dump_json())
        #     return

        dst_ch: Channel | None = self.channels.get(msg.dst, None)
        if not msg.is_answer:
            inspector: INSPECTOR | None = self._inspect_methods.get(msg.method,
                                                                    None)
            if inspector:
                result, answer = await inspector(msg, ws)
                if not result:
                    logger.debug(f'Inspector deny msg: {msg}')
                    await ws.send_text(answer.model_dump_json())
                    return

        if not dst_ch:
            logger.error(f'Destination {msg.dst} not found\n{msg}')
            return
        if msg.is_answer:
            await dst_ch.publish(msg)
        else:
            if ws in dst_ch.subscribers:
                await dst_ch.to_publisher(msg)
            else:
                answer = msg.exception('ValueError',
                                        f'You not subscribed for channel {msg.dst}')
                await ws.send_text(answer.model_dump_json())


    async def _parse_msg(self, data: str, ws) -> Msg | None:
        try:
            return Msg.model_validate_json(data)
        except ValidationError as err:
            logger.warning(err.json(indent=4))
            logger.warning(f'incorrect msg = {data}')
            await ws.send_text(f'Incorrect Msg format: {data}')
            await ws.close(reason='Incorrect Msg format')
            return None

    def __str__(self) -> str:
        result = ''
        for ch_name, ch in self.channels.items():
            result += f'{ch_name}:\n\tpub: {ch.pub_count()}\n\t'\
                      f'sub: {ch.sub_count()}\n'
        return result

    def to_dict(self) -> dict:
        result: dict = {}
        for ch_name, ch in self.channels.items():
            result.update({ch_name: {'pub': ch.pub_count(),
                                     'sub': ch.sub_count()}})
        return result
