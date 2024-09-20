from pydantic import ValidationError
from loguru import logger
from wsmb.msg import Msg


class Channel:
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.subscribers = set()

    def subscribe(self, ws) -> None:
        self.subscribers.add(ws)

    async def publish(self, msg: Msg) -> None:
        for s in self.subscribers:
            await s.send_text(msg.model_dump_json())


class BrokerServer:
    def __init__(self) -> None:
        self.channels: dict[str, Channel] = {}

    def subscribe(self, name: str, ws) -> None:
        ch: Channel | None = self.channels.get(name, None)
        if ch is not None:
            ch.subscribe(ws)
        else:
            ch = Channel(name)
            ch.subscribe(ws)
            self.channels.update({name: ch})


    def unsubscribe(self, ch_name: str, ws) -> None:
        ch: Channel | None = self.channels.get(ch_name, None)
        if ch is None:
            raise ValueError('Channel not exists!')

        ch.subscribers.discard(ws)
        if len(ch.subscribers) == 0:
            self.channels.pop(ch_name)

    async def route(self, data: str, ws) -> None:
        msg = await self._parse_msg(data, ws)
        if msg:
            publisher: Channel | None = self.channels.get(msg.src, None)
            if not publisher:
                raise ValueError('SRC not in channels')
            if ws not in publisher.subscribers:
                raise ValueError('Attempt to access of alien channel')
            destination: Channel | None = self.channels.get(msg.dst, None)
            if destination:
                await destination.publish(msg)
            else:
                ...

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
            result += f'{ch_name}:{len(ch.subscribers)}\n'
        return result