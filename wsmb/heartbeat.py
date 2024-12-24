
import asyncio
from dataclasses import dataclass
from typing import Coroutine
import httpx
from httpx import Response, ConnectTimeout
from loguru import logger
from event import Event

from wsmb.ws_client import WebSocket


@dataclass
class ServerData:
    url: str
    # priority: int
    isOnline: bool = False
    connected: bool = False

    def __str__(self) -> str:
        return f'({self.url}, online: {self.isOnline}, con: {self.connected})'


async def check_connection(url: str) -> bool:
    try:
        async with httpx.AsyncClient() as client:
            response: Response = await client.get(url, timeout=3)
            if response.status_code != 200:
                logger.error(f'{url}: {response.reason_phrase}{response.text}')
                return False
        logger.success(f'{url}: OK')
        return True
    except ConnectTimeout as err:
        logger.error(f'Timeout error! {err}')
    except (httpx.ConnectError, httpx.ReadTimeout):
        ...
    return False


class HeartBeatWorker:
    def __init__(self, url_list: list[str]) -> None:
        # self.servers = sorted(servers, key=lambda x: x.priority, reverse=True)
        self.reconnected: Event = Event(str)
        self.servers: list[ServerData] = [ServerData(url) for url in url_list]
        self.period_sec = 60
        self.connect_retries = 3
        self._ws: WebSocket | None = None
        self._sleep_task: asyncio.Task | None = None

    async def routine(self) -> None:
        while True:
            try:
                await self._check_online()
                if self._is_main_ready():
                    if self._is_connected():
                        await self.disconnect(True)
                        await asyncio.sleep(1)
                if not self._is_connected():
                    await self._make_connection()
                if self._is_connected():
                    coro: Coroutine = asyncio.sleep(self.period_sec)
                    self._sleep_task = asyncio.create_task(coro)
                    await asyncio.sleep(0.001)
                    try:
                        await self._sleep_task
                    except asyncio.CancelledError:
                        ...
            except asyncio.CancelledError:
                await self.disconnect(True)
                return
            except Exception as err:
                logger.error(err)

    def _is_main_ready(self) -> bool:
        return self.servers[0].isOnline and not self.servers[0].connected

    def _is_connected(self) -> bool:
        ws_connected: bool = self._ws is not None and self._ws.connection_status
        return bool(sum([server.connected
                         for server in self.servers])) or ws_connected

    async def _check_online(self) -> None:
        if not self.servers[0].connected:  # not connected to main server
            task_list: list = [check_connection(server.url)
                               for server in self.servers]
            result: list[bool] = await asyncio.gather(*task_list)
            for server, r in zip(self.servers, result):
                server.isOnline = r
                logger.debug(f'{server}')

    async def _make_connection(self) -> None:
        for server in self.servers:
            if server.isOnline:
                self.reconnected.emit(server.url)
                connection_status: bool = await self.connect(server.url)
                server.connected = connection_status
                if connection_status:
                    break

    async def connect(self, uri: str) -> bool:
        raise NotImplementedError

    async def disconnect(self, manual: bool) -> None:
        raise NotImplementedError

    async def on_disconnect(self) -> None:
        if self._sleep_task:
            await self._check_online()
            if not self._is_main_ready():
                await asyncio.sleep(15)
            if self._ws and not self._ws.connection_status:
                for server in self.servers:
                    server.connected = False
                self._sleep_task.cancel()

    def set_ws(self, ws: WebSocket) -> None:
        self._ws = ws
        self.connect = ws.connect
        self.disconnect = ws.disconnect
        ws.connect_retries = self.connect_retries
        ws.disconnected.subscribe(self.on_disconnect)


async def test():
    ws = WebSocket('/ws/gs', {'label': 'TEST_GS'})
    worker = HeartBeatWorker([
        'http://localhost:33011',
        'http://localhost:33321',
        'http://84.237.52.8:33011',
    ])
    worker.period_sec = 15
    worker.reconnected.subscribe(lambda url: print(f'reconnected to {url}'))
    worker.set_ws(ws)
    await ws.connect('http://84.237.52.8:33011')
    await worker.routine()


if __name__ == '__main__':
    asyncio.run(test())
