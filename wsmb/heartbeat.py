
import asyncio
from dataclasses import dataclass
import httpx
from httpx import Response, ConnectTimeout
from loguru import logger

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
        self.servers: list[ServerData] = [ServerData(url) for url in url_list]
        self.period_sec = 60
        self.connect_retries = 5
        self.ws_endpoint: str = '/ws'

    async def routine(self) -> None:
        try:
            while True:
                await self._check_online()
                if self._is_main_ready():
                    if self._is_connected():
                        await self.disconnect(True)
                        await asyncio.sleep(1)
                if not self._is_connected():
                    await self._make_connection()
                await asyncio.sleep(self.period_sec)
        except asyncio.CancelledError:
            await self.disconnect(True)

    def _is_main_ready(self) -> bool:
        return self.servers[0].isOnline and not self.servers[0].connected

    def _is_connected(self) -> int:
        return sum([server.connected for server in self.servers])

    async def _check_online(self) -> None:
        if not self.servers[0].connected:  # not connected to main server
            task_list: list = [self._check(url) for url in self.servers]
            result: list[bool] = await asyncio.gather(*task_list)
            for server, r in zip(self.servers, result):
                server.isOnline = r
                logger.debug(f'{server}')

    async def _make_connection(self) -> None:
        for server in self.servers:
            if server.isOnline:
                connection_status: bool = await self._connect(server.url)
                server.connected = connection_status
                if connection_status:
                    break

    async def _connect(self, url: str) -> bool:
        uri: str = url.replace('https', 'wss')\
                      .replace('http', 'ws') + self.ws_endpoint
        return await self.connect(uri, None)

    async def connect(self, uri: str, headers: dict | None) -> bool:
        raise NotImplementedError

    async def disconnect(self, manual: bool) -> None:
        raise NotImplementedError

    async def _check(self, url: ServerData) -> bool:
        return await check_connection(url.url)

    async def on_disconnect(self) -> None:
        for server in self.servers:
            server.connected = False

    def set_ws(self, ws: WebSocket, endpoint: str = '') -> None:
        self.connect = ws.connect
        self.disconnect = ws.disconnect
        ws.connect_retries = self.connect_retries
        ws.disconnected.subscribe(worker.on_disconnect)
        if endpoint:
            self.ws_endpoint = endpoint


if __name__ == '__main__':
    ws = WebSocket()
    worker = HeartBeatWorker([
        'http://localhost:33300',
        'http://localhost:33321',
        'http://84.237.52.8:33011',
    ])
    worker.set_ws(ws, '/ws/gs')
    asyncio.run(worker.routine())
