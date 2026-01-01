import asyncio
from functools import partial
import socket
from typing import Awaitable, Callable
from loguru import logger
import websockets
from websockets.asyncio.async_timeout import timeout
from websockets.legacy.exceptions import RedirectHandshake
from websockets.legacy.client import WebSocketClientProtocol
from websockets.uri import parse_uri, WebSocketURI
from websockets.extensions.permessage_deflate import ClientPerMessageDeflateFactory
import urllib.parse
from event import Event
from asyncio import Lock

compress = ClientPerMessageDeflateFactory(compress_settings={"memLevel": 5})
task_lock = Lock()

class AuthorizationError(Exception):
    ...


class WebSocket:
    def __init__(self, endpoint: str = '', headers: dict | None = None) -> None:
        self.received: Event = Event(str | bytes)
        self.connected: Event = Event()
        self.disconnected: Event = Event()
        self.error: Event = Event(Exception)
        self.critical_error: Event = Event()
        self.on_token_error: Callable[..., Awaitable] | None = None
        self._last_connection_data: str = ''
        self.read_task: asyncio.Task | None = None
        self.connection_status: bool = False
        self._uri: str = ''
        self.connect_retries = 9999999
        self.token: str = ''
        self.endpoint: str = endpoint
        self.headers: dict = headers or {}

    def handle_redirect(self, uri: str) -> None:
        # Update the state of this instance to connect to a new URI.
        old_uri = self._uri
        old_wsuri = self._wsuri
        new_uri = urllib.parse.urljoin(old_uri, uri)
        new_wsuri = parse_uri(new_uri)

        # Forbid TLS downgrade.
        if old_wsuri.secure and not new_wsuri.secure:
            raise websockets.SecurityError("redirect from WSS to WS")

        same_origin = (old_wsuri.host == new_wsuri.host and
                             old_wsuri.port == new_wsuri.port)

        # Rewrite the host and port arguments for cross-origin redirects.
        # This preserves connection overrides with the host and port
        # arguments if the redirect points to the same host and port.
        if not same_origin:
            # Replace the host and port argument passed to the protocol factory.
            factory = self._create_connection.args[0]
            factory = partial(
                factory.func,
                *factory.args,
                **dict(factory.keywords, host=new_wsuri.host,
                       port=new_wsuri.port),
            )
            # Replace the host and port argument passed to create_connection.
            self._create_connection = partial(
                self._create_connection.func,
                *(factory, new_wsuri.host, new_wsuri.port),
                **self._create_connection.keywords,
            )

        # Set the new WebSocket URI. This suffices for same-origin redirects.
        self._uri = new_uri
        self._wsuri = new_wsuri

    async def connect(self, uri: str) -> bool:
        counter: int = 0
        if not uri:
            uri = self._last_connection_data
        else:
            uri = uri.replace('https', 'wss').replace('http', 'ws') + self.endpoint
            if self.token:
                uri += f'?token={self.token}'
        if self.connection_status:
            logger.warning(f'WSClient already connected to '\
                           f'{self._last_connection_data[0]}')
            if self._last_connection_data[0] == uri:
                return True
            else:
                logger.warning('For connecting to another url use '\
                               '`disconnect` method first')
                return False
        while counter < self.connect_retries:
            counter += 1
            logger.debug(f'Trying to connect to {uri} (try {counter})')
            try:
                async with timeout(5):
                    if await self._connect(uri):
                        return True
            except TimeoutError:
                logger.error('Connection timeout')
                self.error.emit(TimeoutError('Connection timeout'))
            except (ConnectionRefusedError, ConnectionResetError, socket.gaierror) as err:
                logger.error(err)
                await asyncio.sleep(1)
            except AuthorizationError:
                logger.error('Authorization error!')
                return False
        logger.debug('Connect retries finished')
        return False

    async def _connect(self, uri: str) -> bool:
        self._uri: str = uri
        self._wsuri: WebSocketURI = parse_uri(self._uri)

        kwargs: dict = {}
        if self._wsuri.secure:
            kwargs.setdefault("ssl", True)
        if kwargs.get("ssl"):
            kwargs.setdefault("server_hostname", self._wsuri.host)
        self.factory = partial(
            WebSocketClientProtocol,
            extensions=[compress],
            ping_interval=6,
            ping_timeout=3,
            close_timeout=3,
            max_size=2**40,
            extra_headers=self.headers,
            host=self._wsuri.host,
            port=self._wsuri.port,
            secure=self._wsuri.secure,
            **kwargs
        )

        self._protocol: WebSocketClientProtocol
        self._loop = asyncio.get_event_loop()
        self._create_connection = partial(self._loop.create_connection,
                                          self.factory,
                                          host=self._wsuri.host,
                                          port=self._wsuri.port)
        _,  protocol = await self._create_connection()
        try:
            await protocol.handshake(
                self._wsuri,
                origin=protocol.origin,
                available_extensions=protocol.available_extensions,
                available_subprotocols=protocol.available_subprotocols,
                extra_headers=protocol.extra_headers,
            )
        except RedirectHandshake as exc:
            logger.error(f'Redirect handsheke error: {exc}')
            protocol.fail_connection()
            await protocol.wait_closed()
            self.handle_redirect(exc.uri)
        # Avoid leaking a connected socket when the handshake fails.
        except (Exception, asyncio.CancelledError) as err:
            logger.error(err)
            protocol.fail_connection()
            # await protocol.wait_closed()
            if hasattr(err, 'status_code'):
                if err.status_code == 403:  # type: ignore
                    if self.on_token_error:
                        try:
                            response = await self.on_token_error()
                            if response.status_code != 200:
                                logger.error('Can not refresh token')
                            if response.status_code == 403:
                                return False
                        except TimeoutError:
                            logger.error('Timeout of refresh token request')
                        self.critical_error.emit()
                        raise AuthorizationError
            if isinstance(err, Exception):
                self.error.emit(err)
            return False

        self._protocol = protocol
        logger.success(f'Connected to {self._uri}')
        self.read_task = self._loop.create_task(self._read_routine())
        self.read_task.add_done_callback(self._read_done_callback)
        self.connected.emit()
        self._last_connection_data = uri
        self.connection_status = True
        return True

    def _read_done_callback(self, task: asyncio.Task):
        self.read_task = None
        asyncio.create_task(self._reconnect(), name='Reconnect')

    async def _reconnect(self):
        try:
            await self.reconnect()
        except AuthorizationError:
            ...

    async def disconnect(self, manual: bool = False) -> None:
        if self.connection_status:
            self.connection_status = False
            reason = ''
            if manual:
                reason = 'Manual disconnect'
            if self.read_task:
                if manual:
                    self.read_task.remove_done_callback(self._read_done_callback)
                self.read_task.cancel()
                self.read_task = None
            await self._protocol.close(reason=reason)
            self.disconnected.emit()
            logger.debug(f'Disconnected from {self._uri}')
        else:
            logger.warning('WS client was not connected')

    async def reconnect(self):
        if self.connection_status:
            async with task_lock:
                logger.debug('Trying to reconnect')
                if not self._last_connection_data:
                    logger.error('Connection was never established')
                    return
                await self.disconnect()
                await self.connect('')

    async def send(self, data: str | bytes) -> None:
        try:
            await self._protocol.send(data)
        except websockets.exceptions.ConnectionClosedError:
            self.connection_status = True
            await self.reconnect()
            await self._protocol.send(data)

    async def send_text(self, data: str) -> None:
        try:
            await self._protocol.send(data)
        except websockets.exceptions.ConnectionClosedError:
            self.connection_status = True
            await self.reconnect()
            await self._protocol.send(data)

    async def receive_text(self):
        return await self._protocol.recv()

    async def _read_routine(self) -> None:
        try:
            while True:
                data = await self._protocol.recv()
                logger.debug(f'Received: {data}')
                self.received.emit(data)
        except websockets.exceptions.ConnectionClosedError as err:
            logger.error(f'Lost connection with server: {err}')
            self.error.emit(err)
        except websockets.exceptions.ConnectionClosedOK as exc:
            logger.debug(exc)

    def set_token(self, token: str):
        self.token = token