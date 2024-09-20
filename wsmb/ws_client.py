import asyncio
from functools import partial
from typing import Callable, Coroutine
from loguru import logger
import websockets
from websockets.asyncio.async_timeout import timeout
from websockets.uri import parse_uri, WebSocketURI
from websockets.extensions.permessage_deflate import ClientPerMessageDeflateFactory
import urllib.parse


compress = ClientPerMessageDeflateFactory(compress_settings={"memLevel": 5})


class AuthorizationError(Exception):
    ...


class WebSocket:
    def __init__(self) -> None:
        self.on_received: Callable[..., Coroutine] | None = None
        self.on_connected: Callable[..., Coroutine] | None = None
        self.on_disconnected: Callable[..., Coroutine] | None = None
        self.on_error: Callable[..., Coroutine] | None = None
        self.on_token_error: Callable[..., Coroutine] | None = None
        self.on_critical_error: Callable[..., Coroutine] | None = None
        self._destination: str | None = None
        self._last_connection_data: tuple = ()
        self.read_task: asyncio.Task | None = None
        self.connection_status: bool = False

    def set_destination(self, dst: str | None) -> None:
        self._destination = dst

    def get_destination(self) -> str | None:
        return self._destination

    def handle_redirect(self, uri: str) -> None:
        # Update the state of this instance to connect to a new URI.
        old_uri = self._uri
        old_wsuri = self._wsuri
        new_uri = urllib.parse.urljoin(old_uri, uri)
        new_wsuri = parse_uri(new_uri)

        # Forbid TLS downgrade.
        if old_wsuri.secure and not new_wsuri.secure:
            raise websockets.SecurityError("redirect from WSS to WS")

        same_origin = (
            old_wsuri.host == new_wsuri.host and old_wsuri.port == new_wsuri.port
        )

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

    async def connect(self, uri: str, headers: dict | None = None):
        while True:
            try:
                async with timeout(5):
                    if await self._connect(uri, headers):
                        break
            except TimeoutError:
                logger.error('Connection timeout')
                if self.on_error:
                    await self.on_error(TimeoutError('Connection timeout'))
            except ConnectionRefusedError as err:
                logger.error(err)

    async def _connect(self, uri: str, headers: dict | None ) -> bool:
        self._uri: str = uri
        self._wsuri: WebSocketURI = parse_uri(self._uri)

        kwargs: dict = {}
        if self._wsuri.secure:
            kwargs.setdefault("ssl", True)
        if kwargs.get("ssl"):
            kwargs.setdefault("server_hostname", self._wsuri.host)
        self.factory = partial(
            websockets.WebSocketClientProtocol,
            extensions=[compress],
            ping_interval=20,
            ping_timeout=3,
            close_timeout=3,
            extra_headers=headers,
            host=self._wsuri.host,
            port=self._wsuri.port,
            secure=self._wsuri.secure,
            **kwargs
        )

        self._protocol: websockets.WebSocketClientProtocol
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
        except websockets.RedirectHandshake as exc:
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
                        except TimeoutError:
                            logger.error('Timeout of refresh token request')
                        if self.on_critical_error:
                            await self.on_critical_error()
                        raise AuthorizationError
            if self.on_error:
                await self.on_error(err)
            return False

        self._protocol = protocol
        logger.success('Connected')
        self.read_task = self._loop.create_task(self._read_routine())
        self.read_task.add_done_callback(self._read_done_callback)
        if self.on_connected:
            await self.on_connected()
        self._last_connection_data = (uri, headers)
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

    async def disconnect(self) -> None:
        if self.connection_status:
            self.connection_status = False
            if self.read_task:
                self.read_task.cancel()
                self.read_task = None
            await self._protocol.close()
            logger.debug('Disconnected')
        else:
            logger.warning('WS client was not connected')

    async def reconnect(self):
        if self.connection_status:
            if self.on_disconnected:
                await self.on_disconnected()
            logger.debug('Trying to reconnect')
            if not self._last_connection_data:
                logger.error('Connection was never established')
                return
            await self.disconnect()
            await self.connect(*self._last_connection_data)

    async def send(self, data: str | bytes) -> None:
        await self._protocol.send(data)

    async def send_text(self, data: str) -> None:
        await self._protocol.send(data)

    async def receive_text(self):
        return await self._protocol.recv()

    async def _read_routine(self) -> None:
        try:
            while True:
                data = await self._protocol.recv()
                logger.debug(f'Received: {data}')
                if self.on_received:
                    await self.on_received(data)
        except websockets.exceptions.ConnectionClosedError as err:
            logger.error(f'Lost connection with server: {err}')
            if self.on_error:
                await self.on_error(err)
        except websockets.exceptions.ConnectionClosedOK as exc:
            logger.debug(exc)