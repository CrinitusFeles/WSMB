

import asyncio
from wsmb.broker_client import BrokerClient
from wsmb.msg import Msg
from wsmb.test.router import router


class FooBar:
    def __init__(self) -> None:
        self.broker = BrokerClient()
        self.broker.name = 'user'
        self.broker.include_router(router, self)
        self._secret_field = 'secret'


async def main():
    v = FooBar()
    got_msg = Msg(src='server', dst='user', method='test_event', data=(123,))
    await v.broker.route(got_msg.model_dump_json())
    got_msg = Msg(src='server', dst='user', method='another_test_event', data=(123,))
    await v.broker.route(got_msg.model_dump_json())
    await asyncio.sleep(0.1)


if __name__ == '__main__':
    asyncio.run(main())