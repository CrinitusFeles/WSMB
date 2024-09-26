

import asyncio
import json
from wsmb.broker_client import BrokerClient
from wsmb.msg import Msg
from wsmb.test.router import router, get_json


class FooBar:
    def __init__(self) -> None:
        self.broker = BrokerClient()
        self.broker.name = 'user'
        self.broker.include_router(router, self)
        self._secret_field = 'secret'

def on_exception(exc_type, details):
    print(exc_type, details)


async def main() -> None:
    v = FooBar()
    v.broker.on_exception = on_exception
    json_str: str = get_json()
    data: dict = json.loads(json_str)
    got_msg = Msg(src='server', dst='user', method='test_event', data=data)
    await v.broker.route(got_msg.model_dump_json())
    await asyncio.sleep(0.1)


if __name__ == '__main__':
    asyncio.run(main())