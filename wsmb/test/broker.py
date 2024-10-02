

import asyncio
import json
from wsmb.broker_client import BrokerClient
from wsmb.msg import Msg
from wsmb.test.router import router, get_json

class WS_Client:

    async def send_text(self, text: str):
        ...

class FooBar:
    def __init__(self) -> None:
        self.broker = BrokerClient()
        self.ws = WS_Client()
        self.broker.name = 'user'
        self.broker.ws = self.ws  # type: ignore
        self.broker.include_router(router, self)
        self._secret_field = 'secret'

def on_exception(exc_type, details):
    print(exc_type, details)

v = FooBar()

@v.broker.postroute('test_event')
def postroute(msg):
    print(f'postroute: {msg}')


async def main() -> None:
    v.broker.on_exception = on_exception
    json_str: str = get_json()
    data: dict = json.loads(json_str)
    got_msg = Msg(src='server', dst='user', method='test_event', data=data,
                  need_answer=True)
    got_msg2 = Msg(src='server', dst='user', method='test_event2', data=data,
                   need_answer=True)
    await v.broker.route(got_msg2.model_dump_json())
    await v.broker.route(got_msg.model_dump_json())
    await asyncio.sleep(3)


if __name__ == '__main__':
    asyncio.run(main())