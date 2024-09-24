import asyncio
from wsmb.router import Router


router = Router()


@router.subscribe('test_event')
async def foo(self, var: int):
    await asyncio.sleep(0.02)
    print(var, self._secret_field)


@router.subscribe('another_test_event')
def foo2(self, val: str):
    # await asyncio.sleep(0.02)
    print(val)