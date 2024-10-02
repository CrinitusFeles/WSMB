import asyncio
from wsmb.router import Router
from pydantic import ValidationError
from pyvalidate import create_dyn_model
from pyvalidate.examples.models import MyModel
from pyvalidate.examples.example import get_json
router = Router()




@router.subscribe('test_event', postrouter=lambda msg: print('event'))
async def foo(self, v1: list[MyModel], v2: dict[str, list[int]], v3: str, v4: int):
    await asyncio.sleep(1.5)
    print('test_event', v1[0].model_dump(), v2, v3, v4, self._secret_field)


@router.subscribe('test_event2', postrouter=lambda msg: print('event2'))
async def foo2(self, v1: list[MyModel], v2: dict[str, list[int]], v3: str, v4: int):
    await asyncio.sleep(2.5)
    print('test_event2', v1[0].model_dump(), v2, v3, v4, self._secret_field)


async def main():
    json_str = get_json()
    print(json_str)
    try:
        model = create_dyn_model(foo)
        obj = model.model_validate_json(json_str)
        await foo(**obj.__dict__)
    except ValidationError as err:
        print(err.json(include_url=False))

if __name__ == '__main__':
    asyncio.run(main())