import asyncio
from wsmb.router import Router
from pydantic import ValidationError
from pyvalidate import create_dyn_model
from pyvalidate.examples.models import MyModel
from pyvalidate.examples.example import get_json
router = Router()




@router.subscribe('test_event')
async def foo(self, v1: list[MyModel], v2: dict[str, list[int]], v3: str, v4: int):
    await asyncio.sleep(0.1)
    print(v1[0].model_dump(), v2, v3, v4)



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