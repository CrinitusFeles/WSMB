from typing import Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
# from wsmb.ws_client import WebSocket

class Msg(BaseModel):
    src: str
    dst: str
    method: str
    need_answer: bool = False
    is_answer: bool = False
    msg_id: UUID = Field(default_factory=uuid4)
    data: dict | tuple
    # ws: WebSocket | None = Field(exclude=True, default_factory=lambda: None)

    def answer(self, data: Any, dst: str = '') -> "Msg":
        return Msg(src=self.dst,
                   dst=self.src if dst == '' else dst,
                   method=self.method,
                   need_answer=False,
                   is_answer=True,
                   data={'answer': data},
                   msg_id=self.msg_id)
                #    ws=self.ws)

    def exception(self, exception_type: str, details: str = '', dst: str = ''):
        return Msg(src=self.dst,
                   dst=self.src if dst == '' else dst,
                   method=self.method,
                   data={'exception': exception_type,
                         'details': details},
                   need_answer=False,
                   is_answer=True,
                   msg_id=self.msg_id)
                #    ws=self.ws)

    def __hash__(self) -> int:
        return self.msg_id.__hash__()

    def __eq__(self, other) -> bool:
        return self.msg_id.__eq__(other.msg_id)