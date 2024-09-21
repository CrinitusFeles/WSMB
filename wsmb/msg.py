from typing import Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class Msg(BaseModel):
    src: str
    dst: str
    method: str
    need_answer: bool = False
    msg_id: UUID = Field(default_factory=uuid4)
    data: dict | tuple

    def answer(self, data: Any, dst: str = '') -> "Msg":
        return Msg(src=self.dst,
                   dst=self.src if dst == '' else dst,
                   method=self.method,
                   need_answer=False,
                   data={'answer': data},
                   msg_id=self.msg_id)

    def exception(self, exception_type: str, details: str = '', dst: str = ''):
        return Msg(src=self.dst,
                   dst=self.src if dst == '' else dst,
                   method=self.method,
                   data={'exception': exception_type,
                         'details': details},
                   need_answer=False,
                   msg_id=self.msg_id)