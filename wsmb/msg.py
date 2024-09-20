

from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class Msg(BaseModel):
    src: str
    dst: str
    data: dict
    message_id: UUID = Field(default_factory=uuid4)

    def answer(self, data: dict, dst: str = '') -> "Msg":
        return Msg(src=self.dst,
                   dst=self.src if dst == '' else dst,
                   data=data,
                   message_id=self.message_id)
