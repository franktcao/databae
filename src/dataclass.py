import enum
from typing import Annotated
from pydantic import BaseModel, Field
from pydantic.typing import Literal


class FilterConfig(BaseModel):
    method: Literal["keep", "drop"]
    combine: Literal["and", "or"]