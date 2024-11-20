import random
import uuid
from typing import List, Optional

from pydantic import BaseModel, Field


def _reproducible_uuid() -> str:
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


class Break(BaseModel):
    id: str = Field(default_factory=_reproducible_uuid)
    start: int = Field(description="Break start as Unix timestamp in milliseconds")
    finish: int = Field(description="Break finish as Unix timestamp in milliseconds")
    paid: bool = Field(description="Indicates whether break is paid or not.")


class Allowance(BaseModel):
    id: str = Field(default_factory=_reproducible_uuid)
    value: float
    cost: float


class AwardInterpretation(BaseModel):
    id: str = Field(default_factory=_reproducible_uuid)
    date: str = Field(description="Award interpretation date in YYYY-MM-DD format.")
    units: float
    cost: float


class Shift(BaseModel):
    id: str = Field(default_factory=_reproducible_uuid)
    date: str = Field(description="Shift start date in YYYY-MM-DD format.")
    start: int = Field(description="Shift start as Unix timestamp in milliseconds.")
    finish: int = Field(description="Shift finish as Unix timestamp in milliseconds.")
    breaks: List[Break] = Field([], description="List of breaks during shift.")
    allowances: List[Allowance] = Field(
        [], description="List of allowances during shift."
    )
    award_interpretations: List[AwardInterpretation] = Field(
        [], description="List of shift award interpretations."
    )


class NavigationLinks(BaseModel):
    base: str = Field("http://localhost:8000", description="API base URL.")
    prev: Optional[str] = Field(
        None,
        description="Link to the previous results page. "
        "When not present, there are no more previous pages.",
    )
    next: Optional[str] = Field(
        None,
        description="Link to the next results page. "
        "When not present, there are no more results.",
    )


class Response(BaseModel):
    results: List[Shift]
    links: NavigationLinks
    start: int
    limit: int
    size: int
