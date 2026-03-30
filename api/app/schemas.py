from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime

class CompanyIn(BaseModel):
    ticker: str
    name: str
    cik: Optional[str] = None
    sector: Optional[str] = None
    index_memberships: List[str] = Field(default_factory=list)

class CompanyOut(CompanyIn):
    id: int

class SignalIn(BaseModel):
    company_id: int
    ts: datetime
    name: str
    value: float
    meta: Dict = Field(default_factory=dict)

class SignalOut(SignalIn):
    pass