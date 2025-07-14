from datetime import date, datetime
from pydantic import BaseModel


class SpimexTradingResultBase(BaseModel):
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: float
    total: float
    count: int
    date: date


class SpimexTradingResultCreate(SpimexTradingResultBase):
    pass


class SpimexTradingResult(SpimexTradingResultBase):
    id: int
    created_on: datetime
    updated_on: datetime

    class Config:
        from_attributes = True
