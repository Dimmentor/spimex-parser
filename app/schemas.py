from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field


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


class TradingResultResponse(BaseModel):
    id: int
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: Optional[float]
    total: Optional[float]
    count: int
    date: str
    created_on: Optional[str]
    updated_on: Optional[str]


class LastTradingDatesResponse(BaseModel):
    dates: List[str]
    count: int


class DynamicsRequest(BaseModel):
    start_date: date = Field(..., description="обязательно")
    end_date: date = Field(..., description="обязательно")
    oil_id: Optional[str] = Field(None, description="опционально")
    delivery_type_id: Optional[str] = Field(None, description="опционально")
    delivery_basis_id: Optional[str] = Field(None, description="опционально")


class DynamicsResponse(BaseModel):
    results: List[TradingResultResponse]
    count: int


class TradingResultsRequest(BaseModel):
    oil_id: Optional[str] = Field(None, description="опционально")
    delivery_type_id: Optional[str] = Field(None, description="опционально")
    delivery_basis_id: Optional[str] = Field(None, description="опционально")
    limit: int = Field(100, ge=1, le=1000, description="по умолчанию 100")


class TradingResultsResponse(BaseModel):
    results: List[TradingResultResponse]
    count: int
