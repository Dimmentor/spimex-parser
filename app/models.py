from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, func, Text
from .database import Base


class SpimexTradingResult(Base):
    __tablename__ = 'spimex_trading_results'

    id = Column(Integer, primary_key=True, index=True)
    exchange_product_id = Column(String(20), index=True)
    exchange_product_name = Column(Text)
    oil_id = Column(String(10), index=True)
    delivery_basis_id = Column(String(10), index=True)
    delivery_basis_name = Column(Text)
    delivery_type_id = Column(String(5))
    volume = Column(Numeric(20, 2))
    total = Column(Numeric(20, 2))
    count = Column(Integer)
    date = Column(Date, index=True)
    created_on = Column(DateTime, server_default=func.now())
    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())
