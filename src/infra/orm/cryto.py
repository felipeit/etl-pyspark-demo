from uuid import uuid4
from sqlalchemy import Column, Integer, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Crypto(Base):
    __tablename__ = "crypto"

    time_open = Column(DateTime, nullable=False) 
    time_close = Column(DateTime, nullable=False)  
    time_high = Column(DateTime, nullable=False) 
    time_low = Column(DateTime, nullable=False) 
    name = Column(Integer, nullable=True)  
    open = Column(Integer, nullable=True)   
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)  
    close = Column(Float, nullable=False)  
    volume = Column(Float, nullable=False)  
    market_cap = Column(Float, nullable=False)  
    timestamp = Column(Float, nullable=False) 