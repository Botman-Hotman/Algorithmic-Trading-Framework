from dataclasses import dataclass

from sqlalchemy import Column, String, Integer
from ..Base import Base


@dataclass
class Dimension_Time(Base):
    __tablename__ = 'Dimension_Time'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    TimeKey: int = Column(Integer, primary_key=True)
    TimeString: str = Column(String)
    Hour: int = Column(Integer)
    Minute: int = Column(Integer)
    Second: int = Column(Integer)
    TimeOfDay: str = Column(String)
    IsLondonSession: int = Column(Integer)
    IsNewYorkSession: int = Column(Integer)
    IsSydneySession: int = Column(Integer)
    IsTokoyoSession: int = Column(Integer)