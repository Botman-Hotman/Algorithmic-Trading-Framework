from dataclasses import dataclass

from sqlalchemy import Column, String, Integer
from ..Base import Base


@dataclass
class Dimension_Granularity(Base):
    __tablename__ = 'Dimension_Granularity'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    GranularityKey: int = Column(Integer, primary_key=True, autoincrement=True)
    Name: str = Column(String)
    Alias: str = Column(String)
    OandaAlias: str = Column(String)