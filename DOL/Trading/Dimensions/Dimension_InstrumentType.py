from dataclasses import dataclass

from sqlalchemy import Column, String, Integer
from ..Base import Base


@dataclass
class Dimension_InstrumentType(Base):
    __tablename__ = 'Dimension_InstrumentType'
    __table_args__ = {'schema': 'Trading'}

    InstrumentTypeKey: int = Column(Integer, primary_key=True, autoincrement=True)
    Name: str = Column(String, nullable=False)
