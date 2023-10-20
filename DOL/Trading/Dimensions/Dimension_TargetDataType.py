from dataclasses import dataclass

from sqlalchemy import Column, String, Integer
from ..Base import Base


@dataclass
class Dimension_TargetDataType(Base):
    __tablename__ = 'Dimension_TargetDataType'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    TargetDataTypeKey: int = Column(Integer, primary_key=True, autoincrement=True)
    Name: str = Column(String, nullable=False)
    Alias: str = Column(String, nullable=False)
