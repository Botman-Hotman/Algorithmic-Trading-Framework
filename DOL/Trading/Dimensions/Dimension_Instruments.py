from dataclasses import dataclass

from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship

from ..Base import Base


@dataclass
class Dimension_Instruments(Base):
    __tablename__ = 'Dimension_Instruments'
    __table_args__ = {'schema': 'Trading'}

    InstrumentKey: int = Column(Integer, primary_key=True, autoincrement=True)
    Name: str = Column(String, nullable=False)
    InstrumentTypeKey: int = Column(Integer, ForeignKey('Trading.Dimension_InstrumentType.InstrumentTypeKey'), nullable=False)
    InstrumentType = relationship("Dimension_InstrumentType")
