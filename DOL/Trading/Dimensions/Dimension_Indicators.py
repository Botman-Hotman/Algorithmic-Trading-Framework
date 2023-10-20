from dataclasses import dataclass

from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship

from ..Base import Base


@dataclass
class Dimension_Indicators(Base):
    __tablename__ = 'Dimension_Indicators'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    IndicatorKey: int = Column(Integer, primary_key=True, autoincrement=True)
    Name: str = Column(String, nullable=False)
    IndicatorCategoryKey: int = Column(Integer, ForeignKey('Trading.Dimension_IndicatorCategory.IndicatorCategoryKey'), nullable=False)
    IndicatorCategory = relationship("Dimension_IndicatorCategory")

    IndicatorTypeKey: int = Column(Integer, ForeignKey('Trading.Dimension_IndicatorType.IndicatorTypeKey'), nullable=False)
    IndicatorType = relationship("Dimension_IndicatorType")
